# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
# use this file except in compliance with the License. A copy of the License
# is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

from . import DynamoDbExpressionParser, AttributeValueEncoder, DaxCborTypes
from .CborEncoder import CborEncoder
from .DaxError import DaxClientError, DaxErrorCode, DaxValidationError

import six

if six.PY2:
    from .grammar2.DynamoDbGrammarListener import DynamoDbGrammarListener
else:
    from .grammar.DynamoDbGrammarListener import DynamoDbGrammarListener

import antlr4

# TODO I feel like this should be able to take a CborEncoder and fill it
# directly, rather than build a bunch of bytes for appending. But maybe
# in this case it's better to be consistent with other clients than
# clever.

class Expressions(object):
    def __init__(self):
        self.Condition = None
        self.KeyCondition = None
        self.Filter = None
        self.Update = None
        self.Projection = None
        self.ExpressionAttributeNames = None
        self.ExpressionAttributeValues = None

def encode_condition_expression(condition_expr, expr_attr_names, expr_attr_values):
    return encode_expressions(condition_expr, None, None, None, None, expr_attr_names, expr_attr_values).Condition

def encode_key_condition_expression(key_condition_expr, expr_attr_names, expr_attr_values):
    return encode_expressions(None, key_condition_expr, None, None, None, expr_attr_names, expr_attr_values).KeyCondition

def encode_filter_expression(filter_expr, expr_attr_names, expr_attr_values):
    return encode_expressions(None, None, filter_expr, None, None, expr_attr_names, expr_attr_values).Filter

def encode_update_expression(update_expr, expr_attr_names, expr_attr_values):
    return encode_expressions(None, None, None, update_expr, None, expr_attr_names, expr_attr_values).Update

def encode_projection_expression(projection_expr, expr_attr_names, expr_attr_values):
    return encode_expressions(None, None, None, None, projection_expr, expr_attr_names, expr_attr_values).Projection

def encode_expressions(condition_expr, key_condition_expr, filter_expr, update_expr, projection_expr, 
        expr_attr_names, expr_attr_values):
    exprs = [
        ('Condition', condition_expr),
        ('KeyCondition', key_condition_expr),
        ('Filter', filter_expr),
        ('Update', update_expr),
        ('Projection', projection_expr)
    ]

    output = Expressions()

    generator = CborSExprGenerator(expr_attr_names, expr_attr_values)

    for _type, expr in exprs:
        if not expr:
            setattr(output, _type, None)
            continue

        typestr = _type + 'Expression'
        try:
            if _type in ('Condition', 'KeyCondition', 'Filter'):
                expr_array_len = 3
                tree = DynamoDbExpressionParser.parse_condition(expr, ExpressionErrorListener(expr, typestr))
            elif _type == 'Projection':
                expr_array_len = 2
                tree = DynamoDbExpressionParser.parse_projection(expr, ExpressionErrorListener(expr, typestr))
            elif _type == 'Update':
                expr_array_len = 3
                tree = DynamoDbExpressionParser.parse_update(expr, ExpressionErrorListener(expr, typestr))
            else:
                raise DaxClientError('Unknown expresion type ' + str(_type), DaxErrorCode.Validation)
        except Exception:
            # TODO Something, eventually
            raise

        generator._reset(_type)
        antlr4.tree.Tree.ParseTreeWalker.DEFAULT.walk(generator, tree)

        generator._validate_intermediate_state()
        spec = generator.stack.pop()

        enc = CborEncoder()
        enc.append_array_header(expr_array_len)
        enc.append_int(CborSExprGenerator.ENCODING_FORMAT)
        enc.append_raw(spec)

        if _type != 'Projection':
            enc.append_array_header(len(generator.var_values))
            for var_val in generator.var_values:
                enc.append_raw(var_val)

        setattr(output, _type, enc.as_bytes())

    generator._validate_final_state()

    output.ExpressionAttributeNames = expr_attr_names
    output.ExpressionAttributeValues = expr_attr_values

    return output

class CborSExprGenerator(DynamoDbGrammarListener):
    ENCODING_FORMAT = 1
    ATTRIBUTE_VALUE_PREFIX = ':'
    ATTRIBUTE_NAME_PREFIX = '#'

    def __init__(self, expression_attribute_names, expression_attribute_values):
        self.stack = []
        self._reset(None)

        self.expr_attr_names = expression_attribute_names
        self.expr_attr_values = expression_attribute_values

        self.unused_expr_attr_names = set(self.expr_attr_names.keys()) if self.expr_attr_names else set()
        self.unused_expr_attr_values = set(self.expr_attr_values.keys()) if self.expr_attr_values else set()

    def _reset(self, _type):
        self._type = _type
        self.nesting_level = 0
        self.var_name_by_id = {}
        self.var_values = []

    def _validate_intermediate_state(self):
        if len(self.stack) != 1:
            raise DaxValidationError('Invalid {}Expression, Stack size = {}'.format(self._type, len(self.stack)))

        if self.nesting_level != 0:
            raise DaxValidationError('Invalid {}Expression, Nesting level = {}'.format(self._type, self.nesting_level))

    def _validate_final_state(self):
        if self.unused_expr_attr_names:
            names = self._join_missing_names(self.unused_expr_attr_names)
            raise DaxValidationError('Value provided in ExpressionAttributeNames unused in expressions: keys: {' + names + '}')

        if self.unused_expr_attr_values:
            names = self._join_missing_names(self.unused_expr_attr_values)
            raise DaxValidationError('Value provided in ExpressionAttributeValues unused in expressions: keys: {' + names + '}')

    def _validate_not_equals(self, expr_type, actual, not_expected):
        for n in not_expected:
            if actual.lower() == n.lower():
                exp_type_str = expr_type or ''
                raise DaxValidationError(
                    "Invalid {}Expression: The function '{}' is not allowed in a {} expression".format(
                        exp_type_str, actual, exp_type_str.lower()))

    def _join_missing_names(self, names):
        return ', '.join(names)

    def enterComparator(self, ctx):
        self.nesting_level += 1

    def exitComparator(self, ctx):
        arg2 = self.stack.pop()
        func = self.stack.pop()
        arg1 = self.stack.pop()

        self.stack.append(self._encode_array([func, arg1, arg2]))
        self.nesting_level -= 1

    def exitComparator_symbol(self, ctx):
        text = ctx.getText()
        try:
            func = OPERATORS[text]
        except KeyError:
            raise DaxClientError('Invalid function ' + text, DaxErrorCode.Validation, False)

        self.stack.append(self._encode_function_code(func))

    def exitPath(self, ctx):
        n = ctx.getChildCount()
        components = self.stack[-n:]; del self.stack[-n:]

        self.stack.append(self._encode_function(Func.DocumentPath, components))

    def exitListAccess(self, ctx):
        value = ctx.getText()
        ordinal = int(value[1:-1]) # get rid of []
        self.stack.append(self._encode_list_access(ordinal))

    def exitId_(self, ctx):
        _id = ctx.getText()
        if _id[0] == CborSExprGenerator.ATTRIBUTE_NAME_PREFIX:
            try:
                sub = self.expr_attr_names[_id]
            except KeyError:
                raise DaxValidationError('Invalid {}Expression. Substitution value not provided for {}', self._type, _id)

            self.unused_expr_attr_names.discard(_id)
            self.stack.append(self._encode_attribute_value({'S': sub}))
        else:
            self.stack.append(self._encode_document_path_element(_id))
            
    def exitLiteralSub(self, ctx):
        literal = ctx.getText()
        self.stack.append(self._encode_variable(literal[1:]))

    def exitAnd(self, ctx):
        arg2 = self.stack.pop()
        arg1 = self.stack.pop()
        self.stack.append(self._encode_function(Func.And, [arg1, arg2]))

    def exitOr(self, ctx):
        arg2 = self.stack.pop()
        arg1 = self.stack.pop()
        self.stack.append(self._encode_function(Func.Or, [arg1, arg2]))

    def exitNegation(self, ctx):
        arg = self.stack.pop()
        self.stack.append(self._encode_function(Func.Not, [arg]))

    def enterIn(self, ctx):
        self.nesting_level += 1

    def exitIn(self, ctx):
        numargs = (ctx.getChildCount() - 3) // 2    # arg + IN + ( + args*2-1 + )
        args = self.stack[-numargs:]; del self.stack[-numargs:]
        arg1 = self.stack.pop()

        # a in (b,c,d) =>  (In a (b c d))
        self.stack.append(self._encode_function(Func.In, [arg1, self._encode_array(args)]))
        self.nesting_level -= 1

    def enterBetween(self, ctx):
        self.nesting_level += 1

    def exitBetween(self, ctx):
        args = self.stack[-3:]; del self.stack[-3:]

        # a between b and c => (Between a b c)
        self.stack.append(self._encode_function(Func.Between, args))
        self.nesting_level -= 1

    def enterFunctionCall(self, ctx):
        funcname = ctx.ID().getText()
        if self._type == 'Update':
            self._validate_not_equals(self._type, funcname, ['attribute_exists', 'attribute_not_exists',
                'attribute_type', 'begins_with', 'contains', 'size'])
            if self.nesting_level > 0 and funcname.lower() != 'if_not_exists':
                raise DaxValidationError('Only if_not_exists() function can be nested (got ' + funcname.lower() + ')')

        elif self._type == 'Filter' or self._type == 'Condition':
            self._validate_not_equals(self._type, funcname, ['if_not_exists', 'list_append'])
            if self.nesting_level == 0 and funcname.lower() == 'size':
                raise DaxValidationError(
                        "Invalid {}Expression: The function '{}' is not allowed to be used this way in an expression".format(
                    self._type, funcname))
            elif self.nesting_level > 0 and funcname.lower() != 'size':
                raise DaxValidationError('Only size() function can be nested (got ' + funcname.lower() + ')')

        self.nesting_level += 1

    def exitFunctionCall(self, ctx):
        funcname = ctx.ID().getText().lower()

        try:
            func = FUNCS[funcname]
        except KeyError:
            raise DaxValidationError(
                    'Invalid {}Expression: Invalid function name: function: {}'.format(self._type, funcname))

        numargs = (ctx.getChildCount() - 2) // 2     # children = fname + ( + numOperands*2-1 +)
        args = self.stack[-numargs:]; del self.stack[-numargs:]

        # func(a,b,c,..) => (func a b c ..)
        self.stack.append(self._encode_function(func, args))
        self.nesting_level -= 1

    def exitProjection(self, ctx):
        numpaths = (ctx.getChildCount() + 1) // 2 # path, path, ... path
        paths = self.stack[-numpaths:]; del self.stack[-numpaths:]
        self.stack.append(self._encode_array(paths))

    def exitUpdate(self, ctx):
        updates = self.stack[:]; del self.stack[:]
        self.stack.append(self._encode_array(updates))

    def exitSet_action(self, ctx):
        operand = self.stack.pop()
        path = self.stack.pop()
        self.stack.append(self._encode_function(Func.SetAction, [path, operand]))

    def exitRemove_action(self, ctx):
        path = self.stack.pop()
        self.stack.append(self._encode_function(Func.RemoveAction, [path]))

    def exitAdd_action(self, ctx):
        value = self.stack.pop()
        path = self.stack.pop()
        self.stack.append(self._encode_function(Func.AddAction, [path, value]))

    def exitDelete_action(self, ctx):
        value = self.stack.pop()
        path = self.stack.pop()
        self.stack.append(self._encode_function(Func.DeleteAction, [path, value]))

    def enterPlusMinus(self, ctx):
        self.nesting_level += 1

    def exitPlusMinus(self, ctx):
        op2 = self.stack.pop()
        op1 = self.stack.pop()

        operator = ctx.getChild(1).getText()
        try:
            func = OPERATORS[operator]
        except KeyError:
            raise DaxClientError('Must be +/-', DaxErrorCode.Validation, False)

        self.stack.append(self._encode_function(func, [op1, op2]))
        self.nesting_level -= 1

    def _encode_document_path_element(self, s):
        return CborEncoder().append_string(s).as_bytes()

    def _encode_attribute_value(self, val):
        return AttributeValueEncoder.encode_attribute_value(val)

    def _encode_array(self, array):
        enc = CborEncoder()
        enc.append_array_header(len(array))
        for item in array:
            # Array items are already CBOR encoded
            enc.append_raw(item)
        return enc.as_bytes()

    def _encode_function_code(self, func):
        return CborEncoder().append_int(func).as_bytes()

    def _encode_function(self, func, args):
        enc = CborEncoder()
        enc.append_array_header(len(args) + 1)
        enc.append_int(func)
        for arg in args:
            # Args are already CBOR encoded
            enc.append_raw(arg)
        return enc.as_bytes()

    def _encode_list_access(self, ordinal):
        enc = CborEncoder()
        enc.append_tag(DaxCborTypes.TAG_DDB_DOCUMENT_PATH_ORDINAL)
        enc.append_int(ordinal)
        return enc.as_bytes()

    def _encode_variable(self, var_name):
        fullname = CborSExprGenerator.ATTRIBUTE_VALUE_PREFIX + var_name
        try:
            val = self.expr_attr_values[fullname]
        except KeyError:
            raise DaxClientError(
                'Invalid {}Expression: An expression attribute value used in expression is not defined: {}'.format(
                    self._type, fullname), 
                DaxErrorCode.Validation, False)
        
        self.unused_expr_attr_values.discard(fullname)
        try:
            var_id = self.var_name_by_id[var_name]
        except KeyError:
            var_id = len(self.var_values)
            self.var_name_by_id[var_name] = var_id
            self.var_values.append(self._encode_attribute_value(val))

        enc = CborEncoder()
        enc.append_array_header(2)
        enc.append_int(Func.Variable)
        enc.append_int(var_id)
        return enc.as_bytes()

from antlr4.error.ErrorListener import ErrorListener
class ExpressionErrorListener(ErrorListener):
    def __init__(self, expr, expr_type):
        super(ExpressionErrorListener, self).__init__()
        self.expr = expr
        self.expr_type = expr_type

    def syntaxError(self, recognizer, offendingSymbol, line, column, message, exception):
        raise DaxClientError(
            'Invalid {}: Syntax error; token: "{}", near: line {} char {}'.format(self.expr_type, offendingSymbol.text, line, column),
            DaxErrorCode.Validation, False)

class Func:
    # NOTE = Ordinal is used as identifiers in CBor encoded format
    # Comparison operators #
    Equal = 0
    NotEqual = 1
    LessThan = 2
    GreaterEqual = 3
    GreaterThan = 4
    LessEqual = 5

    # Logical operators #
    And = 6
    Or = 7
    Not = 8

    # Range operators #
    Between = 9

    # Enumeration operators #
    In = 10

    # Functions #
    AttributeExists = 11
    AttributeNotExists = 12
    AttributeType = 13
    BeginsWith = 14
    Contains = 15
    Size = 16

    # Document path elements #
    Variable = 17       # takes 1 argument which is a placeholder for a value. function substitutes argument with corresponding value
    DocumentPath = 18   # maps a CBOR object to a document path

    # Update Actions #
    SetAction = 19
    AddAction = 20
    DeleteAction = 21
    RemoveAction = 22

    # Update operations #
    IfNotExists = 23
    ListAppend = 24
    Plus = 25
    Minus = 26
    
OPERATORS = {
    '+': Func.Plus,
    '-': Func.Minus,
    '=': Func.Equal,
    '<>': Func.NotEqual,
    '<': Func.LessThan,
    '<=': Func.LessEqual,
    '>': Func.GreaterThan,
    '>=': Func.GreaterEqual,
    'and': Func.And,
    'or': Func.Or,
    'not': Func.Not,
    'between': Func.Between,
    'in': Func.In,
    ':': Func.Variable,
    '.': Func.DocumentPath,
}

ACTIONS = {
    'SET': Func.SetAction,
    'ADD': Func.AddAction,
    'DELETE': Func.DeleteAction,
    'REMOVE': Func.RemoveAction,
}

FUNCS = {
    'attribute_exists': Func.AttributeExists,
    'attribute_not_exists': Func.AttributeNotExists,
    'attribute_type': Func.AttributeType,
    'begins_with': Func.BeginsWith,
    'contains': Func.Contains,
    'size': Func.Size,
    'if_not_exists': Func.IfNotExists,
    'list_append': Func.ListAppend
}

