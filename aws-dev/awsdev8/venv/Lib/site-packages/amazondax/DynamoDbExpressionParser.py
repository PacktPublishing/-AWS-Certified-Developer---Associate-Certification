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

import six

if six.PY2:
    from amazondax.grammar2.DynamoDbGrammarLexer import DynamoDbGrammarLexer
    from amazondax.grammar2.DynamoDbGrammarParser import DynamoDbGrammarParser
else:
    from amazondax.grammar.DynamoDbGrammarLexer import DynamoDbGrammarLexer
    from amazondax.grammar.DynamoDbGrammarParser import DynamoDbGrammarParser

import antlr4
from antlr4.error.ErrorStrategy import DefaultErrorStrategy, BailErrorStrategy
from antlr4.error.Errors import ParseCancellationException

def parse_projection(expression, error_listener):
    return _ProjectionExpression().parse(expression, error_listener)

def parse_condition(expression, error_listener):
    return _ConditionExpression().parse(expression, error_listener)

def parse_update(expression, error_listener):
    return _UpdateExpression().parse(expression, error_listener)

class _AbstractTwoStageExpressionParser(object):
    def parse(self, expression, error_listener):
        lexer = DynamoDbGrammarLexer(antlr4.InputStream(expression))
        tokens = antlr4.CommonTokenStream(lexer)
        parser = DynamoDbGrammarParser(tokens)
        parser.buildParseTrees = True

        lexer.removeErrorListeners()
        lexer.addErrorListener(error_listener)
        parser.removeErrorListeners()
        
        # DO NOT CALL addErrorListener(errorListener) for BailErrorStrategy
        # ExpressionErrorListener converts syntaxErrors to validation exceptions
        # But when using SLL strategy syntaxErrors can be false
        # Such that a syntaxError thrown by SLL may not necessarily be a real syntaxError
        # For such cases we don't want the syntaxError become a validation exception and go up to customers
        # We need to parse the expression again with LL strategy in case of a syntax error
        # BailErrorStrategy will re-throw RecognitionExceptions as ParseCancellationException
        # Such that it's not caught by underlying parsing rule implemented by parseStub
        parser._interp.predictionMode = antlr4.PredictionMode.SLL
        parser._errHandler = BailErrorStrategy()
        try:
            # Stage 1 parse with PredictionMode.SLL
            # If there are no issues SLL was enough to parse
            # If there were problems LL will be used to try again
            return self.parse_stub(parser)
        except ParseCancellationException as e:
            # If there was an error we don't know if it's a real SyntaxError
            # Or SLL strategy wasn't strong enough
            # Stage 2 parse with default prediction mode
            tokens.reset()
            parser.reset()
            parser.addErrorListener(error_listener)
            parser._errHandler = DefaultErrorStrategy()
            parser._interp.predictionMode = antlr4.PredictionMode.LL
            return self.parse_stub(parser)

    def parse_stub(self, parsers):
        raise NotImplementedError('abstract method')

class _ProjectionExpression(_AbstractTwoStageExpressionParser):
    def parse_stub(self, parser):
        return parser.projection_()

class _ConditionExpression(_AbstractTwoStageExpressionParser):
    def parse_stub(self, parser):
        return parser.condition_()

class _UpdateExpression(_AbstractTwoStageExpressionParser):
    def parse_stub(self, parser):
        return parser.update_()

