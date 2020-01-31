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

import json

from decimal import Decimal

from .Constants import DaxMethodIds, DaxDataRequestParam
from .DaxError import DaxClientError, DaxErrorCode

NAME_PREFIX = '#key'
VALUE_PREFIX = ':val'

def convert_request(request, method_id):
    # Make a copy so we don't modify the user's instance
    request = request.copy()

    # Workaround a bug in the initial release of DAX
    request.setdefault('ReturnConsumedCapacity', 'NONE')

    convert_V1_to_V2_request(request, method_id)

    return request

def is_V1_request(request, method_id):
    if method_id == DaxMethodIds.getItem_263244906_1_Id:
        return 'AttributesToGet' in request
    elif method_id == DaxMethodIds.batchGetItem_N697851100_1_Id:
        items = request.get('RequestItems')
        if items:
            for table_name, table_req in items.items():
                if 'AttributesToGet' in table_req:
                    return True
        return False
    elif method_id in (DaxMethodIds.putItem_N2106490455_1_Id, DaxMethodIds.deleteItem_1013539361_1_Id):
        return 'Expected' in request
    elif method_id == DaxMethodIds.updateItem_1425579023_1_Id:
        return 'Expected' in request or 'AttributeUpdates' in request
    elif method_id == DaxMethodIds.query_N931250863_1_Id:
        return 'AttributesToGet' in request or 'QueryFilter' in request or 'KeyConditions' in request
    elif method_id == DaxMethodIds.scan_N1875390620_1_Id:
        return 'AttributesToGet' in request or 'ScanFilter' in request
    elif method_id == DaxMethodIds.batchWriteItem_116217951_1_Id:
        return False
    else:
        raise DaxClientError('Unknown method_id type {}'.format(method_id), DaxErrorCode.ValidationError, False)

def convert_V1_to_V2_request(request, method_id):
    if method_id == DaxMethodIds.getItem_263244906_1_Id:
        attr_to_get = request.pop('AttributesToGet', None)
        if attr_to_get:
            _convert_AttributesToGet_to_ProjectionExpression_request(request, attr_to_get)

    elif method_id == DaxMethodIds.batchGetItem_N697851100_1_Id:
        items = request.get('RequestItems')
        if items:
            for table_name, table_req in items.items():
                attr_to_get = table_req.pop('AttributesToGet', None)
                if attr_to_get:
                    _convert_AttributesToGet_to_ProjectionExpression_request(table_req, attr_to_get)
    
    elif method_id in (DaxMethodIds.putItem_N2106490455_1_Id, DaxMethodIds.deleteItem_1013539361_1_Id):
        expected = request.pop('Expected', None)
        if expected:
            _convert_Expected_to_ConditionExpression_request(request, expected)

    elif method_id == DaxMethodIds.updateItem_1425579023_1_Id:
        expected = request.pop('Expected', None)
        if expected:
            _convert_Expected_to_ConditionExpression_request(request, expected)

        attr_updates = request.pop('AttributeUpdates', None)
        if attr_updates:
            _convert_AttributeUpdates_to_UpdateExpression_request(request, attr_updates)

    elif method_id == DaxMethodIds.query_N931250863_1_Id:

        attr_to_get = request.pop('AttributesToGet', None)
        if attr_to_get:
            _convert_AttributesToGet_to_ProjectionExpression_request(request, attr_to_get)

        key_conds = request.pop('KeyConditions', None)
        if key_conds:
            _convert_KeyConditions_to_Expression(request, key_conds)

        query_filter = request.pop('QueryFilter', None)
        if query_filter:
            _convert_Filter_to_Expression(request, query_filter)

    elif method_id == DaxMethodIds.scan_N1875390620_1_Id:
        attr_to_get = request.pop('AttributesToGet', None)
        if attr_to_get:
            _convert_AttributesToGet_to_ProjectionExpression_request(request, attr_to_get)

        scan_filter = request.pop('ScanFilter', None)
        if scan_filter:
            _convert_Filter_to_Expression(request, scan_filter)

    elif method_id == DaxMethodIds.batchWriteItem_116217951_1_Id:
        pass
    else:
        raise DaxClientError('Unknown method_id type {}'.format(method_id), DaxErrorCode.ValidationError, False)
    
    # Clean up remaining V1 attributes
    request.pop('ConditionalOperator', None)

def _convert_AttributesToGet_to_ProjectionExpression_request(request, attr_to_get):
    expr_attr_names = request.setdefault('ExpressionAttributeNames', {})
    request['ProjectionExpression'] = _convert_to_expr_attr_string(attr_to_get, NAME_PREFIX, ',', expr_attr_names)

def _convert_Expected_to_ConditionExpression_request(request, expected):
    expr_attr_names = request.setdefault('ExpressionAttributeNames', {})
    expr_attr_values = request.setdefault('ExpressionAttributeValues', {})
    cond_op = request.get('ConditionalOperator', None)
    request['ConditionExpression'] = _convert_conditions_to_expr(
        expected,
        cond_op,
        expr_attr_names,
        expr_attr_values,
        DaxDataRequestParam.ConditionExpression)

def _convert_AttributeUpdates_to_UpdateExpression_request(request, attr_updates):
    expr_attr_names = request.setdefault('ExpressionAttributeNames', {})
    expr_attr_values = request.setdefault('ExpressionAttributeValues', {})
    request['UpdateExpression'] = _convert_update_attr_to_update_expr(attr_updates, expr_attr_names, expr_attr_values)

def _convert_KeyConditions_to_Expression(request, key_conds):
    expr_attr_names = request.setdefault('ExpressionAttributeNames', {})
    expr_attr_values = request.setdefault('ExpressionAttributeValues', {})
    cond_op = request.get('ConditionalOperator', None)
    request['KeyConditionExpression'] = _convert_conditions_to_expr(
            key_conds,
            cond_op,
            expr_attr_names,
            expr_attr_values,
            DaxDataRequestParam.KeyConditionExpression)

def _convert_Filter_to_Expression(request, filter_):
    expr_attr_names = request.setdefault('ExpressionAttributeNames', {})
    expr_attr_values = request.setdefault('ExpressionAttributeValues', {})
    cond_op = request.get('ConditionalOperator', None)
    request['FilterExpression'] = _convert_conditions_to_expr(
            filter_,
            cond_op,
            expr_attr_names,
            expr_attr_values,
            DaxDataRequestParam.FilterExpression)

def _convert_update_attr_to_update_expr(attr_updates, expr_attr_names, expr_attr_values):
    sets = []
    adds = []
    deletes = []
    removes = []

    for attr_name, attr_update in sorted(attr_updates.items()):
        # Move ahead If update entry is null. DynamoDB's behavior.
        if not attr_update:
            continue

        action = attr_update.get('Action', 'PUT')
        attr_value = attr_update.get('Value')
        if not attr_value and action != 'DELETE':
            raise DaxClientError(
                'Only DELETE action is allowed when no attribute value is specified: ' + attr_name, 
                DaxErrorCode.Validation, False)

        if action == 'PUT':
            put_expr = '{} = {}'.format(
                _append_to_expr_attr_map(attr_name, expr_attr_names, NAME_PREFIX),
                _append_to_expr_attr_map(attr_value, expr_attr_values, VALUE_PREFIX))
            sets.append(put_expr)
        elif action == 'ADD':
            add_expr = '{} {}'.format(
                _append_to_expr_attr_map(attr_name, expr_attr_names, NAME_PREFIX),
                _append_to_expr_attr_map(attr_value, expr_attr_values, VALUE_PREFIX))
            adds.append(add_expr)
        elif action == 'DELETE':
            if attr_value:
                del_expr = '{} {}'.format(
                    _append_to_expr_attr_map(attr_name, expr_attr_names, NAME_PREFIX),
                    _append_to_expr_attr_map(attr_value, expr_attr_values, VALUE_PREFIX))
                deletes.append(del_expr)
            else:
                rem_expr = _append_to_expr_attr_map(attr_name, expr_attr_names, NAME_PREFIX)
                removes.append(rem_expr)
        else:
            raise DaxClientError(
                'Action must be one of (ADD, DELETE, PUT) for {}, got {}'.format(attr_name, action),
                DaxErrorCode.Validation, False)

    update_expr = ' ' .join((
        _generate_and_append_update_expr(sets, 'SET'),
        _generate_and_append_update_expr(adds, 'ADD'),
        _generate_and_append_update_expr(deletes, 'DELETE'),
        _generate_and_append_update_expr(removes, 'REMOVE')))

    return update_expr

def _generate_and_append_update_expr(items, op):
    return op + ' ' + ', '.join(items) if items else ''

def _convert_conditions_to_expr(conditions, operator, expr_attr_names, expr_attr_values, expr_type):
    operator = operator or 'AND'
    operator = ' ' + operator.strip() + ' '

    if expr_type == DaxDataRequestParam.FilterExpression:
        encoder = _encode_filter_expr
    elif expr_type == DaxDataRequestParam.ConditionExpression:
        encoder = _encode_condition_expr
    elif expr_type == DaxDataRequestParam.KeyConditionExpression:
        encoder = _encode_key_condition_expr
    else:
        raise ValueError('Unknown expression type: ' + expr_type)

    # Join all non-empty conditions
    return operator.join(filter(None,
        (encoder(cond_attr, cond_details, expr_attr_names, expr_attr_values) \
        for cond_attr, cond_details in sorted(conditions.items()))
    ))

def _encode_filter_expr(attr, condition, expr_attr_names, expr_attr_values):
    if condition is None:
        # This is a special case that is filtered by other clients, so remove it as well
        return

    name = _append_to_expr_attr_map(attr, expr_attr_names, NAME_PREFIX)
    comp_op = condition.get('ComparisonOperator')
    attr_value_list = condition.get('AttributeValueList')
    if attr_value_list and not comp_op:
        raise DaxClientError(
            'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: ' + attr,
            DaxErrorCode.Validation, False)

    return _construct_filter_or_expected_comp_op_expr(
        comp_op, name, attr, attr_value_list, expr_attr_values, 'Unsupported operator on ExpectedAttributeValue: ' + str(condition))

def _encode_condition_expr(attr, expected_attr_val, expr_attr_names, expr_attr_values):
    name = _append_to_expr_attr_map(attr, expr_attr_names, NAME_PREFIX)
    comp_op = expected_attr_val.get('ComparisonOperator')
    if not comp_op:
        return _handle_exists_criteria(attr, expected_attr_val, name, expr_attr_values)

    if 'Exists' in expected_attr_val:
        raise DaxClientError(
            'One or more parameter values were invalid: Exists and ComparisonOperator cannot be used together for Attribute: ' + attr,
            DaxErrorCode.Validation, False)

    attr_value_list = expected_attr_val.get('AttributeValueList')
    if not attr_value_list:
        value = expected_attr_val.get('Value')
        if value:
            attr_value_list = [value]
    else:
        if 'Value' in expected_attr_val:
            raise DaxClientError(
                'Value and AttributeValueList cannot be used together for Attribute: {} {}'.format(attr, attr_value_list),
                DaxErrorCode.Validation, False)

    return _construct_filter_or_expected_comp_op_expr(comp_op, name, attr, attr_value_list, expr_attr_values, 
            'Unsupported operator on ExpectedAttributeValue: ' + str(expected_attr_val))

def _encode_key_condition_expr(attr, key_cond, expr_attr_names, expr_attr_values):
    if not key_cond:
        raise DaxClientError('KeyCondition cannot be null for key: ' + attr, DaxErrorCode.Validation, False)

    comp_op = key_cond.get('ComparisonOperator')
    if not comp_op:
        raise DaxClientError('ComparisonOperator cannot be empty for KeyCondition: ' + key_cond, DaxErrorCode.Validation, False)
    
    attr_value_list = key_cond['AttributeValueList']
    name = _append_to_expr_attr_map(attr, expr_attr_names, NAME_PREFIX)

    if comp_op == 'BETWEEN':
        return _handle_between_condition(comp_op, name, attr_value_list, expr_attr_values, attr)
    elif comp_op == 'BEGINS_WITH':
        return _handle_begins_with_condition(comp_op, name, attr_value_list, expr_attr_values, attr)
    elif comp_op == 'EQ':
        op = '='
    elif comp_op == 'LE':
        op = '<='
    elif comp_op == 'LT':
        op = '<'
    elif comp_op == 'GE':
        op = '>='
    elif comp_op == 'GT':
        op = '>'
    else:
        raise DaxClientError('Unsupported operator on KeyCondition: ' + comp_op, DaxErrorCode.Validation, False)

    _check_num_arguments(comp_op, 1, attr_value_list, attr)
    av0 = attr_value_list[0]
    expr_attr_val0 = _append_to_expr_attr_map(av0, expr_attr_values, VALUE_PREFIX)
    return name + ' ' + op + ' ' + expr_attr_val0

def _construct_filter_or_expected_comp_op_expr(comp_op, name, attr, attr_value_list, expr_attr_values, error_msg):
    if comp_op == 'BETWEEN':
        return _handle_between_condition(comp_op, name, attr_value_list, expr_attr_values, attr)
    elif comp_op == 'BEGINS_WITH':
        return _handle_begins_with_condition(comp_op, name, attr_value_list, expr_attr_values, attr)
    elif comp_op == 'NOT_CONTAINS': # Don't append key name
        return 'attribute_exists(' + name + ') AND NOT ' + _handle_contains_condition(comp_op, name, attr_value_list, expr_attr_values, attr)
    elif comp_op == 'CONTAINS': # Don't append key name
        return _handle_contains_condition(comp_op, name, attr_value_list, expr_attr_values, attr)
    elif comp_op == 'NOT_NULL': # Don't append key name
        _check_num_arguments(comp_op, 0, attr_value_list, attr)
        return 'attribute_exists(' + name + ')'
    elif comp_op == 'NULL': # Don't append key name
        _check_num_arguments(comp_op, 0, attr_value_list, attr)
        return 'attribute_not_exists(' + name + ')'
    elif comp_op == 'IN':
        return _handle_in_condition(comp_op, name, attr_value_list, expr_attr_values, attr)
    elif comp_op == 'EQ':
        op = '='
    elif comp_op == 'LE':
        op = '<='
    elif comp_op == 'LT':
        op = '<'
    elif comp_op == 'GE':
        op = '>='
    elif comp_op == 'GT':
        op = '>'
    elif comp_op == 'NE':
        op = '<>'
    else:
        raise DaxClientError(error_msg, DaxErrorCode.Validation, False)

    _check_num_arguments(comp_op, 1, attr_value_list, attr)
    av0 = attr_value_list[0]
    _check_valid_BSN_type(comp_op, av0)
    expr_attr_val0 = _append_to_expr_attr_map(av0, expr_attr_values, VALUE_PREFIX)
    return '{} {} {}'.format(name, op, expr_attr_val0)

def _convert_to_expr_attr_string(attrs, prefix, delim, expr_attr_names):
    return delim.join(_append_to_expr_attr_map(attr, expr_attr_names, prefix) for attr in attrs)

def _append_to_expr_attr_map(attr, expr_attr_names, prefix):
    suffix = len(expr_attr_names)
    name = prefix + str(suffix)
    while name in expr_attr_names:
        # if it already exists, find a new one
        suffix += 1
        name = prefix + str(suffix)
    
    expr_attr_names[name] = attr
    return name

def _handle_exists_criteria(attr, expected_attr_val, name, expr_attr_values):
    attr_val_list = expected_attr_val.get('AttributeValueList')
    if attr_val_list:
        raise DaxClientError(
            'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: ' + attr,
            DaxErrorCode.Validation, False)

    exists = expected_attr_val.get('Exists') # Exists defaults to true if not specified
    value = expected_attr_val.get('Value')
    if exists is None or exists:
        # If Exists is true, a value must be provided
        if exists and not value:
            raise DaxClientError(
                'One or more parameter values were invalid: Value must be provided when Exists is true for Attribute: ' + attr,
                DaxErrorCode.Validation, False)
    
        if exists is None and not value:
            raise DaxClientError(
                'One or more parameter values were invalid: Value must be provided when Exists is null for Attribute: ' + attr,
                DaxErrorCode.Validation, False)

        expected_attr_val0 = _append_to_expr_attr_map(value, expr_attr_values, VALUE_PREFIX)
        return name + ' = ' + expected_attr_val0
    else:
        # If Exists is false they must not provide a value
        if value:
            raise DaxClientError(
                'One or more parameter values were invalid: Value cannot be used when Exists is false for Attribute: ' + attr,
                DaxErrorCode.Validation, False)

        return 'attribute_not_exists(' + name + ')'

def _handle_between_condition(op, name, attr_value_list, expr_attr_values, attr):
    _check_num_arguments(op, 2, attr_value_list, attr)
    _check_valid_BSN_type(op, attr_value_list[0], attr_value_list[1])
    _check_valid_bounds(attr_value_list[0], attr_value_list[1])
    expr_attr_val0 = _append_to_expr_attr_map(attr_value_list[0], expr_attr_values, VALUE_PREFIX)
    expr_attr_val1 = _append_to_expr_attr_map(attr_value_list[1], expr_attr_values, VALUE_PREFIX)
    return '{} between {} AND {}'.format(name, expr_attr_val0, expr_attr_val1)

def _handle_begins_with_condition(op, name, attr_value_list, expr_attr_values, attr):
    _check_num_arguments(op, 1, attr_value_list, attr)
    av0 = attr_value_list[0]
    if 'B' not in av0 and 'S' not in av0:
        raise DaxClientError(
            'One or more parameter values were invalid: ComparisonOperator {} is not valid for {} AttributeValue type'.format(
                op, list(av0.keys())[0]),
            DaxErrorCode.Validation, false)

    expr_attr_val0 = _append_to_expr_attr_map(av0, expr_attr_values, VALUE_PREFIX)
    return 'begins_with({}, {})'.format(name, expr_attr_val0)

def _handle_contains_condition(op, name, attr_value_list, expr_attr_values, attr):
    _check_num_arguments(op, 1, attr_value_list, attr)
    av0 = attr_value_list[0]
    _check_valid_BSNBoolNull_types(op, av0)
    expr_attr_val0 = _append_to_expr_attr_map(av0, expr_attr_values, VALUE_PREFIX)
    return 'contains({}, {})'.format(name, expr_attr_val0)

def _handle_in_condition(op, name, attr_value_list, expr_attr_values, attr):
    if attr_value_list is None:
        raise DaxClientError(
            'One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: IN for Attribute: ' + attr,
            DaxErrorCode.Validation, False)
    else:
        if len(attr_value_list) == 0:
            raise DaxClientError(
                'One or more parameter values were invalid: Invalid number of argument(s) for the IN ComparisonOperator',
                DaxErrorCode.Validation, False)

    _check_valid_BSN_type(op, *attr_value_list)
    return '{} IN ({})'.format(name, _convert_to_expr_attr_string(attr_value_list, VALUE_PREFIX, ',', expr_attr_values))

def _check_num_arguments(op, expected_args_count, attr_value_list, attr):
    size = len(attr_value_list) if attr_value_list else 0
    if not attr_value_list and expected_args_count > 0:
        raise DaxClientError(
            'One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: ' + op + ' for Attribute: ' + attr, 
         DaxErrorCode.Validation, False)

    if size != expected_args_count:
        raise DaxClientError(
            'One or more parameter values were invalid: Invalid number of argument(s) for the ' + op + ' ComparisonOperator',
            DaxErrorCode.Validation, False)

def _check_valid_BSN_type(op, *attr_vals):
    if op in ('NE', 'EQ'):
        return

    for attr_val in attr_vals:
        if 'B' not in attr_val and 'S' not in attr_val and 'N' not in attr_val:
            raise DaxClientError(
                'One or more parameter values were invalid: ComparisonOperator ' + op + ' is not valid for ' + str(list(attr_val.keys())) + ' AttributeValue type', 
                DaxErrorCode.Validation, False)

def _check_valid_BSNBoolNull_types(op, *attr_vals):
    for attr_val in attr_vals:
        if 'BOOL' in attr_val or 'NULL' in attr_val:
            continue
        _check_valid_BSN_type(op, attr_val)

def _check_valid_bounds(lower_av, upper_av):
    lb_type = list(lower_av.keys())[0]
    ub_type = list(upper_av.keys())[0]

    if lb_type != ub_type:
        raise DaxClientError(
            'One or more parameter values were invalid: AttributeValues inside AttributeValueList must be of same type',
            DaxErrorCode.Validation, False)

    if lb_type == 'S':
        if lower_av['S'] > upper_av['S']:
            raise DaxClientError(
                'The BETWEEN condition was provided a range where the lower bound is greater than the upper bound',
                DaxErrorCode.Validation, False)
    elif lb_type == 'N':
        if Decimal(lower_av['N']) > Decimal(upper_av['N']):
            raise DaxClientError(
                'The BETWEEN condition was provided a range where the lower bound is greater than the upper bound',
                DaxErrorCode.Validation, False)

