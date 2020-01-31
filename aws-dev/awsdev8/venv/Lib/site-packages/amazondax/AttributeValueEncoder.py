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

import re
import operator
import json
import six
from decimal import Decimal
from collections import defaultdict

from .DaxError import DaxValidationError
from .CborEncoder import CborEncoder, DYNAMODB_CONTEXT
from .Constants import DaxDataRequestParam, BINARY_TYPES
from .DocumentPath import DocumentPath
from . import LexDecimal, DaxCborTypes
from .compat import to_bytes

def encode_key(item, schema):
    enc = CborEncoder()
    encode_key_direct(enc, item, schema)
    return enc.as_bytes()

def encode_key_direct(enc, item, schema):
    hashAttrDefn = schema[0]
    hashAttrVal = item[hashAttrDefn['AttributeName']]
    if not hashAttrVal:
        raise DaxValidationError('One of the required keys was not given a value.')

    hashAttrType = hashAttrDefn['AttributeType']
    hashVal = hashAttrVal[hashAttrType]
    if not hashVal:
        raise DaxValidationError('One of the required keys was not given a value.')

    if len(schema) == 2:
        if hashAttrType == 'S':
            enc.append_string(hashVal)
        elif hashAttrType == 'N':
            enc.append_number(hashVal)
        elif hashAttrType == 'B':
            enc.append_binary(hashVal)
        else:
            raise DaxValidationError('Unsupported KeyType encountered in Hash Attribute: ' + str(hashAttrType)) 

        rangeAttrDefn = schema[1]
        rangeAttrVal = item[rangeAttrDefn['AttributeName']]
        if not rangeAttrVal:
            raise DaxValidationError('One of the required keys was not given a value.')
        
        rangeAttrType = rangeAttrDefn['AttributeType']
        rangeVal = rangeAttrVal[rangeAttrType]
        if rangeAttrType == 'S':
            enc.append_raw(to_bytes(rangeVal, 'utf8'))
        elif rangeAttrType == 'N':
            enc.append_raw(LexDecimal.encode(DYNAMODB_CONTEXT.create_decimal(rangeVal)))
        elif rangeAttrType == 'B':
            enc.append_raw(rangeVal)
        else:
            raise DaxValidationError('Unsupported KeyType encountered in Range Attribute: ' + str(rangeAttrType))

    elif len(schema) == 1:
        if hashAttrType == 'S':
            enc.append_raw(to_bytes(hashVal, 'utf8'))
        elif hashAttrType == 'N':
            enc.append_number(hashVal)
        elif hashAttrType == 'B':
            enc.append_raw(hashVal)
        else:
            raise DaxValidationError('Unsupported KeyType encountered in Hash Attribute: ' + str(hashAttrType))
    
    else:
        raise DaxValidationError("Unexpected key length: " + str(len(schema)))

def encode_compound_key(key):
    enc = CborEncoder()
    encode_compound_key_direct(enc, key)
    return enc.as_bytes()

def encode_compound_key_direct(enc, key):
    # This must be a map stream or the server will error
    enc.append_map_stream_header()

    for name, av in key.items():
        enc.append_string(name)
        encode_attribute_value_direct(enc, av)

    enc.append_break()

def encode_values(item, key_schema, attr_names, attr_list_id):
    enc = CborEncoder()

    attr_names = attr_names or _get_canonical_attribute_list(item, key_schema)
    encode_attributes_direct(enc, item, attr_names, attr_list_id)
    
    return enc.as_bytes()

def encode_attributes_direct(enc, item, attr_names, attr_list_id):
    enc.append_int(attr_list_id)
    for attr in attr_names:
        av = item[attr]
        encode_attribute_value_direct(enc, av)

def encode_attribute_value(av):
    enc = CborEncoder()
    encode_attribute_value_direct(enc, av)
    return enc.as_bytes()

def encode_attribute_value_direct(enc, av):
    if len(av) != 1:
        raise DaxValidationError('Attribute Values must have exactly one key, got: ' + str(list(av.keys())))

    # Yes, this loop will only execute once, but it's the easiest way to get at the value...
    for t, value in av.items():
        if t == 'S':
            enc.append_string(value)
        elif t == 'N':
            enc.append_number(value)
        elif t == 'B':
            enc.append_binary(value)
        elif t == 'SS':
            if len(value) == 0:
                raise DaxValidationError('Supplied AttributeValue is empty, must contain exactly one of the supported datatypes')
            enc.append_tag(DaxCborTypes.TAG_DDB_STRING_SET)
            enc.append_array_header(len(value))
            for s in value:
                enc.append_string(s)
        elif t == 'NS':
            if len(value) == 0:
                raise DaxValidationError('Supplied AttributeValue is empty, must contain exactly one of the supported datatypes')
            enc.append_tag(DaxCborTypes.TAG_DDB_NUMBER_SET)
            enc.append_array_header(len(value))
            for n in value:
                enc.append_number(n)
        elif t == 'BS':
            if len(value) == 0:
                raise DaxValidationError('Supplied AttributeValue is empty, must contain exactly one of the supported datatypes')
            enc.append_tag(DaxCborTypes.TAG_DDB_BINARY_SET)
            enc.append_array_header(len(value))
            for b in value:
                enc.append_binary(b)
        elif t == 'M':
            enc.append_map_header(len(value))
            for key, sub_av in sorted(value.items()):
                enc.append_string(key)
                encode_attribute_value_direct(enc, sub_av)
        elif t == 'L':
            enc.append_array_header(len(value))
            for sub_av in value:
                encode_attribute_value_direct(enc, sub_av)
        elif t == 'NULL':
            enc.append_null()
        elif t == 'BOOL':
            enc.append_boolean(value)
        else:
            raise DaxValidationError("Unknown Attribute Value type '{}'".format(t))

        break # Ensure that it only executes once

def encode_expressions(request):
    enc = CborEncoder()
    encode_expressions_direct(enc, request)
    return enc.as_bytes()

def encode_key_condition_expression_direct(enc, request):
    expressions = _parse_expressions_only(request)

    if 'KeyConditionExpression' in request:
        enc.append_binary(expressions.KeyCondition)
    else:
        enc.append_null()

    return expressions

def encode_expressions_direct(enc, request):
    expressions = _parse_expressions_only(request)
    write_expressions_direct(enc, expressions, request)

def write_expressions_direct(enc, expressions, request):
    if expressions.Condition:
        enc.append_int(DaxDataRequestParam.ConditionExpression)
        enc.append_binary(expressions.Condition)

    if expressions.Filter:
        enc.append_int(DaxDataRequestParam.FilterExpression)
        enc.append_binary(expressions.Filter)

    if expressions.Update:
        enc.append_int(DaxDataRequestParam.UpdateExpression)
        enc.append_binary(expressions.Update)

    if expressions.Projection:
        # Store the projection ordinals for use reading the response item
        request['_projection_ordinals'] = _prepare_projection(
                request.ProjectionExpression, expressions.ExpressionAttributeNames)

        enc.append_int(DaxDataRequestParam.ProjectionExpression)
        enc.append_binary(expressions.Projection)

    if expressions.ExpressionAttributeNames:
        enc.append_int(DaxDataRequestParam.ExpressionAttributeNames)
        enc.append_map_header(len(expressions.ExpressionAttributeNames))
        for k, v in sorted(expressions.ExpressionAttributeNames.items()):
            enc.append_string(k)
            enc.append_string(v)
        
    if expressions.ExpressionAttributeValues:
        enc.append_int(DaxDataRequestParam.ExpressionAttributeValues)
        enc.append_map_header(len(expressions.ExpressionAttributeValues))
        for k, v in sorted(expressions.ExpressionAttributeValues.items()):
            enc.append_string(k)
            encode_attribute_value_direct(enc, v)

def encode_ExclusiveStartKey_direct(enc, request):
    if 'IndexName' in request:
        enc.append_binary(encode_compound_key(request.ExclusiveStartKey))
    else:
        enc.append_binary(encode_key(request.ExclusiveStartKey, request._key_schema))

def encode_batchGetItem_N697851100_1_getItemKeys_direct(enc, request):
    # _validate_batchGetItem(request)
    table_proj_ordinals = defaultdict(list)
    key_set = set()

    enc.append_map_header(len(request.RequestItems))

    for table_name, table_request in request.RequestItems.items():
        enc.append_string(table_name)
        enc.append_array_header(3)

        # Item 1: ConsistentRead
        consistent_read = table_request.get('ConsistentRead', False)
        enc.append_boolean(consistent_read)

        # Item 2: Projection Expression
        proj_expression = table_request.get('ProjectionExpression')
        if proj_expression is not None:
            expressions = _parse_expressions_only(table_request)
            table_proj_ordinals[table_name] = _prepare_projection(
                    proj_expression, expressions.ExpressionAttributeNames)
            enc.append_binary(expressions.Projection)
        else:
            enc.append_null()

        keys = table_request.get('Keys', [])

        # Item 3: Keys array
        enc.append_array_header(len(keys))
        for key in keys:
            key_bytes = encode_key(key, request._key_schema_by_table[table_name])
            if key_bytes in key_set:
                raise DaxValidationError('Provided list of item keys contains duplicates:' + json.dumps(key))

            key_set.add(key_bytes)
            enc.append_binary(key_bytes)

    # Store on the request for decoding
    request._projection_ordinals_by_table = dict(table_proj_ordinals)

WRITE_TYPES = {'PutRequest', 'DeleteRequest'}
MAX_WRITE_BATCH_SIZE = 25

def encode_batchWriteItem_116217951_1_keyValuesByTable_direct(enc, request):
    # TODO Run all validation prior to writing anything to the stream
    enc.append_map_header(len(request.RequestItems))
    
    key_set = set()
    total_requests = 0
    for table_name, write_requests in request.RequestItems.items():
        key_schema = request._key_schema_by_table[table_name]
        key_set.clear()

        total_requests += len(write_requests)
        if total_requests > MAX_WRITE_BATCH_SIZE:
            raise DaxValidationError(
                    "Batch size should be less than {}, got {}".format(MAX_WRITE_BATCH_SIZE, total_requests))

        enc.append_string(table_name)

        request_item_count = 0
        is_empty = True
        request_item_count = sum(sum(1 for write_type in write_request if write_type in WRITE_TYPES) \
                for write_request in write_requests)

        enc.append_array_header(request_item_count * 2)

        for write_request in write_requests:
            is_empty = False
            if 'PutRequest' in write_request:
                put_request = write_request['PutRequest']
                item = put_request['Item']
                key_bytes = encode_key(item, key_schema)
                if key_bytes in key_set:
                    raise DaxValidationError('Provided list of item keys contains duplicates:' + json.dumps(item))

                key_set.add(key_bytes)

                enc.append_binary(key_bytes)
                enc.append_binary(
                        encode_values(item, key_schema, put_request['_attr_names'], put_request['_attr_list_id']))

            if 'DeleteRequest' in write_request:
                del_request = write_request['DeleteRequest']
                key = del_request['Key']
                key_bytes = encode_key(key, request._key_schema_by_table[table_name])
                if key_bytes in key_set:
                    raise DaxValidationError('Provided list of item keys contains duplicates:' + json.dumps(key))

                key_set.add(key_bytes)

                enc.append_binary(key_bytes)
                enc.append_null()

        if is_empty:
            raise DaxValidationError("No write requests provided for " + table_name)

    if total_requests == 0:
        raise DaxValidationError("No write requests provided")

def _parse_expressions_only(request):
    # import here to avoid circular reference
    from . import CborSExprGenerator

    expr_attr_names = request.get('ExpressionAttributeNames', {})
    expr_attr_values = request.get('ExpressionAttributeValues', {})

    _check_valid_expression_parameter_names(expr_attr_names, expr_attr_values)

    expressions = CborSExprGenerator.encode_expressions(
        request.get('ConditionExpression'),
        request.get('KeyConditionExpression'),
        request.get('FilterExpression'),
        request.get('UpdateExpression'),
        request.get('ProjectionExpression'),
        expr_attr_names,
        expr_attr_values)

    return expressions

def _check_valid_expression_parameter_names(expr_attr_names, expr_attr_values):
    if expr_attr_names:
        invalid = _check_expression_params(expr_attr_names.keys(), '#')
        if invalid:
            raise DaxValidationError('ExpressionAttributeNames contains invalid key: "' + invalid + '"')

    if expr_attr_values:
        invalid = _check_expression_params(expr_attr_values.keys(), ':')
        if invalid:
            raise DaxValidationError('ExpressionAttributeValues contains invalid key: "' + invalid + '"')

_EXPRESSION_PARAM_NAME_RX = re.compile(r'^[#:][A-Za-z0-9_]+$')
def _check_expression_params(key_names, prefix):
    if not key_names:
        return

    for name in key_names:
        if name[0] != prefix or not _EXPRESSION_PARAM_NAME_RX.match(name):
            return name

def _prepare_projection(expr, expr_attr_names):
    return [DocumentPath.from_path(path.strip(), expr_attr_names) for path in expr.split(',')]

def _get_canonical_attribute_list(item, key_schema):
    key_names = {key['AttributeName'] for key in key_schema}
    return tuple(_as_string(attr_name) for attr_name in sorted(set(item.keys()) - key_names))

def _as_string(s):
    ''' Convert a byte string to a text string if necessary.

    DAX requires the attribute list to be Strings, and the default encoder under Python 2 sends str as a byte array.

    This is probably going to cause errors somewhere, but such is life in Python 2. It uses the default encoding
    as these strings are most likely from the source code.
    '''
    return s.decode() if isinstance(s, BINARY_TYPES) else s

