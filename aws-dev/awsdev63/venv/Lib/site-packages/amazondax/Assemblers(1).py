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

from __future__ import print_function

import socket

from collections import defaultdict

from . import CborTypes, AttributeValueDecoder
from .Constants import DaxResponseParam
from .DaxError import DaxValidationError

# Do not re-order these fields; the indexes match the values hard-coded on the server
ENDPOINT_FIELDS = ('node', 'hostname', 'address', 'port', 'role', 'az', 'leader_session_id')
_ADDRESS_FIELD_IX = ENDPOINT_FIELDS.index('address')
_IP_ADDRESS_FMT = '{:d}.{:d}.{:d}.{:d}'

def endpoints_455855874_1(_, tube):
    endpoints = tube.read_array()
    return [{ENDPOINT_FIELDS[k]: _fixup_endpoint(k, v) for k, v in ep.items()} for ep in endpoints]

def _fixup_endpoint(key, value):
    if key == _ADDRESS_FIELD_IX:
        # Return the address as a string, as that is what Python's connect method uses
        return socket.inet_ntoa(bytes(value))
    else:
        return value

def defineKeySchema_N742646399_1(_, tube):
    schemaMap = tube.read_map()
    return [{'AttributeName': attrName, 'AttributeType': attrType} for attrName, attrType in schemaMap.items()]

def defineAttributeList_670678385_1(_, tube):
    return tube.read_array()

def defineAttributeListId_N1230579644_1(_, tube):
    return tube.read_int()

def batchGetItem_N697851100_1(request, tube):
    size = tube.dec._decode_array_header()
    if size != 2:
        raise DaxValidationError("Incorrect array size: " + str(size))

    # result[0] -> Responses
    responses = {}
    for table_dec in tube.dec.decode_map_iter():
        table_name = table_dec.decode_string()
        proj_ordinals = request._projection_ordinals_by_table.get(table_name)
        items = []

        if proj_ordinals:
            for item_dec in table_dec.decode_array_iter():
                item = AttributeValueDecoder._decode_item_internal(item_dec, request, table_name)
                items.append(item)
        else:
            key_schema = request._key_schema_by_table[table_name]
            num_items = table_dec._decode_array_header()
            for _ in range(num_items, 0, -2):
                key = AttributeValueDecoder._decode_key_bytes(table_dec, key_schema)
                item = AttributeValueDecoder._decode_stream_item(table_dec.decode_cbor())
                item.update(key)
                items.append(item)

        responses[table_name] = items

    # result[1] -> UnprocessedKeys
    unprocessed_keys = {}
    for upk_dec in tube.dec.decode_map_iter():
        request_info = {}
        table_name = upk_dec.decode_string()
        key_schema = request._key_schema_by_table[table_name]
        keys = [AttributeValueDecoder._decode_key_bytes(key_dec, key_schema) \
                for key_dec in upk_dec.decode_array_iter()]

        if keys:
            request_info['Keys'] = keys

            for request_field in ('ProjectionExpression', 'ConsistentRead', 'AttributesToGet', 'ExpressionAttributeNames'):
                if request_field in request.RequestItems[table_name] and request.RequestItems[table_name][request_field]:
                    request_info[request_field] = request.RequestItems[table_name][request_field]

            if request_info:
                unprocessed_keys[table_name] = request_info

    result = {
        'Responses': responses,
        'UnprocessedKeys': unprocessed_keys
    }

    # ConsumedCapacity
    consumed_capacity = [_decode_consumed_capacity(cc_dec.decode_cbor()) for cc_dec in tube.dec.decode_array_iter()]
    if 'ReturnConsumedCapacity' in request and request.ReturnConsumedCapacity != 'NONE' and consumed_capacity:
        # TODO Handle cases where no CC is returned b/c table came from cache
        result['ConsumedCapacity'] = consumed_capacity

    return result

def batchWriteItem_116217951_1(request, tube):
    unprocessed_items_by_table = defaultdict(list)

    for _dec in tube.dec.decode_map_iter():
        table_name = _dec.decode_string()
        num_items = _dec._decode_array_header()
        key_schema = request._key_schema_by_table[table_name]

        for _ in range(num_items, 0, -2):
            key = AttributeValueDecoder._decode_key_bytes(_dec, key_schema)
            if _dec.try_decode_null():
                # DeleteRequest
                unprocessed_items_by_table[table_name].append({'DeleteRequest': {'Key': key}})
            else:
                # PutRequest
                item = AttributeValueDecoder._decode_stream_item(_dec.decode_cbor())
                # These get de-anonymized later
                unprocessed_items_by_table[table_name].append({'PutRequest': {'Item': item}})

    result = {
        'UnprocessedItems': dict(unprocessed_items_by_table)
    }

    consumed_capacity = [_decode_consumed_capacity(cc_dec.decode_cbor()) for cc_dec in tube.dec.decode_array_iter()]
    
    if 'ReturnConsumedCapacity' in request and request.ReturnConsumedCapacity != 'NONE' and consumed_capacity:
        result['ConsumedCapacity'] = consumed_capacity

    item_collection_metrics = defaultdict(list)

    for table_dec in tube.dec.decode_map_iter():
        table_name = table_dec.decode_string()
        key_schema = request._key_schema_by_table[table_name]
        for ic_dec in table_dec.decode_array_iter():
            item_collection_metrics[table_name].append(_decode_item_collection_metrics(ic_dec, key_schema))

    if item_collection_metrics:
        result['ItemCollectionMetrics'] = item_collection_metrics

    return result

def getItem_263244906_1(request, tube):
    return _decodeNormalOperation(request, tube)

def putItem_N2106490455_1(request, tube):
    return _decodeNormalOperation(request, tube)

def updateItem_1425579023_1(request, tube):
    return _decodeNormalOperation(request, tube)

def deleteItem_1013539361_1(request, tube):
    return _decodeNormalOperation(request, tube)

def query_N931250863_1(request, tube):
    result = _decodeNormalOperation(request, tube)
    return result

def scan_N1875390620_1(request, tube):
    result = _decodeNormalOperation(request, tube)
    return result

def _decodeNormalOperation(request, tube):
    result = {}
    if tube.try_read_null():
        return result

    for _dec in tube.dec.decode_map_iter():
        param = _dec.decode_int()
        decoder = _PARAM_DECODERS[param]

        decoder(_dec, request, result)

    return result

def _missing(dec, request, result):
    pass

def decode_Item(dec, request, result):
    result['Item'] = AttributeValueDecoder._decode_item_internal(dec, request)

def decode_Items(dec, request, result):
    result['Items'] = [AttributeValueDecoder._decode_item_internal(_dec, request) for _dec in dec.decode_array_iter()]

def decode_ConsumedCapacity(dec, request, result):
    if dec.try_decode_null():
        return

    consumed_capacity = _decode_consumed_capacity(dec.decode_cbor())
    if 'ReturnConsumedCapacity' in request and request.ReturnConsumedCapacity != 'NONE' and consumed_capacity:
        result['ConsumedCapacity'] = consumed_capacity

def _decode_consumed_capacity(dec):
    consumed_capacity = {}
    consumed_capacity['TableName'] = dec.decode_string()
    consumed_capacity['CapacityUnits'] = dec.decode_number()
    if not dec.try_decode_null():
        consumed_capacity['Table'] = {'CapacityUnits': dec.decode_number()}

    if not dec.try_decode_null():
        consumed_capacity['GlobalSecondaryIndexes'] = _decode_index_consumed_capacity(dec)

    if not dec.try_decode_null():
        consumed_capacity['LocalSecondaryIndexes'] = _decode_index_consumed_capacity(dec)

    return consumed_capacity

def _decode_index_consumed_capacity(dec):
    idx_cc = {}
    for _dec in dec.decode_map_iter():
        index_name = _dec.decode_string()
        units = _dec.decode_number()
        idx_cc[index_name] = {'CapacityUnits': units}

    return idx_cc

def decode_Attributes(dec, request, result):
    return_values = request.get('ReturnValues')
    is_projection = return_values and return_values in ('UPDATED_NEW', 'UPDATED_OLD')

    if is_projection:
        item = AttributeValueDecoder._decode_stream_item_projection(dec.decode_cbor())
    else:
        item = AttributeValueDecoder._decode_stream_item(dec.decode_cbor())
        if item:
            AttributeValueDecoder._reinsert_key(item, request, request._key_schema)
    
    result['Attributes'] = item

def decode_Count(dec, request, result):
    result['Count'] = dec.decode_int()

def decode_ScannedCount(dec, request, result):
    result['ScannedCount'] = dec.decode_int()

def decode_LastEvaluatedKey(dec, request, result):
    if 'IndexName' in request:
        last_eval_key = AttributeValueDecoder._decode_compound_key(dec.decode_cbor())
    else:
        last_eval_key = AttributeValueDecoder._decode_key_bytes(dec, request._key_schema)

    result['LastEvaluatedKey'] = last_eval_key

def decode_ItemCollectionMetrics(dec, request, result):
    result['ItemCollectionMetrics'] = _decode_item_collection_metrics(dec, request._key_schema)

def _decode_item_collection_metrics(dec, key_schema):
    if dec.try_decode_null():
        return None

    _dec = dec.decode_cbor()
    key_av = AttributeValueDecoder._decode_attribute_value(_dec)
    size_lower = _dec.decode_float()
    size_upper = _dec.decode_float()
    item_collection_metrics = {
        'ItemCollectionKey': {key_schema[0]['AttributeName']: key_av},
        'SizeEstimateRangeGB': [size_lower, size_upper]
    }

    return item_collection_metrics

_PARAM_DECODERS = {
    DaxResponseParam.Item: decode_Item,
    DaxResponseParam.ConsumedCapacity: decode_ConsumedCapacity,
    DaxResponseParam.Attributes: decode_Attributes,
    DaxResponseParam.ItemCollectionMetrics: decode_ItemCollectionMetrics,
    DaxResponseParam.Items: decode_Items,
    DaxResponseParam.Count: decode_Count,
    DaxResponseParam.LastEvaluatedKey: decode_LastEvaluatedKey,
    DaxResponseParam.ScannedCount: decode_ScannedCount,
    # These are only in batch operations and are handled differently
    # DaxResponseParam.Responses: ...,
    # DaxResponseParam.UnprocessedKeys: ...,
    # DaxResponseParam.UnprocessedItems: ...,
}

_PARAM_NAME = {val: name for name, val in vars(DaxResponseParam).items() if not name.startswith('__')}

