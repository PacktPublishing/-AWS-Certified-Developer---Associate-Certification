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

from . import CborTypes, CborDecoder, LexDecimal
from .DaxCborTypes import DdbSet
from .DaxError import DaxClientError, DaxErrorCode
from .CborTypes import is_major_type, major_type
from .ItemBuilder import ItemBuilder
from .Constants import STRING_TYPES, BINARY_TYPES, NUMBER_TYPES

def deanonymize_attribute_values(item, attr_names):
    ''' Resolve any attribute lists into attribute value maps in the given item.'''
    attr_values = item['_anonymous_attribute_values']

    if isinstance(attr_values, dict):
        values = {attr_names[idx]: value for idx, value in attr_values.items()}
    else:
        if len(attr_names) < len(attr_values):
            raise DaxClientError(
                'Incorrect number of attributes for attribute list (got {}; expected {})'.format(len(attr_values), len(attr_names)), 
                DaxErrorCode.MalformedResult)
        values = dict(zip(attr_names, attr_values))

    item.update(values)

    del item['_anonymous_attribute_values']
    del item['_attr_list_id']
    return item

def _decode_item_internal(dec, request, table_name=None):
    if dec.try_decode_null():
        return None

    t = dec.peek()
    if is_major_type(t, CborTypes.TYPE_MAP):
        proj_ordinals = _get_projection_ordinals(request, table_name)
        item = _decode_projection(dec, proj_ordinals)
    elif is_major_type(t, CborTypes.TYPE_BYTES):
        item = _decode_stream_item(dec.decode_cbor())
    elif is_major_type(t, CborTypes.TYPE_ARRAY):
        item = _decode_scan_result(dec, request._key_schema)
    else:
        raise DaxClientError("Unknown Item type: " + hex(major_type(t)), DaxErrorCode.MalformedResult)

    _reinsert_key(item, request)
    return item

def _get_projection_ordinals(request, table_name):
    proj_ordinals = request.get('_projection_ordinals')
    if proj_ordinals:
        return proj_ordinals

    # Batch requests
    table_proj_ordinals = request.get('_projection_ordinals_by_table')
    if table_proj_ordinals:
        if not table_name:
            raise DaxClientError('Batch request item decoding must include table name', DaxErrorCode.MalformedResult)

        return table_proj_ordinals.get(table_name, [])

    raise DaxClientError('Non-projected request has projected response', DaxErrorCode.MalformedResult)

def _decode_projection(dec, proj_ordinals):
    if proj_ordinals is None:
        raise DaxClientError('Projection ordinals list must not be null', DaxErrorCode.MalformedResult)

    builder = ItemBuilder()
    # Projections are a map[int, AttributeValue] where the key is an index into
    # projection ordinals collected during the request phase
    for _dec in dec.decode_map_iter():
        ordinal = _dec.decode_int()
        try:
            path = proj_ordinals[ordinal]
        except IndexError:
            raise DaxClientError("Unknown projection ordinal: " + ordinal, DaxErrorCode.MalformedResult)
        
        av = _decode_attribute_value(_dec)
        builder.with_value(path, av)

    return builder.build()

def _decode_stream_item(dec):
    attr_list_id = dec.decode_int()
    anon_attr_values = _decode_anonymous_streamed_values(dec)

    return {
        '_attr_list_id': attr_list_id,
        '_anonymous_attribute_values': anon_attr_values,
    }

def _decode_stream_item_projection(dec):
    # only a partial item is present that will be reconstructed during
    # de-anonymization, when the attrList is available
    # so for now, store the attributes in a map indexed by the ordinal
    
    attr_list_id = dec.decode_int()
    anon_attr_values = {}
    for _dec in dec.decode_map_iter():
        ordinal = _dec.decode_int()
        anon_attr_values[ordinal] = _decode_attribute_value(_dec)

    # If there are no values (which happens when UPDATED_OLD/NEW have no changes)
    # Pretend that there is no attrListId
    # This will result in an empty Attributes list, which is not what DDB proper does
    # It returns the changed attributes, even if the attributes didn't actually change
    if anon_attr_values:
        return {
            '_attr_list_id': attr_list_id,
            '_anonymous_attribute_values': anon_attr_values,
        }
    else:
        return {}

def _decode_scan_result(dec, key_schema):
    # check for array of length 2
    size = dec._decode_array_header()
    if size != 2:
        raise DaxClientError("Invalid scan item length {} (expected 2)".format(size), DaxErrorCode.MalformedResult)

    item = {}
    # Array item 1 -> key
    key = _decode_key_bytes(dec, key_schema)
    item.update(key)

    # Array item 1 -> value
    value = _decode_scan_value(dec)
    item.update(value)

    return item

def _decode_key_bytes(dec, key_schema):
    key = {}
    hash_attr = key_schema[0]
    hash_attr_type = hash_attr['AttributeType']
    hash_attr_name = hash_attr['AttributeName']
    if len(key_schema) == 1:
        if hash_attr_type == 'S':
            value = dec.decode_binary().decode('utf8')
        elif hash_attr_type == 'N':
            value = str(dec.decode_cbor().decode_number())
        elif hash_attr_type == 'B':
            value = dec.decode_binary()
        else:
            raise DaxClientError("Hash key must be S, B or N, got " + hash_attr_type, DaxErrorCode.MalformedResult)

        key[hash_attr_name] = {hash_attr_type: value}
    elif len(key_schema) == 2:
        key_dec = dec.decode_cbor()
        if hash_attr_type == 'S':
            hash_value = key_dec.decode_string()
        elif hash_attr_type == 'N':
            hash_value = str(key_dec.decode_number())
        elif hash_attr_type == 'B':
            hash_value = key_dec.decode_binary()
        else:
            raise DaxClientError("Hash key must be S, B or N, got " + hash_attr_type, DaxErrorCode.MalformedResult)

        key[hash_attr_name] = {hash_attr_type: hash_value}

        range_bytes = key_dec.drain()

        range_attr = key_schema[1]
        range_attr_type = range_attr['AttributeType']
        range_attr_name = range_attr['AttributeName']

        if range_attr_type == 'S':
            range_value = range_bytes.decode('utf8')
        elif range_attr_type == 'N':
            range_value = str(LexDecimal.decode_all(range_bytes))
        elif range_attr_type == 'B':
            range_value = range_bytes
        else:
            raise DaxClientError("Range key must be S, B or N, got " + range_attr_type, DaxErrorCode.MalformedResult)

        key[range_attr_name] = {range_attr_type: range_value}
    else:
        raise DaxClientError(
            "Key schema must be of length 1 or 2; got {} ({})".format(len(key_schema), key_schema),
            DaxErrorCode.MalformedResult)

    return key

def _decode_compound_key(dec):
    # Compund keys ignore the key schema and simply encode what is given
    # Used for indexed Scan/Query

    key = {}
    for _dec in dec.decode_map_iter():
        name = _dec.decode_string()
        value = _decode_attribute_value(_dec)
        key[name] = value

    return key

def _decode_scan_value(dec):
    return _decode_stream_item(dec.decode_cbor())

def _decode_anonymous_streamed_values(dec):
    # There is no delimiter on the item attributes; the AVs are concatenated
    # and must be read until there is no more data.
    values = []
    while True:
        try:
            av = _decode_attribute_value(dec)
        except CborDecoder.NoMoreData:
            break

        values.append(av)

    return values

def _decode_attribute_value(dec):
    t = dec.peek()

    if is_major_type(t, CborTypes.TYPE_ARRAY):
        return {'L': [_decode_attribute_value(_dec) for _dec in dec.decode_array_iter()]}
    elif is_major_type(t, CborTypes.TYPE_MAP):
        # Dynamo only supports maps of String -> AttributeValue
        av_map = {}
        for _dec in dec.decode_map_iter():
            name = dec.decode_string()
            av = _decode_attribute_value(_dec)
            av_map[name] = av

        return {'M': av_map}
    else:
        v = dec.decode_object()
        if v is None:
            return {'NULL': True}
        if v is True or v is False:
            return {'BOOL': v}
        elif isinstance(v, STRING_TYPES):
            return {'S': v}
        elif isinstance(v, BINARY_TYPES):
            return {'B': v}
        elif isinstance(v, NUMBER_TYPES):
            return {'N': str(v)}
        elif isinstance(v, DdbSet):
            # Set types are wrapped with a wrapper class _Set before being returned
            # This tracks the values and type
            return {v.type: v.values}
        else:
            raise DaxClientError('Unknown type', DaxErrorCode.MalformedResult)

def _reinsert_key(item, request, key_schema=None):
    # Handle GetItem, UpdateItem, DeleteItem
    if 'Key' in request and '_projection_ordinals' not in request:
        # The key attributes are only added if it's NOT a projection
        item.update(request['Key'])
        return

    # Handle PutItem
    if 'Item' in request:
        for key_attr in key_schema:
            key_attr_name = key_attr['AttributeName']
            if key_attr_name not in request['Item']:
                raise DaxClientError(
                        'Request Item is missing key attribute "{}".'.format(key_attr_name),
                        DaxErrorCode.MalformedResult);
            
            item[key_attr_name] = request['Item'][key_attr_name]
        
        return

