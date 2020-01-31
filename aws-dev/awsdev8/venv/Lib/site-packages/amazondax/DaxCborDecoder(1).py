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

from .CborDecoder import CborDecoder
from .CborTypes import *
from .DaxCborTypes import *
from .DaxError import DaxClientError, DaxErrorCode

class DaxCborDecoder(CborDecoder):
    def __init__(self, more):
        super(DaxCborDecoder, self).__init__(more, DAX_TAG_HANDLERS)

def _decode_set(dec, tag):
    t = dec.peek()
    if not is_major_type(t, TYPE_ARRAY):
        raise DaxClientError('Type for Set is not array: ' + major_type_name(t), DaxErrorCode.Decoder)

    if tag == TAG_DDB_STRING_SET:
        set_type = 'SS'
        values = [_dec.decode_string() for _dec in dec.decode_array_iter()]
    elif tag == TAG_DDB_NUMBER_SET:
        set_type = 'NS'
        values = [str(_dec.decode_number()) for _dec in dec.decode_array_iter()]
    elif tag == TAG_DDB_BINARY_SET:
        set_type = 'BS'
        values = [_dec.decode_binary() for _dec in dec.decode_array_iter()]
    else:
        raise DaxClientError('Invalid tag to decode set: ' + hex(tag), DaxErrorCode.Decoder)

    return DdbSet(set_type, values)

def _decode_document_path_ordinal(dec, tag):
    if tag != TAG_DDB_DOCUMENT_PATH_ORDINAL:
        raise DaxClientError('Invalid tag to decode document path ordinal: ' + hex(tag), DaxErrorCode.Decoder)

    ordinal = dec.decode_int()

    return DocumentPathOrdinal(ordinal)

DAX_TAG_HANDLERS = {
    TAG_DDB_STRING_SET: _decode_set,
    TAG_DDB_NUMBER_SET: _decode_set,
    TAG_DDB_BINARY_SET: _decode_set,
    TAG_DDB_DOCUMENT_PATH_ORDINAL: _decode_document_path_ordinal
}


