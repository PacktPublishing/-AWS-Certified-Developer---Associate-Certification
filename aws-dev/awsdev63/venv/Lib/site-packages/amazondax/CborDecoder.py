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

# TODO: Better buffer management - track pointers in the buffer rather than
# trimming the front

import struct
import binascii
import io

from collections import OrderedDict

from .CborTypes import *
from .CborEncoder import DYNAMODB_CONTEXT

class NoMoreData(Exception):
    pass

class CborDecoder(object):
    ''' Decode CBOR from a stream.

    Data is provided by the `more` function, which is:

      more(buf: buffer, n: int) -> int

    It should add at lest n bytes to the buffer from whatever source. Returns the number of bytes requested,
    or -1 if no more bytes are available and never will be - socket closed, EOF, etc.

    TODO Buffer handling is very naive, as it just resizes the bytearray. Better
    would be to track locations in the buffer to avoid reallocating.

    TODO Ignore unexpected tags during all decoding.

    TODO Add chunked reads of fixed-size byte records (decode_bytes_iter?)
    '''
    def __init__(self, more, tag_handlers=None):
        self.more = more
        self.buffer = bytearray()

        self.tag_handlers = _DEFAULT_TAG_HANDLERS.copy()
        if tag_handlers:
            self.tag_handlers.update(tag_handlers)

    def peek(self):
        self._ensure_available(1)
        return self.buffer[0]

    def skip(self):
        # skip to the next entry
        # TODO Do this more efficiently, without creating an object and just reading types
        self.decode_object()

    def _consume(self, n=1):
        del self.buffer[:n]

    def drain(self):
        ''' Return all available remaining bytes. 
        
        This assumes a fixed amount of data (as in a nested decoder) or a
        suitable end-of-stream, otherwise it may try to run forever.
        '''
        while True:
            try:
                self._ensure_available(4096)
            except NoMoreData:
                break

        result = bytes(self.buffer)
        del self.buffer[:]
        return result

    def decode_string(self):
        t = self.peek()
        if not is_major_type(t, TYPE_UTF):
            raise DaxClientError('Type is not string: ' + major_type_name(t), DaxErrorCode.Decoder)

        length = self._decode_value(t)

        if length != -1:
            self._ensure_available(length)
            result = self.buffer[:length].decode('utf-8')
            self._consume(length)
        else:
            f = io.StringIO()
            while not self._try_decode_break():
                f.write(self.decode_string())
            result = f.getvalue()

        return result

    def decode_binary(self):
        t = self.peek()
        if not is_major_type(t, TYPE_BYTES):
            raise DaxClientError('Type is not bytes: ' + major_type_name(t), DaxErrorCode.Decoder)

        length = self._decode_value(t)

        if length != -1:
            self._ensure_available(length)
            result = self.buffer[:length]
            self._consume(length)
        else:
            f = io.BytesIO()
            while not self._try_decode_break():
                f.write(self.decode_binary())
            result = f.getvalue()

        return bytes(result)

    def decode_number(self):
        t = self.peek()
        if t in (TYPE_FLOAT_16, TYPE_FLOAT_32, TYPE_FLOAT_64):
            return self.decode_float()

        mt = major_type(t)
        if mt in (TYPE_POSINT, TYPE_NEGINT):
            return self.decode_int()
        elif mt == TYPE_TAG:
            tag = self._decode_tag(t)
            if tag in (TAG_POSBIGINT, TAG_NEGBIGINT):
                return _decode_bigint(self, tag)
            elif tag == TAG_DECIMAL:
                return _decode_decimal(self, tag)

        raise DaxClientError('Type is not a number: ' + major_type_name(t), DaxErrorCode.Decoder)

    def decode_int(self):
        t = self.peek()

        if major_type(t) not in (TYPE_POSINT, TYPE_NEGINT):
            if is_major_type(t, TYPE_TAG):
                tag = self._decode_tag(t)
                if tag in (TAG_POSBIGINT, TAG_NEGBIGINT):
                    return _decode_bigint(self, tag)

            raise DaxClientError('Type is not int: ' + major_type_name(t), DaxErrorCode.Decoder)

        v = self._decode_value(t)
        return v if is_major_type(t, TYPE_POSINT) else -v - 1

    def decode_decimal(self):
        t = self.peek()
        if not is_major_type(t, TYPE_TAG):
            raise DaxClientError('Decimal value must have TAG_DECIMAL tag.', DaxErrorCode.Decoder)

        tag = self._decode_tag(t)
        return _decode_decimal(self, tag)
    
    def decode_float(self):
        t = self.peek()

        if t == TYPE_FLOAT_16:
            self._ensure_available(3)
            result = _parseHalf(self.buffer[1:3])
            excess = 2
        elif t == TYPE_FLOAT_32:
            self._ensure_available(5)
            result, = struct.unpack_from('!f', self.buffer, 1)
            excess = 4
        elif t == TYPE_FLOAT_64:
            self._ensure_available(9)
            result, = struct.unpack_from('!d', self.buffer, 1)
            excess = 8
        else:
            raise DaxClientError('Type is not float: ' + major_type_name(t), DaxErrorCode.Decoder)
        
        self._consume(1+excess)
        return result

    def decode_map(self):
        # A simple dict comprehension doesn't work here, because the order of call _dec.decode_object() doesn't work
        # The calls need to be split into seperate variables
        def _():
            for _dec in self.decode_map_iter():
                k = _dec.decode_object()
                v = _dec.decode_object()
                yield k, v

        return OrderedDict(_())

    def decode_map_iter(self):
        ''' Allow for iterative decoding of maps, rather than reading the whole map in.
        
        This function returns a generator that produces CborDecoder instances that can be used to read the next
        elements. It handles both fixed-length and streaming maps.

        Example:
            for _dec in dec.read_map_iter():
                key = _dec.decode_object()
                value = _dec.decode_object()
                result[key] = value
        '''
        size = self._decode_map_header()

        i = 0
        while i != size:
            if self._try_decode_break():
                break

            yield self
            i += 1

    def decode_array(self):
        return [_dec.decode_object() for _dec in self.decode_array_iter()]

    def decode_array_iter(self):
        ''' Allow for iterative decoding of arrays, rather than reading the whole array in.
        
        This function returns a generator that produces CborDecoder instances that can be used to read the next
        elements. It handles both fixed-length and streaming arrays.

        Example:
            for _dec in dec.read_array_iter():
                value = _dec.decode_object()
                result.append(value)
        '''
        size = self._decode_array_header()

        i = 0
        while i != size:
            if self._try_decode_break():
                break
            
            yield self
            i += 1

    def decode_object(self):
        if self.try_decode_null():
            return None

        t = self.peek()

        # Check simple types first
        if t == TYPE_NULL or t == TYPE_UNDEFINED:
            self._consume(1)
            return None
        elif t == TYPE_TRUE:
            self._consume(1)
            return True
        elif t == TYPE_FALSE:
            self._consume(1)
            return False
        elif t in (TYPE_FLOAT_16, TYPE_FLOAT_32, TYPE_FLOAT_64):
            return self.decode_float()
        elif t == TYPE_BREAK:
            raise DaxClientError('Unexpected break', DaxErrorCode.Decoder)
        else:
            # proceed to complex types
            pass

        mt = major_type(t)
        if mt == TYPE_POSINT or mt == TYPE_NEGINT:
            return self.decode_int()
        elif mt == TYPE_BYTES:
            return self.decode_binary()
        elif mt == TYPE_UTF:
            return self.decode_string()
        elif mt == TYPE_ARRAY:
            return self.decode_array()
        elif mt == TYPE_MAP:
            return self.decode_map()
        elif mt == TYPE_TAG:
            return self._decode_tagged_type(t)
        else:
            raise DaxClientError('Unhandled type: ' + major_type_name(t), DaxErrorCode.Decoder)

    def decode_cbor(self):
        ''' Read nested CBOR stored in a bytes record.

        Returns a CborDecoder instance that can be used to read the nested CBOR.
        
        TODO Used a chunked reading (decode_binary_iter) when available, or tie the nested more function into the
        parent more, rather than reading the whole bytes record.
        '''
        data = self.decode_binary()

        def more(buf, n):
            # Normally would use nonlocal if 2.x support not needed
            if more.p >= len(data):
                return -1
            q = more.p+n
            buf += data[more.p:q]
            more.p = q
            return n
        more.p = 0

        dec = self.__class__(more)
        return dec

    def _decode_map_header(self):
        t = self.peek()
        if not is_major_type(t, TYPE_MAP):
            raise DaxClientError('Type is not map (was {})'.format(major_type_name(t)), DaxErrorCode.Decoder)

        return self._decode_value(t)

    def _decode_array_header(self):
        t = self.peek()
        if not is_major_type(t, TYPE_ARRAY):
            raise DaxClientError('Type is not array (was {})'.format(major_type_name(t)), DaxErrorCode.Decoder)

        return self._decode_value(t)

    def _decode_tag(self, t):
        if not is_major_type(t, TYPE_TAG):
            raise DaxClientError('Type is not a tag (was {})'.format(major_type_name(t)), DaxErrorCode.Decoder)

        return self._decode_value(t)

    def _decode_tagged_type(self, t):
        tag = self._decode_tag(t)
        if tag in self.tag_handlers:
            return self.tag_handlers[tag](self, tag)
        else:
            raise DaxClientError('Unknown tag ' + str(tag), DaxErrorCode.Decoder)

    def _try_decode_break(self):
        val = self.peek()

        if val == TYPE_BREAK:
            self._consume()
            return True
        else:
            return False

    def try_decode_null(self):
        val = self.peek()

        if val == TYPE_NULL:
            self._consume()
            return True
        else:
            return False

    def _decode_value(self, v):
        size = minor_type(v)
        if size < SIZE_8:
            result = size
            excess = 0
        elif size == SIZE_8:
            self._ensure_available(2)
            result = self.buffer[1]
            excess = 1
        elif size == SIZE_16:
            self._ensure_available(3)
            result, = struct.unpack_from("!H", self.buffer, 1)
            excess = 2
        elif size == SIZE_32:
            self._ensure_available(5)
            result, = struct.unpack_from("!I", self.buffer, 1)
            excess = 4
        elif size == SIZE_64:
            self._ensure_available(9)
            result, = struct.unpack_from("!Q", self.buffer, 1)
            excess = 8
        elif size == SIZE_STREAM:
            result = -1
            excess = 0
        else:
            raise DaxClientError('Invalid value size', DaxErrorCode.Decoder)
    
        self._consume(1+excess)
        return result

    def _ensure_available(self, n):
        ''' Ensure that at least n bytes are available in the buffer.

        This will call the `more` callback until the desired amount is reached.
        '''
        while len(self.buffer) < n:
            l = n - len(self.buffer)
            m = self.more(self.buffer, l)
            if m < 0:
                raise NoMoreData()

def _decode_bigint(dec, tag):
    if tag not in (TAG_POSBIGINT, TAG_NEGBIGINT):
        raise DaxClientError('Invalid tag to decode BigInt: ' + hex(tag), DaxErrorCode.Decoder)

    t = dec.peek()
    if not is_major_type(t, TYPE_BYTES):
        raise DaxClientError('Type for BigInt is not binary: ' + major_type_name(t), DaxErrorCode.Decoder)

    data = dec.decode_binary()
    val = int(binascii.hexlify(data), 16)

    return val if tag == TAG_POSBIGINT else -val - 1

def _decode_decimal(dec, tag):
    if tag != TAG_DECIMAL:
        raise DaxClientError('Decimal value must have TAG_DECIMAL tag (got ' + hex(tag) + ')', DaxErrorCode.Decoder)
    
    size = dec._decode_array_header()
    if size != 2:
        raise DaxClientError('Decimal value has wrong array size (' + str(size) + ')', DaxErrorCode.Decoder)

    exponent = dec.decode_int()
    mantissa = dec.decode_int()

    # There should be a faster way to do this than string formatting
    if mantissa >= 0:
        sign = 0
        digits = tuple(map(int, str(mantissa)))
    else:
        sign = 1
        digits = tuple(map(int, str(-mantissa)))

    return DYNAMODB_CONTEXT.create_decimal((sign, digits, exponent))


_DEFAULT_TAG_HANDLERS = {
    TAG_POSBIGINT: _decode_bigint,
    TAG_NEGBIGINT: _decode_bigint,
    TAG_DECIMAL: _decode_decimal,
}

_NaN = float('NaN')

def _parseHalf(buf):
    sign = -1 if buf[0] & 0x80 else 1
    exp = (buf[0] & 0x7C) >> 2
    mant = ((buf[0] & 0x03) << 8) | buf[1]
    if not exp:
        return sign * 5.9604644775390625e-8 * mant
    elif exp == 0x1f:
        return sign * (_NaN if mant else 2e308)
    else:
        return sign * pow(2, exp - 25) * (1024 + mant)
