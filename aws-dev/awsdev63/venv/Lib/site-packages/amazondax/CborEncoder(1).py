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

import struct
import binascii
import ast
import decimal
import six

from . import CborTypes, compat

MAX_2 = 2 ** 16 - 1
MAX_4 = 2 ** 32 - 1
MAX_8 = 2 ** 64 - 1
MIN_8 = -MAX_8 - 1

# Taken from boto3 for compatibility
DYNAMODB_CONTEXT = decimal.Context(
    Emin=-128, Emax=126, prec=38,
    traps=[decimal.Clamped, decimal.Overflow, decimal.Inexact, decimal.Rounded, decimal.Underflow])

class CborEncoder(object):
    def __init__(self):
        # Preallocate common buffers
        self.buf_2 = bytearray(2)
        self.buf_4 = bytearray(4)
        self.buf_8 = bytearray(8)

        self._buffer = bytearray()
    
    def buffer(self):
        return self._buffer

    def as_bytes(self):
        return bytes(self._buffer)

    def reset(self):
        # TODO More efficient buffer handling, with pointers
        self._buffer = bytearray()

    def _drain(self, n):
        ''' Remove the first n bytes from the buffer. '''
        del self._buffer[:n]

    def append_raw(self, data):
        self._buffer += data
        return self

    def append_int(self, n):
        if isinstance(n, six.integer_types):
            if n >= 0:
                if n <= MAX_8:
                    self._append_type(CborTypes.TYPE_POSINT, n)
                else:
                    self._append_bigint(CborTypes.TAG_POSBIGINT, n)
            else:
                if n >= MIN_8:
                    self._append_type(CborTypes.TYPE_NEGINT, -n - 1)
                else:
                    self._append_bigint(CborTypes.TAG_NEGBIGINT, -n - 1)
        else:
            raise TypeError('Expected {}, got {}'.format(six.integer_types, type(n).__name__))

        return self

    def append_float(self, f):
        self._append_type(CborTypes.TYPE_FLOAT_64)
        self._append_double(f)
        return self

    def append_decimal(self, d):
        if not isinstance(d, decimal.Decimal):
            raise TypeError('Decimals must be decimal.Decimal; got ' + type(b).__name__)

        self.append_tag(CborTypes.TAG_DECIMAL)
        self.append_array_header(2)

        exponent, mantissa = _crack_decimal(d)
        self.append_int(exponent)
        self.append_int(mantissa)

        return self

    def append_number(self, n):
        if isinstance(n, bytes):
            n = six.text_type(n, 'utf8')
        elif isinstance(n, bytearray):
            # Python 2 compat - can't decode bytearray with unicode()
            n = n.decode('utf8')

        if isinstance(n, six.text_type):
            if _is_decimal(n):
                n = DYNAMODB_CONTEXT.create_decimal(n)
            else:
                n = compat.bigint(n)

        if isinstance(n, six.integer_types):
            return self.append_int(n)
        elif isinstance(n, decimal.Decimal):
            return self.append_decimal(n)
        elif isinstance(n, float):
            return self.append_float(n)
        else:
            raise TypeError('Numbers must be strings, bytes, or a number type; got ' + type(n).__name__)

    def append_string(self, s):
        if not isinstance(s, six.string_types):
            raise TypeError("String must be of type {}, got {}".format(six.string_types, type(s).__name__))
        
        if not isinstance(s, six.text_type) and isinstance(s, bytes):
            # If we're passed a str object on Python 2, try UTF-8
            try:
                u = unicode(s, 'utf8')
            except:
                # Let any UnicodeDecodeError exceptions just bubble up
                raise
            else:
                # It's already utf8 so no need to convert it back
                b = s
        else:
            b = compat.to_bytes(s, 'utf8')

        self._append_type(CborTypes.TYPE_UTF, len(b))
        self._buffer += b
        return self

    def append_binary(self, b):
        self._append_type(CborTypes.TYPE_BYTES, len(b))
        if isinstance(b, six.text_type):
            buf = compat.to_bytes(b, 'utf8')
        elif isinstance(b, (bytes, bytearray)):
            buf = b
        else:
            raise TypeError('Binary data must be {}, bytes, or bytearray; got {}'.format(
                six.text_type, type(b).__name__))

        self._buffer += buf
        return self

    def append_boolean(self, b):
        self._buffer.append(CborTypes.TYPE_TRUE if b else CborTypes.TYPE_FALSE)
        return self

    def append_null(self):
        self._buffer.append(CborTypes.TYPE_NULL)
        return self

    def append_break(self):
        self._buffer.append(CborTypes.TYPE_BREAK)
        return self

    def append_array_header(self, size):
        self._append_type(CborTypes.TYPE_ARRAY, size)
        return self

    def append_map_header(self, size):
        self._append_type(CborTypes.TYPE_MAP, size)
        return self

    def append_string_stream_header(self):
        self._buffer.append(CborTypes.TYPE_UTF_STREAM)
        return self

    def append_byte_stream_header(self):
        self._buffer.append(CborTypes.TYPE_BYTES_STREAM)
        return self

    def append_array_stream_header(self):
        self._buffer.append(CborTypes.TYPE_ARRAY_STREAM)
        return self

    def append_map_stream_header(self):
        self._buffer.append(CborTypes.TYPE_MAP_STREAM)
        return self

    def append_tag(self, tag):
        self._append_type(CborTypes.TYPE_TAG, tag)
        return self

    def _append_type(self, major_type, val=0):
        if not isinstance(val, six.integer_types):
            raise TypeError('Type value must be an integer, got ' + type(val).__name__)
        
        if val == 0:
            self._buffer.append(major_type)
        elif val < 24:
            self._buffer.append(major_type | val)
        elif val <= 0xff:
            self._buffer.append(major_type | CborTypes.SIZE_8)
            self._buffer.append(val)
        elif val <= MAX_2:
            self._buffer.append(major_type | CborTypes.SIZE_16)
            self._append_uint16be(val)
        elif val <= MAX_4:
            self._buffer.append(major_type | CborTypes.SIZE_32)
            self._append_uint32be(val)
        elif val <= MAX_8:
            self._buffer.append(major_type | CborTypes.SIZE_64)
            self._append_uint64be(val)
        else:
            raise ValueError('Type value must be <= 2^64, got ' + str(val))

    def _append_bigint(self, tag, val):
        self.append_tag(tag)
        
        # There may be faster ways than using a string conversion but bigint
        # encoding is unlikely to be a bottleneck
        hval = '%x' % val
        bval = binascii.unhexlify('0' + hval if len(hval) % 2 != 0 else hval)

        self.append_binary(bval)
    
    def _append_uint16be(self, val):
        struct.pack_into("!H", self.buf_2, 0, val)
        self._buffer += self.buf_2

    def _append_uint32be(self, val):
        struct.pack_into("!I", self.buf_4, 0, val)
        self._buffer += self.buf_4

    def _append_uint64be(self, val):
        struct.pack_into("!Q", self.buf_8, 0, val)
        self._buffer += self.buf_8

    def _append_float(self, val):
        struct.pack_into('!f', self.buf_4, 0, val)
        self._buffer += self.buf_4

    def _append_double(self, val):
        struct.pack_into('!d', self.buf_8, 0, val)
        self._buffer += self.buf_8

def _is_decimal(s):
    for c in s:
        if c == '.' or c == 'e' or c == 'E':
            return True

    return False

def _crack_decimal(d):
    '''
    Oddly enough, this is the fastest method in Python, because as_tuple() is
    so slow:

    In [5]: timeit('str(d)', globals=globals())
    Out[5]: 0.44962017599027604

    In [6]: timeit('d.as_tuple()', globals=globals())
    Out[6]: 1.1225044820166659

    Code originally from https://stackoverflow.com/a/24945278/129592 & added
    engineering notation handling & proper 0 handling.
    '''
    base_exp = 0
    s = str(d)
    eng_parts = s.split('E')

    if len(eng_parts) == 2:
        # We have engineering notation
        base_exp = int(eng_parts[1])
        s = eng_parts[0]

    parts = s.split('.')

    if len(parts) == 2:
        # Negative exponent
        exponent = -len(parts[1])
        mantissa = int(parts[0] + parts[1])
    else:
        # Positive exponent
        digits = parts[0].rstrip('0')
        digits = digits if digits != '' else '0' # Handle 0 case
        exponent = len(parts[0]) - len(digits)
        mantissa = int(digits)

    return base_exp+exponent, mantissa

