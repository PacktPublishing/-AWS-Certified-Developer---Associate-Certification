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

# Type encoding sizes measured in bits.
SIZE_8 = 0b00011000
SIZE_16 = 0b00011001
SIZE_32 = 0b00011010
SIZE_64 = 0b00011011
SIZE_ILLEGAL_1 = 0b00011100
SIZE_ILLEGAL_2 = 0b00011101
SIZE_ILLEGAL_3 = 0b00011110
SIZE_STREAM = 0b00011111

# Upper 3 bits of type header defines the major type.
MAJOR_TYPE_MASK = 0b11100000

# Lower 5 bits of type header defines the minor type.
MINOR_TYPE_MASK = 0b00011111

# Positive integer types.
TYPE_POSINT = 0b00000000 # 0..23
TYPE_POSINT_8 = TYPE_POSINT + SIZE_8
TYPE_POSINT_16 = TYPE_POSINT + SIZE_16
TYPE_POSINT_32 = TYPE_POSINT + SIZE_32
TYPE_POSINT_64 = TYPE_POSINT + SIZE_64

# Negative integer types.
TYPE_NEGINT = 0b00100000 # -1..-24
TYPE_NEGINT_8 = TYPE_NEGINT + SIZE_8
TYPE_NEGINT_16 = TYPE_NEGINT + SIZE_16
TYPE_NEGINT_32 = TYPE_NEGINT + SIZE_32
TYPE_NEGINT_64 = TYPE_NEGINT + SIZE_64

# Byte string types.
TYPE_BYTES = 0b01000000 # 0..23 bytes in length
TYPE_BYTES_8 = TYPE_BYTES + SIZE_8
TYPE_BYTES_16 = TYPE_BYTES + SIZE_16
TYPE_BYTES_32 = TYPE_BYTES + SIZE_32
TYPE_BYTES_64 = TYPE_BYTES + SIZE_64
TYPE_BYTES_STREAM = TYPE_BYTES + SIZE_STREAM

# UTF-8 string types.
TYPE_UTF = 0b01100000 # 0..23 bytes in length
TYPE_UTF_8 = TYPE_UTF + SIZE_8
TYPE_UTF_16 = TYPE_UTF + SIZE_16
TYPE_UTF_32 = TYPE_UTF + SIZE_32
TYPE_UTF_64 = TYPE_UTF + SIZE_64
TYPE_UTF_STREAM = TYPE_UTF + SIZE_STREAM

# Array types.
TYPE_ARRAY = 0b10000000 # 0..23 elements
TYPE_ARRAY_8 = TYPE_ARRAY + SIZE_8
TYPE_ARRAY_16 = TYPE_ARRAY + SIZE_16
TYPE_ARRAY_32 = TYPE_ARRAY + SIZE_32
TYPE_ARRAY_64 = TYPE_ARRAY + SIZE_64
TYPE_ARRAY_STREAM = TYPE_ARRAY + SIZE_STREAM

# Map types.
TYPE_MAP = 0b10100000 # 0..23 element pairs
TYPE_MAP_8 = TYPE_MAP + SIZE_8
TYPE_MAP_16 = TYPE_MAP + SIZE_16
TYPE_MAP_32 = TYPE_MAP + SIZE_32
TYPE_MAP_64 = TYPE_MAP + SIZE_64
TYPE_MAP_STREAM = TYPE_MAP + SIZE_STREAM

# Tagged types.
TYPE_TAG = 0b11000000 # for tag type 0..23
TYPE_TAG_8 = TYPE_TAG + SIZE_8
TYPE_TAG_16 = TYPE_TAG + SIZE_16
TYPE_TAG_32 = TYPE_TAG + SIZE_32
TYPE_TAG_64 = TYPE_TAG + SIZE_64

# Simple and special types.
TYPE_SIMPLE = 0b11100000 # not a real type
TYPE_FALSE = TYPE_SIMPLE + 0b00010100
TYPE_TRUE = TYPE_SIMPLE + 0b00010101
TYPE_NULL = TYPE_SIMPLE + 0b00010110
TYPE_UNDEFINED = TYPE_SIMPLE + 0b00010111
TYPE_SIMPLE_8 = TYPE_SIMPLE + SIZE_8 # next byte specifies type 32..255
TYPE_FLOAT_16 = TYPE_SIMPLE + SIZE_16
TYPE_FLOAT_32 = TYPE_SIMPLE + SIZE_32
TYPE_FLOAT_64 = TYPE_SIMPLE + SIZE_64
TYPE_BREAK = TYPE_SIMPLE + SIZE_STREAM

# A few standard tags.
TAG_DATETIME = 0 # string
TAG_TIMESTAMP = 1 # seconds from epoch
TAG_POSBIGINT = 2
TAG_NEGBIGINT = 3
TAG_DECIMAL = 4
TAG_BIGFLOAT = 5

# return type
RET_INT = 0
RET_UTF = 1
RET_BUF = 2
RET_BIGINT = 3
RET_BIGDEC = 4
RET_FLOAT = 5
RET_TAG = 6
RET_BOOL = 7
RET_NULL = 8
RET_UNDEFINED = 9
RET_MAP_HEADER = 10
RET_ARR_HEADER = 11
RET_STREAM_BREAK = 12

def is_major_type(v, t):
    return  (v & MAJOR_TYPE_MASK) == t

def major_type(v):
    return v & MAJOR_TYPE_MASK

def minor_type(v):
    return v & MINOR_TYPE_MASK

def major_type_name(v):
    ''' Attempt to find major type name for the given value. '''
    t = major_type(v)

    type_names = [name for name, value in globals().items() if name.startswith('TYPE') and value == t]
    return '|'.join(type_names) if type_names else 'UNKNOWN'

