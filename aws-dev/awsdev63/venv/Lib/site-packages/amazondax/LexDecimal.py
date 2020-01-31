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

import math
import struct
import six
from decimal import Decimal, localcontext

from . import compat
from .CborEncoder import DYNAMODB_CONTEXT

NULL_BYTE_LOW = 0       # Byte to use for undefined, low ordering
NULL_BYTE_HIGH = 0xff   # Byte to use for undefined, high ordering

def create_decimal(d):
    ''' Create Decimal instances using the proper context.
    
    This uses the same Context settings as botocore for consistency.
    '''
    return DYNAMODB_CONTEXT.create_decimal(d)

def encode(value, mask=0, context=None):
    if value is None:
        raise ValueError('value must not be None')

    if not isinstance(value, Decimal):
        value = create_decimal(value)

    if value == 0:
        return compat.bytelist([0x80 ^ mask])

    value_parts = value.as_tuple()
    encoded = bytearray(_estimateLength(value_parts))

    with localcontext(context or DYNAMODB_CONTEXT) as ctx:
        ctx.prec = ctx.prec + 2  # Include extra precision for handling unscaled values
        offset = _encode(value, value_parts, encoded, 0, mask)
    return bytes(encoded[:offset])

def _encode(value, value_parts, dst, offset, mask):
    precision = len(value_parts.digits)
    scale = -value_parts.exponent
    exponent = precision - scale

    # Do the header
    if value.is_signed():
        # Negative
        if exponent >= -0x3e and exponent < 0x3e:
            dst[offset] = (0x3f - exponent) ^ mask
            offset += 1
        else:
            dst[offset] = 0x7e ^ mask if exponent < 0 else 0x01 ^ mask
            # Force values to unsigned to write raw bits
            _writeInt32BE(dst, (exponent & 0xffffffff) ^ mask ^ 0x7fffffff, offset + 1)
            offset += 5
    else:
        # Positive or zero
        if exponent >= -0x3e and exponent < 0x3e:
            dst[offset] = (exponent + 0xc0) ^ mask
            offset += 1
        else:
            dst[offset] = 0x81 ^ mask if exponent < 0 else 0xfe ^ mask
            # Force values to unsigned to write raw bits
            _writeInt32BE(dst, (exponent & 0xffffffff) ^ mask ^ 0x80000000, offset + 1);
            offset += 5;

    # Now do the significand.
    with localcontext() as ctx:
        # Use a higher scaling value for intermediate calcs
        ctx.Emax = max(ctx.Emax, scale)
        scaler = ctx.create_decimal((0, (1, ), scale))
        unscaled_value = value * scaler

    # Ensure a non-fractional amount of base-1000 digits.
    m = precision % 3
    if m == 1:
        terminator = 0
        unscaled_value *= 100
    elif m == 2:
        terminator = 1
        unscaled_value *= 10
    else:
        terminator = 2

    # 10 bits per base-1000 digit and 1 extra terminator digit. Digit values 0..999 are
    # encoded as 12..1011. Digit values 0..11 and 1012..1023 are used for terminators

    if not value.is_signed():
        # Positive
        digit_adj = 12
    else:
        digit_adj = 999 + 12
        terminator = 1023 - terminator

    pos = int(math.floor(math.ceil(precision / math.log(2) / 3.0 * math.log(1000) + 9) / 10) + 1)
    digits = [0] * pos
    pos -= 1
    digits[pos] = terminator

    while unscaled_value != 0:
        divrem = (unscaled_value // 1000, unscaled_value % 1000)

        pos -= 1
        if pos < 0:
            # Handle rare case when an extra digit is required.
            digits.insert(0, 0)
            pos = 0

        digits[pos] = int(divrem[1]) + digit_adj
        unscaled_value = divrem[0]

    # Now encode digits in proper order, 10 bits per digit. 1024 possible
    # values per 10 bits, and so base 1000 is quite efficient.

    accum = 0
    bits = 0
    for digit in digits:
        accum = (accum << 10) | digit
        bits += 10
        while bits >= 8:
            bits -= 8
            dst[offset] = ((accum >> bits) ^ mask) & 0xff
            offset += 1

    if bits != 0:
        dst[offset] = ((accum << (8 - bits)) ^ mask) & 0xff
        offset += 1

    return offset

def _estimateLength(value_parts):
    # Exactly predicting encoding length is hard, so overestimate to be safe.

    # Calculate number of base-1000 digits, rounded up. Add two more digits to account for
    # the terminator and a rare extra digit.
    # Probably a faster way, given how slow Decimal.as_tuple() is...
    digits = 2 + int(math.ceil(len(value_parts.digits) / 3.0))

    # Calculate the number of bytes required to encode the base-1000 digits, at 10 bits
    # per digit. Add 5 for the maximum header length, and add 7 for round-up behavior when
    # dividing by 8.
    return 5 + (((digits * 10) + 7) >> 3)

def decode_all(data, mask=0, context=None):
    if not isinstance(data, (bytes, bytearray)):
        # TODO or memoryview?
        raise TypeError('data must be bytes or bytearray, got ' + type(value).__name__)

    with localcontext(context or DYNAMODB_CONTEXT) as ctx:
        value, offset = _decode(data, 0, mask, ctx)
        return value

def decode(src, offset, mask=0, context=None):
    if not isinstance(src, (bytes, bytearray)):
        # TODO or memoryview?
        raise TypeError('src must be bytes or bytearray, got ' + type(value).__name__)

    with localcontext(context or DYNAMODB_CONTEXT) as ctx:
        return _decode(src, offset, mask, ctx)

def _decode(src, offset, mask, context):
    orig_offset = offset

    header = six.indexbytes(src, offset) ^ mask

    if header == NULL_BYTE_HIGH or header == NULL_BYTE_LOW:
        return (None, 1)
    elif header == 0x7f or header == 0x80:
        return (context.create_decimal(0), 1)
    elif header == 0x01 or header == 0x7e:
        digit_adj = 999 + 12
        exponent = _readInt32BE(src, offset + 1) ^ mask ^ 0x7fffffff
        if header == 0x7e:
            # Correct negative exponents, as written values are unsigned
            exponent = (exponent & 0x7fffffff) - (exponent & 0x80000000)
        offset += 5
    elif header == 0x81 or header == 0xfe:
        digit_adj = 12
        exponent = _readInt32BE(src, offset + 1) ^ mask ^ 0x80000000
        if header == 0x81:
            # Correct negative exponents, as written values are unsigned
            exponent = (exponent & 0x7fffffff) - (exponent & 0x80000000)
        offset += 5
    else:
        exponent = six.indexbytes(src, offset) ^ mask
        offset += 1
        if exponent >= 0x82:
            digit_adj = 12
            exponent -= 0xc0
        else:
            digit_adj = 999 + 12
            exponent = 0x3f - exponent

    # Significand is base 1000 encoded, 10 bits per digit.
    unscaled_value = None
    precision = 0

    accum = 0
    bits = 0
    last_digit = None

    while True:
        accum = (accum << 8) | (six.indexbytes(src, offset) ^ mask)
        offset += 1
        bits += 8
        if bits >= 10:
            digit = (accum >> (bits - 10)) & 0x3ff

            if digit == 0 or digit == 1023:
                last_digit //= 100
                unscaled_value = last_digit if unscaled_value is None else unscaled_value.fma(10, last_digit)
                precision += 1
                break
            elif digit == 1 or digit == 1022:
                last_digit //= 10
                unscaled_value = last_digit if unscaled_value is None else unscaled_value.fma(100, last_digit)
                precision += 2
                break
            elif digit == 2 or digit == 1021:
                unscaled_value = last_digit if unscaled_value is None else unscaled_value.fma(1000, last_digit)
                precision += 3
                break
            else:
                if unscaled_value is None:
                    unscaled_value = last_digit
                    if unscaled_value is not None:
                        precision += 3
                else:
                    unscaled_value = unscaled_value.fma(1000, last_digit)
                    precision += 3

                bits -= 10
                last_digit = context.create_decimal(digit - digit_adj)

    scale = precision - exponent
    with localcontext() as ctx:
        # Allow extra scale for intermediate values 
        ctx.Emax = max(ctx.Emax, -scale)
        scaler = ctx.create_decimal((0, (1,), -scale))
        value = unscaled_value * scaler

    return (value, offset - orig_offset)

def compareUnsigned(a, b):
    if not isinstance(a, (bytes, bytearray)):
        raise TypeError('lhs must be bytes or bytearray')
    if not isinstance(b, (bytes, bytearray)):
        raise TypeError('rhs must be bytes or bytearray')

    return _compareUnsigned(a, 0, len(a), b, 0, len(b))

def _compareUnsigned(a, aoff, alen, b, boff, blen):
    minLen = min(alen, blen)
    for i in range(minLen):
        ab = a[aoff + i]
        bb = b[boff + i]
        if ab != bb:
            return (ab & 0xff) - (bb & 0xff)
    
    return alen - blen;

def _writeInt32BE(buf, value, offset):
    struct.pack_into('>I', buf, offset, value)

def _readInt32BE(buf, offset):
    return struct.unpack_from('>I', buf, offset)[0]

