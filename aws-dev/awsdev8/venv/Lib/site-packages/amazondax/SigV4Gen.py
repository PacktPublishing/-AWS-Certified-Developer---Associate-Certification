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

# Taken mainly from http://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
# and the DaxJSClient implementation

from __future__ import unicode_literals

import hmac
import hashlib
import datetime
import six
from collections import namedtuple, OrderedDict

Signature = namedtuple('Signature', 'signature string_to_sign token')

_SIGNED_HEADERS = tuple(sorted(('host', 'x-amz-date')))

def generate_signature(creds, endpoint, region, payload, now=None):
    if not now:
        now = datetime.datetime.utcnow()

    headers = _get_headers(endpoint, now, creds)
    return _get_authorization_header('POST', now, headers, payload, creds, region)

def _get_authorization_header(method, now, headers, payload, creds, region):
    canonical_uri = '/'
    canonical_querystring = ''
    credential_scope = datestamp(now) + '/' + region + '/dax/aws4_request'
    canonical_request = '\n'.join((method, 
        canonical_uri,  
        canonical_querystring, 
        _get_canonical_headers(headers), 
        _get_signed_headers(),  
        _SHA256(payload)))

    string_to_sign = '\n'.join(('AWS4-HMAC-SHA256',
        amzdate(now),  
        credential_scope,  
        _SHA256(canonical_request.encode('utf-8'))))

    signing_key = getSignatureKey(creds.secret_key, datestamp(now), region, 'dax')
    signature = _HmacSHA256(signing_key, string_to_sign)
    token = creds.token if hasattr(creds, 'token') and creds.token else None

    return Signature(signature, string_to_sign, token)

def _SHA256(data):
    return hashlib.sha256(data).hexdigest()

def _HmacSHA256(key, data):
    return six.text_type(hmac.new(key, data.encode('utf-8'), hashlib.sha256).hexdigest())

# Key derivation functions. See:
# http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python
def sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

def getSignatureKey(key, dateStamp, regionName, serviceName):
    kDate = sign(('AWS4' + key).encode('utf-8'), dateStamp)
    kRegion = sign(kDate, regionName)
    kService = sign(kRegion, serviceName)
    kSigning = sign(kService, 'aws4_request')
    return kSigning

def _get_canonical_headers(headers):
    return '\n'.join(h + ':' + headers[h] for h in _SIGNED_HEADERS) + '\n'

def _get_signed_headers():
    return ';'.join(_SIGNED_HEADERS)

def _get_headers(host, now, creds):
    headers = OrderedDict()
    headers['host'] = host[8:] if host.startswith('https://') else host
    headers['x-amz-date'] = amzdate(now)
    if hasattr(creds, 'token') and creds.token:
        headers['x-amz-security-token'] = creds.token

    return headers

def amzdate(t):
    return t.strftime('%Y%m%dT%H%M%SZ')

def datestamp(t):
    # Date w/o time, used in credential scope
    return t.strftime('%Y%m%d') 

