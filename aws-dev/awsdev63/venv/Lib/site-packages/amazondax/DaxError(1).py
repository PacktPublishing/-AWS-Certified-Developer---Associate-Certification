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

from collections import defaultdict

from botocore.exceptions import ClientError as BotoClientError

class DaxErrorCode(object):
    Decoder = 'DecoderException'
    Unrecognized = 'UnrecognizedClientException'
    Authentication = 'MissingAuthenticationTokenException'
    MalformedResult = 'MalformedResultException'
    EndOfStream = 'EndOfStreamException'
    IllegalArgument = 'IllegalArgumentException'
    Validation = 'ValidationException'
    NoRoute = 'NoRouteException'
    ResourceNotFound = 'ResourceNotFoundException'
    ResourceInUse = 'ResourceInUseException'
    ProvisionedThroughputExceeded = 'ProvisionedThroughputExceededException'
    ConditionalCheckFailed = 'ConditionalCheckFailedException'
    InternalServerError = 'InternalServerErrorException'
    ItemCollectionSizeLimitExceeded = 'ItemCollectionSizeLimitExceededException'
    LimitExceeded = 'LimitExceededException'
    Throttling = 'ThrottlingException'
    AccessDenied = 'AccessDeniedException'


class DaxClientError(BotoClientError):
    def __init__(self, message, ddb_code, retryable=False):
        error_response, code = _make_error_response(message, None, None, ddb_code, None)
        super(DaxClientError, self).__init__(error_response, '')

        self.code = code
        self.codes = None
        self.retryable = retryable
        self.auth_error = False
        self.wait_for_recovery_before_retry = False
        self._tube_invalid = False

class DaxValidationError(DaxClientError):
    def __init__(self, message):
        super(DaxValidationError, self).__init__(message, DaxErrorCode.Validation, False)

class DaxServiceError(BotoClientError):
    def __init__(self, operation_name, message, dax_codes, request_id, ddb_code, http_status):
        error_response, code = _make_error_response(message, dax_codes, request_id, ddb_code, http_status)
        super(DaxServiceError, self).__init__(error_response, operation_name)

        self.code = code
        self.codes = dax_codes
        self.retryable = _determine_retryability(self.codes, code or ddb_code, http_status)
        self.auth_error = _determine_auth_error(self.codes)
        self.wait_for_recovery_before_retry = _determine_wait_for_recovery_before_retry(self.codes)
        self._tube_invalid = _determine_tube_validity(self.codes)

def _make_error_response(message, codes, request_id, ddb_code, http_status):
    # Make an error response that mimics Boto's for compatibility
    yodict = lambda: defaultdict(yodict)  # A recursive default dict
    response = yodict()

    code = _pick_error_code(codes) or ddb_code
    if code is not None:
        response['Error']['Code'] = code

    if message:
        response['Error']['Message'] = message

    if request_id:
        response['ResponseMetadata']['RequestId'] = request_id

    if http_status:
        response['ResponseMetadata']['HTTPStatusCode'] = http_status

    # Convert to a regular dict for compatibility
    return _unwrap_defaultdict(response), code

def _unwrap_defaultdict(d):
    if isinstance(d, defaultdict):
        return {k: _unwrap_defaultdict(v) for k, v in d.items()}
    else:
        return d

def _pick_error_code(codes):
    if not codes:
        return None

    if codes == [3, 37, 54]:
        return DaxErrorCode.InternalServerError

    if codes[0] != 4:
        return None

    code = None
    lookup = ERROR_MAP
    for e in codes[1:]:
        lookup = lookup.get(e)
        if lookup is None:
            break
    else:
        code = lookup

    return code

def _determine_retryability(codes, code, http_status):
    return (codes and codes[0] != 4) \
            or code in RETRYABLE_ERRORS \
            or http_status in (500, 503)

def _determine_auth_error(codes):
    return codes is not None \
            and len(codes) >= 3 \
            and codes[1] == 23 and codes[2] == 31

def _determine_wait_for_recovery_before_retry(codes):
    return len(codes) >= 1 and codes[0] == 2

def _determine_tube_validity(codes):
    return len(codes) >= 4 \
            and codes[1] == 23 and codes[2] == 31 \
            and 32 <= codes[3] <= 34

ERROR_MAP = {
    23: {
        24: DaxErrorCode.ResourceNotFound,
        35: DaxErrorCode.ResourceInUse,
    },
    37: {
        38: {
            39: {
                40: DaxErrorCode.ProvisionedThroughputExceeded,
                41: DaxErrorCode.ResourceNotFound,
                43: DaxErrorCode.ConditionalCheckFailed,
                45: DaxErrorCode.ResourceInUse,
                46: DaxErrorCode.Validation,
                47: DaxErrorCode.InternalServerError,
                48: DaxErrorCode.ItemCollectionSizeLimitExceeded,
                49: DaxErrorCode.LimitExceeded,
                50: DaxErrorCode.Throttling,
                },
            42: DaxErrorCode.AccessDenied,
            44: (DaxErrorCode.Validation, 'NotImplementedException'),
        }
    }
}

RETRYABLE_ERRORS = {
    DaxErrorCode.Throttling, 
    DaxErrorCode.ProvisionedThroughputExceeded, 
    DaxErrorCode.InternalServerError,
    DaxErrorCode.LimitExceeded
}
