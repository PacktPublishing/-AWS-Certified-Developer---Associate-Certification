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

TAG_DDB_STRING_SET = 3321
TAG_DDB_NUMBER_SET = 3322
TAG_DDB_BINARY_SET = 3323
TAG_DDB_DOCUMENT_PATH_ORDINAL = 3324

class DdbSet(object):
    def __init__(self, set_type, values):
        self.values = values
        self.type = set_type

    def __repr__(self):
        return 'DdbSet({}, {})'.format(self.type, self.values)

class DocumentPathOrdinal(object):
    def __init__(self, ordinal):
        self.ordinal = ordinal

