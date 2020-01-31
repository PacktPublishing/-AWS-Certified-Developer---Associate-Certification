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

from .DaxError import DaxClientError, DaxErrorCode

class DocumentPath(object):
    def __init__(self, elements):
        self.elements = elements

    def __eq__(self, other):
        return self.elements == other.elements

    def __repr__(self):
        return 'DocumentPath(' + repr(self.elements) + ')'

    @classmethod
    def from_path(cls, path, attr_names=None):
        attr_names = attr_names or {}

        split = path.split('.')
        elements = []

        # TODO Rather than mutating element, would it be faster to 
        for element in split:
            index = element.find('[')
            if index == -1:
                elements.append(attr_names.get(element, element))
                continue
            elif index == 0:
                raise DaxClientError('Invalid path: ' + path, DaxErrorCode.Validation, False)

            initial = element[:index]
            elements.append(attr_names.get(initial, initial))

            while index != -1:
                element = element[index+1:]
                index = element.find(']')

                if index == -1:
                    raise DaxClientError('Invalid path: ' + path, DaxErrorCode.Validation, False)

                ordinal = int(element[:index])
                elements.append(ordinal)

                element = element[index+1:]
                index = element.find('[')

                if index > 0:
                    raise DaxClientError('Invalid path: ' + path, DaxErrorCode.Validation, False)

            if element:
                raise DaxClientError('Invalid path: ' + path, DaxErrorCode.Validation, False)

        return cls(elements)

