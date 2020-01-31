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

from __future__ import unicode_literals

import six
from .DocumentPath import DocumentPath

class ItemBuilder(object):
    def __init__(self):
        self._parts = []

    def build(self):
        # This ensures that paths with different list indexes are reconstructed
        # in document order, not request order
        self._parts.sort()

        # It's marginally simpler to start with a fake map and throw the top
        # level away than handle the top-level case
        item = {'M': {}}
        for path, av in self._parts:
            _merge(item, _build_path(path, av), 'M')

        _fixup_lists(item)

        return item['M']

    def with_value(self, path, av):
        if isinstance(path, DocumentPath):
            path = path.elements

        self._parts.append((path, av))

def _merge(existing, new, t):
    for k in new[t]:
        if k not in existing[t]:
            existing[t][k] = new[t][k]
        else:
            if _is_map(existing[t][k]):
                _merge(existing[t][k], new[t][k], 'M')
            elif _is_list(existing[t][k]):
                _merge(existing[t][k], new[t][k], 'L')
            else:
                raise TypeError("Unexpected type: " + type(existing[t][k]).__name__)

def _build_path(path, av):
    if len(path) == 0:
        return av
    else:
        head, tail = path[0], path[1:]
        e = _build_value(head, _build_path(tail, av))
        return e

def _build_value(index, value):
    if _is_key(index):
        return {'M': {index: value}}
    elif _is_ordinal(index):
        # Store the index in case any other paths are the same index into the list
        return {'L': {index: value}}
    else:
        raise TypeError('index must be a string or integer type, got {}'.format(type(index).__name__))

def _fixup_lists(item):
    if _is_list(item):
        item['L'] = _flatten_list(item['L'])
    elif _is_map(item):
        for value in item['M'].values():
            _fixup_lists(value)
    else:
        # Leaf AV, stop
        return

def _flatten_list(ml):

    if isinstance(ml, dict):
        # Flatten a map list into a list sorted by index
        flat = []
        for index, value in sorted(ml.items()):
            _fixup_lists(value)
            flat.append(value)
        return flat
    else:
        # Nevermind, it's a regular list, just return it
        return ml

def _is_key(index):
    return isinstance(index, six.string_types)

def _is_ordinal(index):
    return isinstance(index, six.integer_types)

def _is_map(c):
    return len(c) == 1 and 'M' in c

def _is_list(c):
    return len(c) == 1 and 'L' in c

