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

import threading
import time
import types

from collections import OrderedDict

class SimpleCache(object):
    ''' THIS CLASS MUST BE THREAD SAFE '''
    def __init__(self, size, fetcher):
        self.size = size
        self._fetcher = fetcher
        self._cache = OrderedDict()
        self._lock = threading.RLock()
        
    def get(self, key, tube):
        with self._lock:
            while True:
                try:
                    return self._get(key)
                except KeyError:
                    if key in self._cache:
                        # The item is actually there, but is a condition
                        # Try again from the top
                        continue

                    try:
                        value = self._fetch(key, tube)
                    except KeyError:
                        continue

                    self._put(key, value)
                    return value

    def _get(self, key):
        # This must be called from within a lock context
        value = self._cache[key]

        # Conditions are used as sentinels to track when an object is in
        # the process of being fetched, so multiple threads do not repeat
        # work.
        if _is_condition(value):
            value.wait(1.0)
            raise KeyError(key)
        else:
            return value

    def _put(self, key, value):
        # This must be called from within a lock context
        self._cache[key] = value
        if len(self._cache) > self.size:
            # Remove the least-recently-added item
            self._cache.popitem(last=False)

    def _fetch(self, key, tube):
        # This must be called from within a lock context
        # Release the lock while fetching
        cond = threading.Condition(self._lock)
        self._cache[key] = cond

        self._lock.release()
        try:
            new_value = self._fetcher(key, tube)
        except:
            self._lock.acquire()
            # Fetch failed
            # If nothing else has updated the key, remove the condition so
            # that another thread can try again
            if self._cache[key] is cond:
                del self._cache[key]
            cond.notify_all()
            raise

        self._lock.acquire()
        try:
            cur_value = self._cache[key]
            if cur_value is cond:
                # Only update if no one else has overwritten
                self._cache[key] = new_value
                return new_value
            else:
                # Updated by somebody else, pretend it doesn't exist
                raise KeyError(key)
        finally:
            cond.notify_all()

class RefreshingCache(SimpleCache):
    def __init__(self, size, fetcher, ttl_millis, clock=time.time):
        super(RefreshingCache, self).__init__(size, fetcher)
        self.ttl_millis = ttl_millis
        self.clock = clock

    def _get(self, key):
        # This must be called from within a lock context
        try:
            value, expiry = super(RefreshingCache, self)._get(key)
        except KeyError:
            raise

        now = self.clock()
        if now >= expiry:
            # If it's expired, remove it and pretend it was never there
            del self._cache[key]
            raise KeyError(key)
        else:
            return value

    def _put(self, key, value):
        # This must be called from within a lock context
        now = self.clock()
        expiry = now + self.ttl_millis/1000.0
        return super(RefreshingCache, self)._put(key, (value, expiry))

def _is_condition(value):
    # On newer versions Condition is a class, on older it's a function returning a _Condition
    ct = threading.Condition if not isinstance(threading.Condition, types.FunctionType) else threading._Condition
    return isinstance(value, ct)
