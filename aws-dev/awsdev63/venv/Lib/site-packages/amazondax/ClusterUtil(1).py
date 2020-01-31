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
import random
import time

class PeriodicTask(threading.Thread):
    def __init__(self, period, func, args=None, kwargs={}, jitter=None, clock=time.time):
        super(PeriodicTask, self).__init__(name='PeriodicTask-' + getattr(func, '__name__', ''))
        self.daemon = True # Ensure tasks do not block shutdown

        self.period = period
        self.func = func
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.jitter = jitter
        self.tick = threading.Event()
        self.starttime = clock()
        self._clock = clock

    @property
    def cancelled(self):
        return self.tick.is_set()

    def cancel(self):
        # This flags the event (and thus task) as cancelled
        self.tick.set()

    def run(self):
        while not self.tick.is_set():
            # Lock the interval to the system clock, so that it's periodic
            interval = self.period - ((self._clock() - self.starttime) % self.period)

            # Add random jitter if requested
            jit = random.uniform(-self.jitter, self.jitter) if self.jitter else 0.0

            # Wait for the given interval
            self.tick.wait(interval + jit)

            if not self.tick.is_set():
                # If it hasn't been cancelled, run the function
                self.func(*self.args, **self.kwargs)

def periodic_task(func, period, jitter=None):
    ''' period -> seconds '''
    task_man = PeriodicTask(period, func, jitter=jitter)
    task_man.start()
    return task_man

