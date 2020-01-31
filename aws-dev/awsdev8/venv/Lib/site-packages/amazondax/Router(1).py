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

import random
import threading
import time
import socket
import itertools

from concurrent.futures import ThreadPoolExecutor, wait as future_wait
from collections import defaultdict
from contextlib import closing

from . import ClusterUtil
from .DaxError import DaxClientError, DaxErrorCode

import logging
logger = logging.getLogger(__name__)

class Role(object):
    LEADER = 1
    REPLICA = 2

    @staticmethod
    def name(n):
        if n == Role.LEADER:
            return "LEADER"
        elif n == Role.REPLICA:
            return "REPLICA"
        else:
            raise ValueError("Unknown role %d" % n)

class Router(object):
    ''' Determine which nodes to route requests to. 

    This class takes ownership of the backends passed in to add_route
    '''
    def __init__(self, client_factory, health_check_interval, selector=None, executor=None):
        self.client_factory = client_factory
        self.selector = selector or random_backend

        # backends is a dict[role, dict[(hostname,port,node), backend]
        self.backends = defaultdict(dict)
        self._pending_backends = {}

        self._health_check_interval = health_check_interval

        self._lock = threading.Lock()
        self._route_change = threading.Condition(self._lock)

        self._background = executor or ThreadPoolExecutor(max_workers=2)

    @property
    def all_backends(self):
        return list(self._all_backends())

    def _all_backends(self):
        return itertools.chain.from_iterable(role_backends.values() for role, role_backends in self.backends.items())

    @property
    def leader_backends(self):
        return list(self.backends[Role.LEADER].values())

    def close(self):
        with self._lock:
            all_backends = self.all_backends

        closers = [self._background.submit(backend.close) for backend in all_backends]
        done, not_done = future_wait(closers, timeout=5.0)
        # TODO What to do with not_done?

        with self._lock:
            self.backends.clear()
            self.backends = None

        self._background.shutdown()

    def update(self, service_endpoints):
        ''' Update the set of connected endpoints.

        If an endpoint has changed roles, move it. If it is new, create a new backend.
        If a backend is not in the new endpoint list, close & remove it.
        '''
        pending = []
        with self._lock:
            for ep in service_endpoints:
                self._update_endpoint(ep)

            pending.extend(self._purge_endpoints(service_endpoints))

        done, not_done = future_wait(pending, timeout=5.0)
        # TODO What to do with not_done?

        logger.debug('Current backends: %s', self.backends)

    def _update_endpoint(self, service_endpoint):
        ''' Update any endpoints that have changed or are new.
        
        Not thread-safe. Must be called from inside update().
        '''
        ep_key = service_endpoint.addrport
        ep_role = service_endpoint.role

        backend = self.backends[ep_role].get(ep_key)
        if backend is not None:
            # Backend is up-to-date, no changes necessary
            backend.update(service_endpoint)
        else:
            # Role may have changed, so search other roles to see if we're already connected
            for role, role_backends in self.backends.items():
                if role == ep_role:
                    # Already checked...
                    continue

                backend = role_backends.pop(ep_key, None)
                if backend is not None:
                    # Backend has only changed roles, so just move it
                    backend.update(service_endpoint)
                    self.backends[ep_role][ep_key] = backend
                    logger.debug('%s changing role from %s to %s', backend, Role.name(role), Role.name(ep_role))
                    break
            else:
                # Backend does not exist, so create it
                # It will add itself when it is ready
                self._create_backend(service_endpoint)

    def _purge_endpoints(self, service_endpoints):
        ''' Remove backends that no longer have corresponding endpoints.
        
        Not thread-safe. Must be called from inside update().
        '''
        endpoints = {ep.addrport for ep in service_endpoints}

        pending = []
        for role_backends in self.backends.values():
            to_remove = []
            for addrport, backend in role_backends.items():
                if addrport not in endpoints:
                    # This backend is no longer in the service endpoints, so close it
                    # It will remove itself when closed
                    logger.debug('Removing unused backend %s', backend)
                    pending.append(self._background.submit(backend.close)) # Method ref, not call
                    to_remove.append(addrport) # Can't delete while looping

            # Remove all keys
            for addrport in to_remove:
                del role_backends[addrport]

        return pending

    def _create_backend(self, service_endpoint):
        ''' Create a new Backend instance and add it to the pending list.

        If a backend is already pending, do nothing.
        
        Not thread-safe. Must be called from inside update().
        '''
        pending_backend = self._pending_backends.get(service_endpoint.addrport)
        if pending_backend is None:
            logger.debug('Creating backend for %s', service_endpoint)
            backend = Backend(
                    service_endpoint, 
                    self.client_factory, 
                    self._health_check_interval,
                    self._backend_up,
                    self._backend_down)

            # Bring the backend up in the background
            up_f = self._background.submit(backend.up)
            self._pending_backends[backend.addrport] = up_f
            # When the backend is up (or up() fails), remove it from the pending list
            up_f.add_done_callback(lambda f: self._pending_backends.pop(backend.addrport, None))
        else:
            # There is already a pending backend for this endpoint,
            # so let it continue instead
            pass

    def _backend_up(self, backend):
        with self._lock:
            self.backends[backend.role][backend.addrport] = backend
            self._route_change.notify_all()
        logger.debug('Backend up: %s', backend)

    def _backend_down(self, backend):
        with self._lock:
            del self.backends[backend.role][backend.addrport]
            self._route_change.notify_all()
        logger.debug('Backend down: %s', backend)

    def wait_for_routes(self, min_healthy, leader_min, timeout=None):
        start = _clock()
        while True:
            with self._lock:
                if self._has_active_any(min_healthy) and self._has_active_leaders(leader_min):
                    return
                else:
                    now = _clock()
                    if timeout is not None and now - start > timeout:
                        raise DaxClientError(
                            "Not enough routes after {}s: expected {}/{}, found {}/{} (healthy/leaders)".format(
                                timeout, min_healthy, leader_min, self._active_any(), self._active_leaders()), 
                            DaxErrorCode.NoRoute)

                # Wait for a signal that the endpoints have changed
                self._route_change.wait(timeout)
    
    def _active_any(self):
        return sum(len(role_backend) for role, role_backend in self.backends.items())

    def _has_active_any(self, min_healthy):
        if min_healthy < 1:
            return True
        else:
            return self._active_any() >= min_healthy

    def _active_leaders(self):
        return len(self.backends[Role.LEADER])

    def _has_active_leaders(self, leader_min):
        if leader_min < 1:
            return True
        else:
            return self._active_leaders() >= leader_min

    def next_leader(self, prev_client):
        ''' Returns the next leader entry that is not the given prev value if one such entry is available.
        
        If there is only one entry and that is equals to prev, prev is returned.
        Returns None if nothing is available.
        '''

        with self._lock:
            if self.backends is not None:
                return self.selector(prev_client, self.leader_backends)

    def next_any(self, prev_client):
        ''' Returns any entry that is not the given prev value, if any such entry is available.
            
        If there is only one entry and that is equals to prev, prev is returned.
        Returns None if nothing is available.
        '''
        with self._lock:
            if self.backends is not None:
                return self.selector(prev_client, self.all_backends)

def random_backend(prev_client, backends):
    ''' Select a random available client. '''
    if not backends:
        return None
    elif len(backends) == 1:
        return backends[0]
    else:
        # Select a random backend
        i = random.randrange(len(backends))
        be = backends[i]
        if be.client is prev_client:
            # Just pick the next one
            be = backends[(i + 1) % len(backends)]
        return be

class Backend(object):
    MIN_ERROR_COUNT_FOR_UNHEALTHY = 5

    @staticmethod
    def _noop(*args, **kwargs):
        pass

    def __init__(self, service_endpoint, client_factory, health_check_interval, on_up=None, on_down=None):
        self._service_endpoint = service_endpoint
        self._client_factory = client_factory
        self._health_check_interval = health_check_interval
        self._connection_timeout = self._health_check_interval / 2

        self._on_up = on_up or Backend._noop
        self._on_down = on_down or Backend._noop

        self._error_count = 0
        self._error_count_for_unhealthy = Backend.MIN_ERROR_COUNT_FOR_UNHEALTHY

        self.client = None
        self.session = None
        self.active = False
        self._healthy = True  # Healthy until proven otherwise
        self._closed = False

        self._lock = threading.Lock()
        self._timer = None

    def update(self, new_service_endpoint):
        if self._service_endpoint != new_service_endpoint:
            if self._service_endpoint.addrport != new_service_endpoint.addrport:
                raise ValueError('Cannot update backend to new address.')

            self._service_endpoint = new_service_endpoint
            return True

        return False

    def close(self):
        if self._closed:
            return

        self.down()

        # TODO if connecting, cancel (when I switch to connect with a future)

        self._closed = True
    
    @property
    def addrport(self):
        return self._service_endpoint.addrport

    @property
    def role(self):
        return self._service_endpoint.role

    @property
    def leader(self):
        return self._service_endpoint.role == Role.LEADER

    @property
    def healthy(self):
        return self._healthy \
                and self._error_count < self._error_count_for_unhealthy \
                and (self.client is None or self.client._tube_pool is not None)

    def up(self):
        if self._closed:
            return

        upped = False
        with self._lock:
            if not self.active:
                # TODO There should be an IO exception callback passed to the client
                # for error detection and counting
                self.client = self._client_factory(*self.addrport)
                self._error_count = 0
                self.active = True
                upped = True

        if upped:
            self._on_up(self)
            with self._lock:
                if self._timer:
                    self._timer.cancel()

                self._timer = ClusterUtil.periodic_task(
                        self._health_check_task,
                        self._health_check_interval,
                        self._health_check_interval * 0.1)

    def down(self):
        if not self.active or self._closed:
            return

        self._on_down(self)

        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None

            self.active = False
            if self.client:
                self.client.close()
                self.client = None

    def health_check(self):
        if self._closed:
            return False

        start = _clock()

        # Determine health by opening a connection to the server and immediately closing
        try:
            with closing(socket.create_connection(self.addrport, self._connection_timeout)):
                end = _clock()
        except OSError:
            return False
        else:
            self._ping = end-start

        return True
    
    def _health_check_task(self):
        if self._closed or not self.active:
            # Skip background health checks
            return

        if not self.health_check():
            # Don't immediately fail on a single health check failure
            self._error_count += 1
            logger.debug("Health check failed for %s (%d/%d)", self, self._error_count, self._error_count_for_unhealthy)

        if not self.healthy:
            self.down()
        else:
            # host is healthy, reset the error counts from piling up and prevent host from going down unnecessarily.
            self._error_count = 0

    def __repr__(self):
        return 'Backend({})'.format(self._service_endpoint)

def _clock():
    return time.time()

