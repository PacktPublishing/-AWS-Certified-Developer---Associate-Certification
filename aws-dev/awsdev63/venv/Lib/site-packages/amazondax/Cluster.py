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

import sys
import time
import socket
import random
import weakref
import threading
import traceback

from collections import namedtuple
from contextlib import closing

from . import ClusterUtil
from .Tube import SocketTubePool, _SessionVersion
from .DaxClient import DaxClient
from .DaxError import DaxServiceError, DaxClientError, DaxErrorCode
from .Router import Router, Role
from .Assemblers import ENDPOINT_FIELDS

import logging
logger = logging.getLogger(__name__)

DEFAULT_PORT = 8111

AddrPort = namedtuple('AddrPort', ('address', 'port'))
_ServiceEndpoint = namedtuple('_ServiceEndpoint', ENDPOINT_FIELDS)

class ServiceEndpoint(_ServiceEndpoint):
    @property
    def addrport(self):
        try:
            return self._addrport
        except AttributeError:
            self._addrport = AddrPort(self.address, self.port)
            return self._addrport

class Cluster(object):
    def __init__(self, region_name, seeds, credentials, user_agent=None, user_agent_extra=None, connect_timeout=None,
                 read_timeout=None, source=None, client_factory=None):
        self._region_name = region_name
        self._seeds = [_parse_host_ports(endpoint) for endpoint in seeds]
        self._credentials = credentials
        self._user_agent = user_agent
        self._user_agent_extra = user_agent_extra
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout

        self._closed = False
        self._service_endpoints = None

        # All time intervals are in seconds to match time.time
        self._cluster_update_interval = 4.0
        self._health_check_interval = 5.0
        self._idle_connection_reap_delay = 30.0
        
        self._source = source or Source(self._seeds, self._cluster_update_interval, self._new_client, self._update)
        self._router = Router(client_factory or self._new_client, self._health_check_interval)

        self._lock = threading.Lock()

    def start(self, min_healthy=1):
        try:
            service_endpoints = self._source.refresh(immediate=True)
            self._update(service_endpoints)
        except DaxServiceError as e:
            if e.auth_error:
                # Startup will fail with authentication errors if region
                # is not set correctly on client config. Region can also be
                # set after instantiating cluster client. Ignore the exception.
                logger.warning('Auth exception while starting up cluster client: %s', e.message)
                min_healthy = 0 # Do not wait for healthy routes as it would timeout
            else:
                raise

        self._source.start()

        if min_healthy <= 0:
            return

        self.wait_for_routes(min_healthy, 1)

    def close(self):
        if self._closed:
            return

        self._closed = True

        try:
            self._source.close()
        except:
            pass
        finally:
            self._source = None

        try:
            self._router.close()
        except:
            pass
        finally:
            self._router = None

    def read_client(self, prev_client=None):
        ''' Return a read client.

        Caller should not close the client.
        '''
        backend = self._router.next_any(prev_client)
        if backend is not None:
            return backend.client
        else:
            raise DaxClientError("No cluster endpoints available", DaxErrorCode.NoRoute)

    def write_client(self, prev_client=None):
        ''' Return a write client.

        Caller should not close the client.
        '''
        backend = self._router.next_leader(prev_client)
        if backend is not None:
            return backend.client
        else:
            raise DaxClientError("No cluster endpoints available", DaxErrorCode.NoRoute)

    def wait_for_routes(self, min_healthy, leader_min):
        return self._router.wait_for_routes(min_healthy, leader_min)
    
    def _new_client(self, host, port):
        ''' Create a new client.

        Caller is responsible for closing the client.
        '''
        tube_pool = SocketTubePool(
                host, port, 
                lambda: self._credentials, 
                self._region_name, 
                self._user_agent, 
                self._user_agent_extra,
                self._connect_timeout,
                self._read_timeout)
        return DaxClient(tube_pool)

    def _update(self, service_endpoints):
        if service_endpoints:
            logger.debug('Got new endpoints %s', service_endpoints)
            self._service_endpoints = service_endpoints
            self._router.update(service_endpoints)

class Source(object):
    ''' Manage source of endpoint info. '''
    def __init__(self, seeds, refresh_interval, client_factory, on_changed):
        self._seeds = seeds
        self._refresh_interval = refresh_interval
        self._client_factory = client_factory
        self._on_changed = on_changed
        self._service_endpoints = set()
        self._timer = None

        self._lock = threading.Lock()

    def start(self):
        logger.debug('Starting source refresher for %s', self._seeds)
        with self._lock:
            if self._timer is None:
                self._timer = ClusterUtil.periodic_task(
                        self.refresh, 
                        self._refresh_interval, 
                        self._refresh_interval * 0.1)

    def refresh(self, immediate=False):
        logger.debug('Refreshing source from %s', self._seeds)
        latest = set(self._pull(self._seeds))
        with self._lock:
            existing = self._service_endpoints
            if existing == latest:
                return
            else:
                logger.debug('Endpoints changing from %s to %s', existing, latest)
                self._service_endpoints = latest

        if not immediate:
            self._on_changed(latest)

        return latest

    def close(self):
        self._timer.cancel()

    def _pull(self, seeds):
        for host, port in seeds:
            # Find all DNS addresses for this hostname
            addrs = _resolve_dns(host, port)
            random.shuffle(addrs)

            logger.debug('Source: Resolved addresses %s for %s', addrs, host)

            for ip, port in addrs:
                # Stop after the first successful call
                try:
                    new_endpoints = self._pull_from(ip, port)
                    if new_endpoints:
                        return new_endpoints
                except Exception as e:
                    logger.error('Failed to retrieve endpoints', exc_info=True)

        raise DaxClientError('Failed to configure cluster endpoints from {}'.format(seeds), DaxErrorCode.NoRoute)

    def _pull_from(self, ip, port):
        service_endpoints = []
        with closing(self._client_factory(ip, port)) as client:
            endpoints = client.endpoints()
            for endpoint in endpoints:
                # Set the hostname to the address if none provided
                if 'hostname' in endpoint:
                    if 'address' not in endpoint:
                        # No address, so go ask DNS
                        addresses = _resolve_dns(endpoint['hostname'], port)
                        if addresses:
                            # If multiple addresses, pick one
                            endpoint['address'] = random.choice(addresses)[0]
                        else:
                            # Could not resolve hostname, bad endpoint, ignore it
                            continue
                else:
                    if endpoint.setdefault('hostname', endpoint.get('address')) is None:
                        # No hostname or address, go to the next endpoint
                        continue

                endpoint.setdefault('leader_session_id', 0)
                service_endpoints.append(ServiceEndpoint(**endpoint))

        return service_endpoints

def _parse_host_ports(endpoint):
    parts = endpoint.split(':', 1)
    if len(parts) == 1:
        return parts[0].strip(), DEFAULT_PORT
    else:
        return parts[0].strip(), int(parts[1].strip())

def _resolve_dns(host, port):
    try:
        # Deliberately restrict it to IPv4 addresses
        return [sockaddr \
                for family, socktype, proto, canonname, sockaddr \
                in socket.getaddrinfo(host, port, socket.AF_INET, 0, socket.IPPROTO_TCP)]
    except socket.gaierror:
        # if there is an error, return no addresses
        return []

