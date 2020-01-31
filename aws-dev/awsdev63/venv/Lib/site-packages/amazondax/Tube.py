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

import time
import threading
import socket
import binascii
import datetime

import base64

from random import SystemRandom

from . import SigV4Gen, __version__ as pkg_version
from .CborEncoder import CborEncoder
from .DaxCborDecoder import DaxCborDecoder
from .Constants import STRING_TYPES, BINARY_TYPES, INTEGRAL_TYPES, SEQ_TYPES

import logging
logger = logging.getLogger(__name__)

class Tube(object):
    MAGIC_STRING = b'gJ7yne5G'
    USER_AGENT_STRING = u'UserAgent' 
    DEFAULT_USER_AGENT = u'DaxPythonClient-' + pkg_version
    WINDOW_SCALAR = 0.1
    DAX_ADDR = u'https://dax.amazonaws.com'
    DEFAULT_FLUSH_SIZE = 4096
    MIN_READ_SIZE = 4096

    def __init__(self, sock, version, cred_provider, region, user_agent=None, user_agent_extra=None, clock=time.time):
        self._socket = sock
        self._version = version
        self._cred_provider = cred_provider
        self._region = region
        self._access_key_id = None
        self._auth_exp = 0
        self._auth_ttl_millis = 5 * 60 * 1000
        self._pool_window = (self._auth_ttl_millis / 2)
        self._tube_window = (self._auth_ttl_millis * Tube.WINDOW_SCALAR)
        self._last_pool_auth = 0
        self.clock = clock

        self._user_agent = user_agent or Tube.DEFAULT_USER_AGENT
        if user_agent_extra:
            self._user_agent += ' ' + user_agent_extra

        self.enc = CborEncoder()
        self.dec = DaxCborDecoder(self._more)

        self._connect(self._version)

    def _connect(self, session):
        # Send the initialization info to the server
        self.enc.append_raw(Tube.MAGIC_STRING)
        self.enc.append_int(0)     # No layering

        if session is None:
            self.enc.append_null()
        else:
            self.enc.append_binary(session.as_bytes())

        self.enc.append_map_header(1)
        self.enc.append_string(Tube.USER_AGENT_STRING)
        self.enc.append_string(self._user_agent)

        self.enc.append_int(0)     # No types

        self.flush()

    def _more(self, buf, n):
        # TODO This may be more efficient with a memoryview on the buffer
        # Although the rest of the buffer handling needs to be rewritten as well

        # Attempt to read at least MIN_READ_SIZE to avoid lots of 1 byte reads.
        data = self._socket.recv(max(n, Tube.MIN_READ_SIZE))
        buf += data

        return len(data)

    def close(self):
        ''' Close the underlying socket. '''
        if self._socket:
            self._socket.close()
            self._socket = None

    def reauth(self):
        ''' Resend the authentication request (if necessary) before the next API call. '''
        # Run everything in millis to be consistent with other clients
        now = self.clock() * 1000

        if self._auth_exp - now <= self._tube_window or now - self._last_pool_auth >= self._pool_window:
            creds = self._cred_provider()
            self._checkAndUpdateAccessKeyId(creds.access_key)
            self._last_pool_auth = now
            self._auth_exp = now + self._auth_ttl_millis

            self._auth(creds, now/1000)

    def write(self, b):
        ''' Write raw bytes to the destination. '''
        self.enc.append_raw(b)
        self._maybe_flush()

    def write_object(self, o):
        ''' Write an object of unknown type to the destination.

        Only Python primitives (and their subclasses) are supported, not arbitrary objects.
        '''
        if o is None:
            self.write_null()
        elif isinstance(o, bool):
            self.write_boolean(o)
        elif isinstance(o, INTEGRAL_TYPES):
            self.write_int(o)
        elif isinstance(o, STRING_TYPES):
            self.write_string(o)
        elif isinstance(o, BINARY_TYPES):
            self.write_binary(o)
        elif isinstance(o, SEQ_TYPES):
            self.write_array(o)
        elif isinstance(o, dict):
            self.write_map(o)
        else:
            raise ValueError('Unknown type: ' + type(o).__name__)
        
        # Flush is performed by the type-specific function

    def write_int(self, i):
        ''' Write a CBOR int record (Python int) to the destination. '''
        self.enc.append_int(i)
        self._maybe_flush()

    def write_binary(self, b):
        ''' Write a CBOR bytes record (Python bytes or bytearray) to the destination. '''
        self.enc.append_binary(b)
        self._maybe_flush()

    def write_string(self, s):
        ''' Write a CBOR text record (Python str) to the destination. '''
        self.enc.append_string(s)
        self._maybe_flush()

    def write_array(self, a):
        ''' Write a CBOR array (Python seq/list/tuple) to the destination. '''
        self.enc.append_array_header(len(a))
        for item in a:
            self.write_object(item)
        self._maybe_flush()

    def write_map(self, m):
        ''' Write a CBOR array (Python dict) to the destination. '''
        self.enc.append_map_header(len(a))
        for key, value in m.items():
            self.write_object(key)
            self.write_object(value)
        self._maybe_flush()

    def write_boolean(self, b):
        ''' Write a CBOR boolean of the appropriate value.'''
        self.enc.append_boolean(b)
        self._maybe_flush()

    def write_null(self):
        ''' Write a CBOR NULL record to the destination. '''
        self.enc.append_null()
        self._maybe_flush()

    def peek(self):
        ''' Peek at the next CBOR record. '''
        return self.dec.peek()

    def skip(self):
        ''' Skip the next CBOR record. '''
        return self.dec.skip()

    def read_int(self):
        ''' Read the next int. '''
        return self.dec.decode_int()

    def read_string(self):
        ''' Read the next int. '''
        return self.dec.decode_string()

    def read_object(self):
        ''' Read the next object. '''
        return self.dec.decode_object()

    def read_map(self):
        ''' Read a map. '''
        return self.dec.decode_map()

    def read_array(self):
        ''' Read an array. '''
        return self.dec.decode_array()

    def try_read_null(self):
        ''' Check for CBOR NULL.
        
        If the next token is a CBOR NULL, consume it and return True. Otherwise,
        leave it and return False.
        '''
        return self.dec.try_decode_null()

    def _maybe_flush(self):
        ''' Flush some data if the buffer is larger than the current flush size. '''
        if len(self.enc._buffer) > Tube.DEFAULT_FLUSH_SIZE:
            self._flush_some()

    def _flush_some(self):
        ''' Flush as much as possible in one call. '''
        n = self._socket.send(self.enc._buffer)
        self.enc._drain(n)

    def flush(self):
        ''' Completely flush the write buffer. '''
        self._socket.sendall(self.enc._buffer)
        self.enc.reset()

    def clear(self):
        self.enc.reset()

    def _auth(self, creds, now):
        ''' Send an authentication RPC prior to the next request. '''
        logger.debug('Sending authentication request')
        dtnow = datetime.datetime.utcfromtimestamp(now)
        sig = SigV4Gen.generate_signature(creds, Tube.DAX_ADDR, self._region, b'', dtnow)

        self.enc.append_int(1)
        self.enc.append_int(1489122155) # authorizeConnection method ID
        self.enc.append_string(creds.access_key)
        self.enc.append_string(sig.signature)
        self.enc.append_binary(sig.string_to_sign)
        
        token = getattr(sig, 'token')
        if token:
            self.enc.append_string(token)
        else:
            self.enc.append_null()

        if self._user_agent:
            self.enc.append_string(self._user_agent)
        else:
            self.enc.append_null()
    
    def _checkAndUpdateAccessKeyId(self, akid):
        if not akid:
            raise ValueError('AWSCredentialsProvider provided null AWSAccessKeyId')

        if akid != self._access_key_id:
            self._access_key_id = akid
            return True
        else:
            return False

class TubePool(object):
    def __init__(self):
        self._head_tube = None
        self._last_active_tube = None
        self._session_version = _SessionVersion()
        self._connection = 0
        self._total = 0

        self._lock = threading.RLock()
        self._avail = threading.Condition(self._lock)

        # TODO Start a Timer job to reap idle tubes

    def get(self):
        with self._lock:
            while True:
                tube = self._head_tube
                self._total += 1
                if tube is not None:
                    self._head_tube = tube._next_tube
                    if self._last_active_tube is tube:
                        self._last_active_tube = self._head_tube
                    tube._next_tube = None
                    return _TubeManager(self, tube)
                else:
                    self._connection += 1
                    self._lock.release()
                    try:
                        # Connect outside of the lock
                        # TODO Ideally this is an asynchronous process
                        # Otherwise connect() can block until available, when a
                        # tube may get recycled sooner
                        # The Java client uses a future; perhaps we could do that
                        # here as well
                        new_tube = self._alloc()
                    finally:
                        self._lock.acquire()

                    self.recycle(new_tube)
    
    def recycle(self, tube):
        if tube is None:
            return

        with self._lock:
            if tube._version == self._session_version:
                tube._next_tube = self._head_tube
                self._head_tube = tube
                self._avail.notify()
                return

        tube.close()

    def reset(self, tube):
        if tube is None:
            return

        tube.close()

        with self._lock:
            if tube._version != self._session_version:
                return

            self._version_bump()
            tube = self._head_tube
            self._head_tube = self._last_active_tube = None
            self._avail.notify_all()

        self._close_all(tube)

    def close(self):
        with self._lock:
            self._version_bump()
            head = self._head_tube
            self._head_tube = self._last_active_tube = None
        self._close_all(head)

    def reap_idle_tubes(self):
        to_reap = None
        with self._lock:
            # Select last active tube
            last_active_tube = self._last_active_tube
            # If it's not null, break chain after this tube
            if last_active_tube is not None:
                to_reap = last_active_tube._next_tube
                last_active_tube._next_tube = None

            self._last_active_tube = self._head_tube

        # Close tubes downstream of the to_reap one
        self._close_all(to_reap)
        self._connection = 0

    def _close_all(self, head_tube):
        reap_count = 0
        tube = head_tube
        while tube is not None:
            reap_count += 1
            tube.close()

            _next = tube._next_tube
            tube._next_tube = None
            tube = _next

        return reap_count

    def _version_bump(self):
        self._session_version = _SessionVersion()

    def _alloc(self):
        raise NotImplementedError('abstract method')

class SocketTubePool(TubePool):
    DEFAULT_CONNECT_TIMEOUT = 1 # seconds
    DEFAULT_READ_TIMEOUT = 60   # seconds

    def __init__(self, hostname, port, cred_provider, region, user_agent=None, user_agent_extra=None, 
                 connect_timeout=None, read_timeout=None):
        super(SocketTubePool, self).__init__()
        self.hostname = hostname
        self.port = port
        self.cred_provider = cred_provider
        self.region = region
        self.user_agent = user_agent
        self.user_agent_extra = user_agent_extra
        self.connect_timeout = connect_timeout if connect_timeout is not None else SocketTubePool.DEFAULT_CONNECT_TIMEOUT
        self.read_timeout = read_timeout if read_timeout is not None else SocketTubePool.DEFAULT_READ_TIMEOUT

    def _alloc(self):
        logger.debug('SocketTubePool: New connection to %s:%s', self.hostname, self.port)
        sock = socket.create_connection((self.hostname, self.port), timeout=self.connect_timeout)

        # set socket options NODELAY, timeouts, keepalives
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(self.read_timeout)

        return Tube(sock,
            self._session_version, 
            self.cred_provider, 
            self.region, 
            self.user_agent, 
            self.user_agent_extra)

class _TubeManager(object):
    def __init__(self, pool, tube):
        self._tube = tube
        self._pool = pool

    def __enter__(self):
        return self._tube

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            # Normal exit case
            self._pool.recycle(self._tube)
        else:
            # An exception was thrown
            self._pool.reset(self._tube)

_version_rand = SystemRandom()
class _SessionVersion(object):
    def __init__(self):
        self.session = _version_rand.getrandbits(128)

    def __eq__(self, other):
        return self.session == other.session

    def as_bytes(self):
        # Convert the integer to bytes, regardless of integer size
        hval = '%x' % self.session
        bval = binascii.unhexlify('0' + hval if len(hval) % 2 != 0 else hval)

        return bval
