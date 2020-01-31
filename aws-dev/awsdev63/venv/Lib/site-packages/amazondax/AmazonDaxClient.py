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

import logging
import time

from .Cluster import Cluster
from .DaxError import DaxErrorCode, DaxClientError, DaxServiceError

import botocore.session
from botocore.client import Config, ClientMeta
from botocore.credentials import Credentials
from botocore.model import ServiceModel
from botocore.exceptions import PartialCredentialsError, DataNotFoundError, UnknownServiceError
from botocore.retryhandler import create_exponential_delay_function
from botocore.hooks import first_non_none_response

logger = logging.getLogger(__name__)

class AmazonDaxClient(object):
    ''' Construct a DAX Client. 
    
    Use
        session = botocore.session.get_session()
        dax = AmazonDaxClient(session, ...)

    instead of
        session = botocore.session.get_session()
        ddb = session.create_client('dynamodb', ...)

    Remember to close the cluster when done, or use a with-statement:
        with AmazonDaxClient(session, ...) as dax:
            ...

    To pass the endpoints, use the 'endpoint_url' or 'endpoints' parameters:

        AmazonDaxClient(endpoint_url='myDAXcluster.2cmrwl.clustercfg.dax.use1.cache.amazonaws.com:8111')
        AmazonDaxClient(endpoints=['myDAXcluster.2cmrwl.clustercfg.dax.use1.cache.amazonaws.com:8111'])

    AmazonDaxClient instances can be shared between threads.
    '''
    _PY_TO_OP_NAME = {
        'get_item': 'GetItem',
        'put_item': 'PutItem',
        'delete_item': 'DeleteItem',
        'update_item': 'UpdateItem',
        'query': 'Query',
        'scan': 'Scan',
        'batch_get_item': 'BatchGetItem',
        'batch_write_item': 'BatchWriteItem'
    }

    @staticmethod
    def resource(session=None,
                 region_name=None, api_version=None,
                 use_ssl=False, verify=None, endpoint_url=None,
                 aws_access_key_id=None, aws_secret_access_key=None,
                 aws_session_token=None, config=None, endpoints=None):
        ''' Create a high-level Boto Resource matching DynamoDB. 
        
        Instead of
            ddb = boto3.resource('dynamodb')

        use
            dax = AmazonDaxClient.resource()
        '''
        # Local import so it doesn't fail if boto3 is not available
        from .Resource import is_boto3_session, DaxSession

        if session is None or isinstance(session, botocore.session.Session):
            session = DaxSession(
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key, 
                    aws_session_token=aws_session_token,
                    region_name=region_name, 
                    botocore_session=session,
                    endpoints=endpoints)
        else:
            if is_boto3_session(session):
                session = DaxSession.from_boto3(session, endpoints)
            else:
                raise ValueError('session must be a botocore or boto3 session')

        # Create the resource
        res = session.resource('dynamodb', region_name=region_name, api_version=api_version,
             use_ssl=use_ssl, verify=verify, endpoint_url=endpoint_url,
             aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
             aws_session_token=aws_session_token, config=config)
        
        return res

    def __init__(self, session=None, 
                  region_name=None, api_version=None,
                  use_ssl=None, verify=None, endpoint_url=None,
                  aws_access_key_id=None, aws_secret_access_key=None,
                  aws_session_token=None, config=None, endpoints=None):
        if session is None:
            session = botocore.session.get_session()
        else:
            if not isinstance(session, botocore.session.Session):
                try:
                    # Check for a boto3 session-ish object and get the internal botocore session
                    _session = session._session
                except AttributeError:
                    raise ValueError('session must be a botocore or boto3 session')
                else:
                    session = _session

        if use_ssl:
            raise ValueError('SSL/TLS is not supported. Set use_ssl=False.')

        self._session = session
        default_client_config = self._session.get_default_client_config()
        if config is not None and default_client_config is not None:
            # If a config is provided and a default config is set, then
            # use the config resulting from merging the two.
            config = default_client_config.merge(config)
        elif default_client_config is not None:
            # If a config was not provided then use the default
            # client config from the session
            config = default_client_config
        self._client_config = config if config is not None else Config(region_name=region_name)

        # resolve the region name
        if region_name is None:
            if config and config.region_name:
                region_name = config.region_name
            else:
                region_name = self._session.get_config_variable('region')
        self._region_name = region_name

        # Figure out the verify value base on the various
        # configuration options.
        if verify is None:
            verify = self._session.get_config_variable('ca_bundle')
        self._verify = verify

        # Gather endpoints
        self._endpoints = endpoints or []
        if endpoint_url and endpoint_url not in self._endpoints:
            # If endpoint_url is provided, include it 
            self._endpoints.insert(0, endpoint_url)
    
        if not self._endpoints:
            raise ValueError('No endpoints provided')

        # Resolve credentials
        if aws_access_key_id is not None and aws_secret_access_key is not None:
            self._credentials = Credentials(aws_access_key_id, aws_secret_access_key, aws_session_token)
        elif self._session._missing_cred_vars(aws_access_key_id, aws_secret_access_key):
            raise PartialCredentialsError(
                provider='explicit',
                cred_var=self._session._missing_cred_vars(aws_access_key_id, aws_secret_access_key))
        else:
            self._credentials = self._session.get_credentials()

        # Fake out the meta information as much as possible
        loader = session.get_component('data_loader')
        json_model = loader.load_service_model('dynamodb', 'service-2', api_version=api_version)
        service_model = ServiceModel(json_model, service_name='dynamodb')
        event_emitter = session.get_component('event_emitter')
        partition = None
        self.meta = ClientMeta(event_emitter, self._client_config,
                               self._endpoints[0], service_model,
                               self._PY_TO_OP_NAME, partition)

        # Check signing version
        if self._client_config.signature_version and self._client_config.signature_version != 'v4':
            logger.warning('DAX only supports SigV4 signing; given signature_version "%s" ignored.',
                    self._client_config.signature_version)
    
        # Start cluster connection & background tasks
        self._cluster = Cluster(self._region_name, 
                self._endpoints, 
                self._credentials, 
                self._client_config.user_agent, 
                self._client_config.user_agent_extra,
                self._client_config.connect_timeout,
                self._client_config.read_timeout)
        self._cluster.start()

    def close(self):
        if self._cluster:
            self._cluster.close()
            self._cluster = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def batch_get_item(self, **kwargs):
        return self._read_request('batch_get_item', **kwargs)

    def batch_write_item(self, **kwargs):
        return self._write_request('batch_write_item', **kwargs)

    def delete_item(self, **kwargs):
        return self._write_request('delete_item', **kwargs)

    def get_item(self, **kwargs):
        return self._read_request('get_item', **kwargs)

    def put_item(self, **kwargs):
        return self._write_request('put_item', **kwargs)

    def query(self, **kwargs):
        return self._read_request('query', **kwargs)

    def scan(self, **kwargs):
        return self._read_request('scan', **kwargs)

    def update_item(self, **kwargs):
        return self._write_request('update_item', **kwargs)

    # vvv TODO Boto service methods vvv
    def can_paginate(self, **kwargs):
        raise NotImplementedError()

    def generate_presigned_url(self, **kwargs):
        raise NotImplementedError()

    def get_paginator(self, **kwargs):
        raise NotImplementedError()

    def get_waiter(self, **kwargs):
        raise NotImplementedError()

    # vvv Internal Methods vvv

    def _read_request(self, op_name, **kwargs):
        rclient = self._cluster.read_client()
        return self._retryable_request(rclient, 1000.0, op_name, **kwargs)

    def _write_request(self, op_name, **kwargs):
        wclient = self._cluster.write_client()
        return self._retryable_request(wclient, 10.0, op_name, **kwargs)

    def _retryable_request(self, client, scale, op_name, **kwargs):
        action = getattr(client, op_name)
        retryer = RetryHandler(self._client_config, scale)
        while True:
            response = None
            try:
                response = self._do_request(action, op_name, **kwargs)
            except Exception as e:
                if retryer.should_retry(response, e):
                    retryer.delay(e)
                    continue
                else:
                    raise
            
            return response

    def _do_request(self, action, op_name, **kwargs):
        api_params = kwargs
        operation_model = self.meta.service_model.operation_model(self.meta.method_to_api_mapping[op_name])
        request_context = {
            'client_region': self.meta.region_name,
            'client_config': self.meta.config,
            'has_streaming_input': operation_model.has_streaming_input,
            'auth_type': operation_model.auth_type,
        }

        # Copy boto3's events so that the resource works properly

        # Emit an event that allows users to modify the parameters at the
        # beginning of the method. It allows handlers to modify existing
        # parameters or return a new set of parameters to use.
        responses = self.meta.events.emit(
            'provide-client-params.dynamodb.{operation_name}'.format(operation_name=operation_model.name),
            params=api_params, model=operation_model, context=request_context)
        api_params = first_non_none_response(responses, default=api_params)

        self.meta.events.emit(
            'before-parameter-build.dynamodb.{operation_name}'.format(operation_name=operation_model.name),
            params=api_params, model=operation_model, context=request_context)

        response = action(**api_params)

        # Ignore the HTTP response and just handle the parsed response
        self.meta.events.emit(
            'after-call.dynamodb.{operation_name}'.format(operation_name=operation_model.name),
            http_response=None, parsed=response,
            model=operation_model, context=request_context
        )

        return response

    # vvv Unsupported Table/Service Methods vvv
    def create_backup(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")
    
    def create_global_table(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")
    
    def create_table(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")
    
    def delete_backup(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def delete_table(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def describe_backup(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def describe_continuous_backups(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def describe_global_table(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def describe_limits(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def describe_table(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def describe_time_to_live(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def list_backups(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def list_global_tables(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def list_tables(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def list_tags_of_resource(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def restore_table_from_backup(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def tag_resource(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def untag_resource(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def update_global_table(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def update_table(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

    def update_time_to_live(self, **kwargs):
        raise NotImplementedError("Table operations are not supported on DAX. Use the boto DynamoDB client instead.")

class RetryHandler(object):
    DEFAULT_RETRIES = 1     # Same as other DAX clients

    def __init__(self, config, scale):
        self._delay = create_exponential_delay_function('rand', 2)
        self._attempts = 0
        self._max_attempts = config.retries.get('max_attempts', RetryHandler.DEFAULT_RETRIES) if config and config.retries \
                else RetryHandler.DEFAULT_RETRIES
        self.scale = scale

    def should_retry(self, response, exc):
        if not isinstance(exc, (DaxClientError, DaxServiceError)):
            return False

        if self._attempts > self._max_attempts:
            return False

        return exc.retryable

    def delay(self, exc):
        self._attempts += 1
        self._wait_for_cluster(exc)
        return time.sleep(self._delay(attempts=self._attempts) / self.scale)

    def _wait_for_cluster(self, exc):
        if not isinstance(exc, (DaxClientError, DaxServiceError)):
            return
        
        if exc.code == DaxErrorCode.NoRoute:
            self._cluster.wait_for_routes()
        elif exc.wait_for_recovery_before_retry:
            self._cluster.wait_for_recovery()



