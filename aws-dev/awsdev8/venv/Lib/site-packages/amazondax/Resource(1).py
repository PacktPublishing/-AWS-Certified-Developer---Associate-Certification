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

import botocore, boto3

from .AmazonDaxClient import AmazonDaxClient

def is_boto3_session(session):
    return isinstance(session, boto3.session.Session)

class DaxSession(boto3.session.Session):
    @classmethod
    def from_boto3(cls, session, endpoints):
        ''' Create a DaxSession from a boto3 Session by copying the internals. '''
        # This makes a copy in case the session is reused in other contexts
        # Hopefully copying doesn't have any other side-effects
        return cls(session._session.credentials.aws_access_key_id,
            session._session.credentials.aws_secret_access_key,
            session._session.credentials.aws_session_token,
            session.region_name,
            session._session,
            session.profile_name,
            endpoints)

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 aws_session_token=None, region_name=None,
                 botocore_session=None, profile_name=None, endpoints=None):
        super(DaxSession, self).__init__(aws_access_key_id=aws_access_key_id, 
                aws_secret_access_key=aws_secret_access_key, 
                aws_session_token=aws_session_token,
                region_name=region_name)
        self.endpoints = endpoints

    def client(self, service_name, region_name=None, api_version=None,
               use_ssl=True, verify=None, endpoint_url=None,
               aws_access_key_id=None, aws_secret_access_key=None,
               aws_session_token=None, config=None):
        ''' Create a DAX client instead of a dynamodb client. '''
        if service_name != 'dynamodb' and service_name != 'dax':
            raise ValueError('service_name must be "dynamodb" or "dax"')

        return AmazonDaxClient(self,
            region_name=region_name, api_version=api_version,
            use_ssl=use_ssl, verify=verify, endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token, config=config,
            endpoints=self.endpoints)
    
    def resource(self, service_name, region_name=None, api_version=None,
                 use_ssl=True, verify=None, endpoint_url=None,
                 aws_access_key_id=None, aws_secret_access_key=None,
                 aws_session_token=None, config=None):
        ''' Create a DAX resource, which is really just a dynamodb resource with a different client. '''
        if service_name != 'dynamodb' and service_name != 'dax':
            raise ValueError('service_name must be "dynamodb" or "dax"')

        res = super(DaxSession, self).resource('dynamodb',
            region_name=region_name, api_version=api_version,
            use_ssl=use_ssl, verify=verify, endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token, config=config)

        # BLACK MAGIC (not really)
        # The context manager methods __enter__ and __exit__ must be defined on the type, not the instance
        # But what we have is an instance with a dynamically-generated base class, and sticking the methods on
        # that might propagate to other instances.
        # It looks like the current version of boto generates a new class on every call to .resource()
        # but I don't want to rely on that.
        # Soooo ... we generate a new dynamic class that uses THAT dynamic class as its base,
        # and stick __enter__ and __exit__ on it.
        dax_svc_res_attrs = {
            '__enter__': _resource_enter,
            '__exit__': _resource_exit
        }
        dax_svc_res = type('DaxServiceResource', (type(res),), dax_svc_res_attrs)

        # Then, make the new class the base class of the existing instance
        res.__class__ = dax_svc_res

        # Now the resource has our custom class as its base but all other attributes and behaviour are preserved

        return res

def _resource_enter(self):
    return self

def _resource_exit(self, exc_type, exc_value, traceback):
    return self.meta.client.__exit__(exc_type, exc_value, traceback)

