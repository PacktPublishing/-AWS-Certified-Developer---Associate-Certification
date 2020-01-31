#!/usr/bin/env python
import base64
import boto3
import sys
from os import environ
import logging
import argparse

REGION = 'eu-west-2'
PROFILE = 'aws-dev'

parser = argparse.ArgumentParser()
parser.add_argument("-value", help="the secret you want to encrypt")
parser.add_argument("-key", help="the reference you want to use /dev/password for example ")

#logging config
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def createClient(region):
    if environ.get('CODEBUILD_BUILD_ID') is not None:
        return boto3.client('ssm', region_name=region)
    else:
        logging.info('using profile {}'.format(PROFILE))
        session = boto3.Session(profile_name=PROFILE)
        return session.client('ssm', region_name=region)

def getParamter(client,reference):
    try:
        result = client.get_parameter(Name=reference,WithDecryption=True)
    except Exception as e:
        print("error: {}".format(e))
        sys.exit(1)
    else:
        return result

def putParameter(client,pkey,pvalue):
    try:
        result = client.put_parameter(Name=pkey,Value=pvalue,Type='SecureString',Overwrite=True)
    except Exception as e:
        print("error: {}".format(e))
        sys.exit(1)
    else:
        return result

def main():
        args = parser.parse_args()
        client = createClient(REGION)
        pput = putParameter(client,args.key,args.value)
        logging.info("put response:{}".format(pput['ResponseMetadata']['HTTPStatusCode']))
        pget = getParamter(client,args.key)
        logging.info("get response for key:{} value:{} version:{}".format(pget['Parameter']['Name'],pget['Parameter']['Value'],pget['Parameter']['Version']))
        return

if __name__ == "__main__":
        main()
