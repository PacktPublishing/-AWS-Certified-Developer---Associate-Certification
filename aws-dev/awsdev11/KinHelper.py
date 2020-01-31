#!/usr/bin/env python
import boto3
from os import environ
import logging
import base64


# logging config
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# globals
MODULE = "Kinesis-helper"
PROFILE = "aws-dev"
REGION = "eu-west-2"
MESSAGENUM = 100
STREAM = "awsdev2"

def createClient(region):
    if environ.get('CODEBUILD_BUILD_ID') is not None:
        return boto3.client('kinesis', region_name=region)
    else:
        logging.info('using profile {}'.format(PROFILE))
        session = boto3.Session(profile_name=PROFILE)
        return session.client('kinesis', region_name=region)


def MessagePublisher(client,stream,count):
    logging.info("publishing messgage:{}".format(count))
    message = ("message " + str(count)).encode()
    if (count % 2) == True:
        pkey = "Even"
    else:
        pkey = "Odd"
    try:
        response = client.put_record(StreamName=stream,Data=message,PartitionKey=pkey)
    except Exception as e:
        logging.error("error:{} messgae:{}".format(e,count))
    else:
        logging.info("message:{} statusCode:{}".format(message,response['ResponseMetadata']['HTTPStatusCode']))


def main ():
    print('Running:{}'.format(MODULE))
    kinC = createClient(REGION)
    for count in range(1,MESSAGENUM):
        MessagePublisher(kinC,STREAM,count)
if __name__ == "__main__":
    main()



