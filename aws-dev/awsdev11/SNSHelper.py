#!/usr/bin/env python
import boto3
from os import environ
import logging
import threading
from time import sleep
import sys


# logging config
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# globals
MODULE = "SNS-helper"
PROFILE = "aws-dev"
REGION = "eu-west-2"
TOPIC = "arn:aws:sns:eu-west-2:763988453897:awsdev123"
MESSAGE = "ALARM! ALARM!"


def createClient(region):
    if environ.get('CODEBUILD_BUILD_ID') is not None:
        return boto3.client('sns', region_name=region)
    else:
        logging.info('using profile {}'.format(PROFILE))
        session = boto3.Session(profile_name=PROFILE)
        return session.client('sns', region_name=region)

def MessagePublisher(client,topic,message_txt):
    logging.info("publishing messgage:{}".format(topic))
    try:
        response = client.publish(TopicArn=topic,Message=message_txt)
    except Exception as e:
        logging.error("error:{}".format(e))
    else:
        logging.info("statusCode:{}".format(response['ResponseMetadata']['HTTPStatusCode']))
        return response


def main ():
    print('Running:{}'.format(MODULE))
    notificationC = createClient(REGION)
    MessagePublisher(notificationC,TOPIC,MESSAGE)

if __name__ == "__main__":
    main()



