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
MODULE = "SQS-helper"
PROFILE = "aws-dev"
REGION = "eu-west-2"
MESSAGENUM = 20
POLL_INTERVAL = 2
WAIT_TIME = 20
QUEUE = "LQ123"

def createClient(region):
    if environ.get('CODEBUILD_BUILD_ID') is not None:
        return boto3.client('sqs', region_name=region)
    else:
        logging.info('using profile {}'.format(PROFILE))
        session = boto3.Session(profile_name=PROFILE)
        return session.client('sqs', region_name=region)

def createQueue(client,qname,waittime,mesretension):
    try:
        response = client.create_queue(QueueName=qname,Attributes={ \
        'ReceiveMessageWaitTimeSeconds': str(waittime), \
        'MessageRetentionPeriod':mesretension})
    except Exception as e:
        logging.error("error:{}".format(e))
        sys.exit(1)
    else:
        logging.info("response:{}".format(response))
        return response['QueueUrl']

def queuePoller(client,queue,poll_interval,wait_time):
    logging.info("creating queue poller for:{} WaitTime:{}".format(queue,wait_time))
    if wait_time > 0:
        while True:
            try:
                logging.info("long polling for:{}".format(queue))
                response = client.receive_message(QueueUrl=queue,WaitTimeSeconds=wait_time)
                for message in response['Messages']:
                    logging.info("message received: {}".format(message['Body']))
                    client.delete_message(QueueUrl=queue, ReceiptHandle=message['ReceiptHandle'])
            except Exception as e:
                logging.error("error:{}".format(e))
                sys.exit(1)
    else:
        while True:
            try:
                logging.info("short polling for:{}".format(queue))
                response = client.receive_message(QueueUrl=queue)
                #logging.info("message received:{}".format(response))
                for message in response['Messages']:
                    logging.info("message received: {}".format(message['Body']))
                    client.delete_message(QueueUrl=queue,ReceiptHandle=message['ReceiptHandle'])
            except Exception as e:
                logging.error("error:{}".format(e))
            sleep(poll_interval)
    return

def MessagePublisher(client,queue,count):
    logging.info("publishing messgage:{}".format(count))
    message = "message " + str(count)
    try:
        response = client.send_message(QueueUrl=queue,MessageBody=message)
    except Exception as e:
        logging.error("error:{}".format(e))
    else:
        logging.info("statusCode:{}".format(response['ResponseMetadata']['HTTPStatusCode']))
        return response

def main ():
    print('Running:{}'.format(MODULE))
    queueC = createClient(REGION)
    queue_url = createQueue(queueC,QUEUE,WAIT_TIME,'60')
    t = threading.Thread(target=queuePoller, args=(queueC, queue_url,POLL_INTERVAL,WAIT_TIME))
    t.start()
    sleep(13)
    for count in range(1,MESSAGENUM):
        MessagePublisher(queueC,queue_url,count)

if __name__ == "__main__":
    main()



