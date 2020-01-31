#!/usr/bin/env python
import json
import boto3
import time
import sys

#setup logging
LOG_GROUP='application1'
LOG_STREAM='stream1'
LOG_REGION='eu-west-2'


def getNextToken(logClient,group,stream):
        try:
            response = logClient.describe_log_streams(logGroupName=group,logStreamNamePrefix=stream)
            nextToken = response['logStreams'][0]['uploadSequenceToken']
        except Exception as e:
            print(e)
            nextToken = ""
        return nextToken    

def creatLogs(logClient,group,stream):
        try:
            logClient.create_log_group(logGroupName=group)
            logClient.create_log_stream(logGroupName=group, logStreamName=stream)
        except Exception as e:
            try:
                logClient.create_log_stream(logGroupName=group, logStreamName=stream)
            except Exception as e:
                pass 
        return

def putLog(logClient,group,stream,event):
        token=getNextToken(logClient,group,stream)
        timestamp = int(round(time.time() * 1000))
        message = {'timestamp':timestamp, 'message': time.strftime('%Y-%m-%d %H:%M:%S')+event}
        try:
            logClient.put_log_events(logGroupName=group,logStreamName=stream,logEvents=[message],sequenceToken=token)
            print("success:{}".format(message))
        except Exception as e: 
            print("error:{}".format(e))
            sys.exit(1)
        return 

def createClient():
        return boto3.client('logs', region_name=LOG_REGION)

def function1(logClient):
        event="678 - wrong email"
        putLog(logClient,LOG_GROUP,LOG_STREAM,event)
        return

def function2(logClient):
        event="679 - wrong token"
        putLog(logClient,LOG_GROUP,LOG_STREAM,event)
        return 

def main():
        client = createClient()
        creatLogs(client,LOG_GROUP,LOG_STREAM)
        putLog(client,LOG_GROUP,LOG_STREAM,"678 - wrong email")
        putLog(client,LOG_GROUP,LOG_STREAM,"679 - wrong token")
        return

if __name__ == "__main__":
        main()