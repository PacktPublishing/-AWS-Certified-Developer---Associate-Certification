#!/usr/bin/env python
import boto3
from botocore.client import Config
import requests
import sys

PROFILE = 'aws-dev'
KEY = 'AWS_Cloud_Best_Practices.pdf'
BUCKET = 'awsdev-packt-dublin-dev-1'
REGION='eu-west-1'

def createClient(region):
    return boto3.client('s3',config=Config(signature_version='s3v4'),region_name=region)

def getURL(client,bucket,key):
    try:
        url = client.generate_presigned_url(ClientMethod='get_object',Params={'Bucket': bucket,'Key': key})
    except Exception as e:
        print("error:{}".format(e))
        sys.exit(1)
    return url

def main():
    boto3.setup_default_session(profile_name=PROFILE)
    client = createClient(REGION)
    url = getURL(client,BUCKET,KEY)
    print("url: \"{}\"".format(url))

if __name__ == "__main__":
    main()