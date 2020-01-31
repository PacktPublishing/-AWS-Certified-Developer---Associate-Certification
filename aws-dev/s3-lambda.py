from __future__ import print_function

import json
import urllib
import boto3
from botocore.client import Config
TABLE = "productManuals"
print('Loading function')

s3 = boto3.client('s3',config=Config(signature_version='s3v4'))
ddb = boto3.client('dynamodb')

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print('got object {}'.format(key))
        try:
            url=s3.generate_presigned_url(ClientMethod='get_object',Params={'Bucket': bucket,'Key': key})
            print("retrived pre-signed url:{}".format(url))
        except Exception as e:
            print('error getting pre-signed url error:e'.format(e))
            url = ""
        product = (response['Metadata']['product'])
        pfamily  = (response['Metadata']['pfamily'])
        contentType = response['ContentType']
        try:
            dbresponse = ddb.put_item(TableName=TABLE, Item={'product-file':{'S':key},'product':{'S':product},'content-type':{'S':contentType},'url':{'S':url},'family':{'S':pfamily}})
            print('updated DynamoDB table productManuals')
        except Exception as e:
            print(e)
            print('Error writing to dynamodb for object {} from bucket {}'.format(key, bucket))
            raise e
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e