#!/usr/bin/env python
import boto3
import botocore.session
from os import environ
import logging
import json
#DAX
import amazondax

#logging config
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.WARN,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)



#globals
MODULE = "dynamodb-helper"
PROFILE = "aws-dev"
REGION = "eu-west-2"
TABLE = "productManuals"
INDEX = "pfamily-contenttype-index"
ENDPOINT = "mydaxcluster.lxh32w.clustercfg.dax.euw1.cache.amazonaws.com:8111"


def createClient(region):
    if environ.get('CODEBUILD_BUILD_ID') is not None:
        return boto3.client('dynamodb', region_name=region)
    else:
        logging.info('using profile {}'.format(PROFILE))
        session = boto3.Session(profile_name=PROFILE)
        return session.client('dynamodb', region_name=region)

def createDAXClient(region,endpoint):
    if environ.get('CODEBUILD_BUILD_ID') is not None:
        return amazondax.AmazonDaxClient(region_name=region, endpoints=[endpoint])
    else:
        logging.info('using profile {}'.format(PROFILE))
        session = botocore.session.Session(profile=PROFILE)
        return amazondax.AmazonDaxClient(session,region_name=region, endpoints=[endpoint])

def simplyDDBData(item):
    result = {}
    for mkey in item.keys():
        for key,value in item[mkey].items():
            result[mkey] = value
    return result

def getTable(client,table):
    results = {}
    items = []
    try:
        data = client.scan(TableName=table)
        if data != None:
            for item in data['Items']:
                result = simplyDDBData(item)
                items.append(result)
        results['items'] = items
    except Exception as e:
        logging.error('error getting results from table:{} error:{}'.format(table,e))
        results['error'] = "scan error"
    return results

def queryIndex(client,table,index,pfamily):
        results = {}
        items = []
        try:
            data = client.query(TableName=table,IndexName=index,KeyConditionExpression='pfamily = :f',ExpressionAttributeValues={":f":{"S":pfamily}})
            if data != None:
                for item in data['Items']:
                    result = simplyDDBData(item)
                    items.append(result)
            results['items'] = items
        except Exception as e:
            logging.error('error getting results from table:{} error:{}'.format(table, e))
            results['error'] = "query error"
        return results


def main ():
    print('Running:{}'.format(MODULE))
    ddb = createClient(REGION)
    dax = createDAXClient('eu-west-1',ENDPOINT)
    items = getTable(ddb,TABLE)
    print(json.dumps(items))
    items2 = queryIndex(ddb,TABLE,INDEX,'Maxi')
    print(json.dumps(items2))
    items3 = getTable(dax,TABLE)



if __name__ == "__main__":
    main()