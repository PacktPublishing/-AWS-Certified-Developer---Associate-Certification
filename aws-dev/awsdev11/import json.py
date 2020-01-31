import json
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
patch_all()
import ddbHelper as db 
REGION = "eu-west-2"
ddb = db.createClient(REGION)

def lambda_handler(event, context):
    HTTPresponse = db.getTable(ddb,'productManuals')
    if 'error' not in HTTPresponse:
        status = 200
        HTTPresponse['request'] = 'ok'
    else:
        status = 500
        HTTPresponse['request'] = 'fail' 
    return {"statusCode":status, "mimetype":"application/json","data":HTTPresponse}