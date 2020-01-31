import json
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
patch_all()


def lambda_handler(event, context):
    subsegment = xray_recorder.begin_subsegment('active-directory')
    print("active directory event:{}".format(event))
    subsegment.put_annotation('lambda_id', context.aws_request_id)
    subsegment.put_metadata('AD',json,"ad.widgets.com")
    xray_recorder.end_subsegment()
    return {"statusCode":200}