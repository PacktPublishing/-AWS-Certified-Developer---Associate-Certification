import json

def lambda_handler(event, context):
    print("active directory event:{}".format(event))
    return 200