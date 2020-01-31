#!/usr/bin/env python
import boto3
import time
import sys
import random
import os

#setup logging for main()

APP_NAMESPACE='User Experince'
APP_NAME='application1'
APP_VERSION='0.1'
METRIC_NAME='Error Rates'
METRIC_REGION='eu-west-2'

def getMetricValue():
    return random.randint(1, 999)

def getCWMetric(appName,appVersion):
    metricValue = getMetricValue()
    dimension1 ={"Name":"application","Value":appName}
    dimension2 ={"Name":"API version","Value":appVersion}
    dimension3 ={"Name":"Instance","Value":os.uname()[1]}
    dimensions =[dimension1,dimension2,dimension3]
    result = {"MetricName":METRIC_NAME,"Dimensions":dimensions,"Unit":"None","Value":metricValue}
    return result

def createClient(region):
    return boto3.client('cloudwatch', region_name=region)

def putMetric(client,payload,namespace):
    try:
        client.put_metric_data(MetricData=[payload],Namespace=namespace)
        print("success:{}".format(payload))
    except Exception as e:
        print("error:{}".format(e))
        sys.exit(1)
    return


def main():
    client = createClient(METRIC_REGION)
    metricData = getCWMetric(APP_NAME,APP_VERSION)
    putMetric(client,metricData,APP_NAMESPACE)
    return

if __name__ == "__main__":
    main()

    return result

def createClient(region):
    return boto3.client('cloudwatch', region_name=region)

def putMetric(client,payload,namespace):
    try:
        client.put_metric_data(MetricData=[payload],Namespace=namespace)
        print("success:{}".format(payload))
    except Exception as e:
        print("error:{}".format(e))
    sys.exit(1)
    return


def main():
    client = createClient(METRIC_REGION)
    metricData = getCWMetric(APP_NAME,APP_VERSION)
    putMetric(client,metricData,APP_NAMESPACE)
    return

if __name__ == "__main__":
    main()