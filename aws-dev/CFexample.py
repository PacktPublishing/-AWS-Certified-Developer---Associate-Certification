#!/usr/bin/env python
import json
import boto3
import sys
from os import environ

REGION = 'eu-west-2'
PROFILE = 'aws-dev'
templates = ("cf-iam-groups","cf-iam-std-dev-policy","cf-iam-dev-user","cf-vpc-dev")
count = 0

def createClient(region):
    if environ.get('CODEBUILD_BUILD_ID') is not None:
        return boto3.client('cloudformation', region_name=region)
    else:
        session = boto3.Session(profile_name=PROFILE)
        return session.client('cloudformation', region_name=region)


def validateTemplate(client,templateName):
    try:
        templateFileName = templateName + r".json"
        with open(templateFileName,'r') as tfile:
            template = tfile.read() 
            try:
                result = client.validate_template(TemplateBody=template)
                print('File {} is OK'.format(templateFileName))
            except Exception as e:
                print("error: {}".format(e))
                sys.exit(1)
    except Exception as e:
            print("error: {}".format(e))
            sys.exit(1)
    return result['ResponseMetadata']['HTTPStatusCode']


def stackExist(client,stackName):
    stacks = client.list_stacks()['StackSummaries']
    for stack in stacks:
        if stack['StackStatus'] == 'DELETE_COMPLETE':
            continue
        if stackName == stack['StackName']:
            return True
    return False

def createStack(client, stackName, templateName):
    templateFileName = templateName + r".json"
    paramFileName = templateName + "_param" + r".json"
    try:
        template = open(templateFileName, 'r').read()
        request = {'StackName': stackName, 'TemplateBody': template}
    except Exception as e:
        print("error: {}".format(e))
        sys.exit(1)
    try:
        parms = open(paramFileName, 'r').read()
        jparms = json.loads(parms)
        request['Parameters'] = jparms
    except Exception as e:
        pass
    if "iam" in templateName:
        request['Capabilities'] = ['CAPABILITY_NAMED_IAM']
    try:
        result = client.create_stack(**request)
        waiter = client.get_waiter('stack_create_complete')
        print("...waiting for stack to be ready...")
        waiter.wait(StackName=stackName)
    except Exception as e:
        print("error: {}".format(e))
        sys.exit(1)
    stackDetail=(client.describe_stacks(StackName=result['StackId']))
    print(stackDetail)
    return




def main():
    stack = templates[count]
    client = createClient(REGION)
    result = validateTemplate(client,templates[count])
    if result == 200 and not stackExist(client,stack):
        createStack(client,stack,templates[count])
    elif stackExist(client,stack):
        print("Stack Name already exist")
    else:
        print("failed with error code{}".format(result))

if __name__ == "__main__":
    main()
