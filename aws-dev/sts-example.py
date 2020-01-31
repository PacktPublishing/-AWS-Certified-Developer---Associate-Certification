#!/usr/bin/env python
import boto3
import sys
import os

profile = 'aws-dev-child'
profile = 'aws-dev'

account = "507038840153"
role = "s3-role"


def main():
	boto3.setup_default_session(profile_name=profile)
	if 'child' in profile:
		listS3(profile)
	else:
		credentials = getSTSCreds(account, role)
		listS3STS(credentials)
	return

def getSTSCreds(account,role):
	arn = "arn:aws:iam::" + account + ":role/" + role`
	sts_client = boto3.client('sts')
	try:
		assumedRoleObject = sts_client.assume_role(RoleArn=arn,
												   RoleSessionName=role + "_session")
	except Exception as e:
		print("error:{}".format(e))
		sys.exit(1)
	return assumedRoleObject['Credentials']

def listS3STS(credentials):
	print('using STS credentials')
	try:
		s3_resource = boto3.resource('s3',aws_access_key_id = credentials['AccessKeyId'],aws_secret_access_key = credentials['SecretAccessKey'],aws_session_token = credentials['SessionToken'])
		for bucket in s3_resource.buckets.all():
			print(bucket.name)
	except Exception as e:
		print("error:{}".format(e))
		sys.exit(1)

def listS3(tprofile):
	print('using CLI profile')
	try:
		s3_resource = boto3.resource('s3')
		for bucket in s3_resource.buckets.all():
			print(bucket.name)
	except Exception as e:
		print("error:{}".format(e))
		sys.exit(1)

if __name__ == "__main__":
	main()


