#!/usr/bin/env python
import boto3
import sys
import os

profile = 'aws-dev'
account = "507038840153"
role = "s3-role-mfa"
mfaARN = "arn:aws:iam::763988453897:mfa/sysdev"


def main():
	boto3.setup_default_session(profile_name=profile)
	credentials = getSTSCreds(account, role)
	listS3STS(credentials)
	return

def getMFA():
	return input("Enter the MFA code: ")

def getSTSCreds(account,role):
	arn = "arn:aws:iam::" + account + ":role/" + role
	mfaToken = getMFA()
	sts_client = boto3.client('sts')
	try:
		assumedRoleObject = sts_client.assume_role(RoleArn=arn, SerialNumber=mfaARN, TokenCode=mfaToken, RoleSessionName=role + "_session")
	except Exception as e:
		print("error in assume role:{}".format(e))
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


if __name__ == "__main__":
	main()


