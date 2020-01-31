#!/usr/bin/env python
import base64
import boto3
import sys
from os import environ
import logging
import argparse
import Crypto.Cipher.AES as AES
from Crypto import Random

pad = lambda s: s + (32 - len(s) % 32) * ' '
REGION = 'eu-west-2'
PROFILE = 'aws-dev'

parser = argparse.ArgumentParser()
parser.add_argument("-secret", help="the secret you want to encrypt")
parser.add_argument("-cmk_id", help="the ARN of the CMK you want to use")

#logging config
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def createClient(region):
    if environ.get('CODEBUILD_BUILD_ID') is not None:
        return boto3.client('kms', region_name=region)
    else:
        logging.info('using profile {}'.format(PROFILE))
        session = boto3.Session(profile_name=PROFILE)
        return session.client('kms', region_name=region)

def getDataKey(client,cmk):
    try:
        result = client.generate_data_key(KeyId=cmk,KeySpec='AES_256')
    except Exception as e:
        print("error: {}".format(e))
        sys.exit(1)
    else:
        return result.get('Plaintext'), result.get('CiphertextBlob')

def getPlainTextKey(client,encryted_key):
    try:
        result = client.decrypt(CiphertextBlob=encryted_key).get('Plaintext')
    except Exception as e:
        print("error: {}".format(e))
        sys.exit(1)
    else:
        return result

def encrytData(key,secret):
    aes = AES.new(key) #no IV demo only
    return base64.b64encode(aes.encrypt(pad(secret))) 

def decrytData(key,ciphertext):
    aes = AES.new(key) #no IV demo only
    return aes.decrypt(base64.b64decode(ciphertext)).rstrip() 

def main():
        args = parser.parse_args()
        client = createClient(REGION)
        plaintext,encrypted  = getDataKey(client,args.cmk_id) 
        logging.info("plaintext key:{}".format(plaintext))
        cypertext = encrytData(plaintext,args.secret)
        logging.info("encrypted secret:{} ".format(cypertext))
        plaintext2 = getPlainTextKey(client,encrypted)
        original_secret = decrytData(plaintext2,cypertext)
        logging.info("original secret:{} ".format(original_secret.decode('utf-8')))
        return

if __name__ == "__main__":
        main()
