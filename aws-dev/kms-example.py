#!/usr/bin/env python
import base64
import boto3
import sys

CMK_ID = 'arn:aws:kms:eu-west-2:763988453897:key/1effa909-2496-4c8c-9a5f-eb4bfd43d516'
REGION = 'eu-west-2'
PROFILE = 'aws-dev'

def createClient(region):
        return boto3.client('kms', region_name=region)

def encrytData(client,key,plaintext):
        try:
                result = client.encrypt(KeyId=key,Plaintext=plaintext)
        except Exception as e:
                print("error: {}".format(e))
                sys.exit(1)
        return result

def decrytData(client,ciphertext):
        try:
            result = client.decrypt(CiphertextBlob=ciphertext)
        except Exception as e:
            print("error: {}".format(e))
            sys.exit(1)
        return result

def getDataKey(client,key):
                try:
                        result = client.generate_data_key(KeyId=key,KeySpec='AES_256')
                except Exception as e:
                        print("error: {}".format(e))
                        sys.exit(1)
                return result

def main():
        boto3.setup_default_session(profile_name=PROFILE)
        client = createClient(REGION)
        text = 'thisismydatakey'
        eresponse = encrytData(client,CMK_ID,text)
        print()
        print('Encrypted client provided datakey:{}'.format(base64.b64encode(eresponse['CiphertextBlob'])))
        print()
        dresponse = decrytData(client,eresponse['CiphertextBlob'])
        print('Decrypted client provided datakey:{}'.format(dresponse['Plaintext'].decode()))
        print()
        kresponse = getDataKey(client,CMK_ID)
        plaintext = base64.b64encode(kresponse['Plaintext'])
        print('plaintext KMS provided datakey:{}'.format(plaintext))
        print()
        return

if __name__ == "__main__":
        main()
