#!/usr/bin/env python
import boto3
from os import environ
import logging
import random
import string
import hmac, base64, hashlib
from jose import jwk, jwt
from jose.utils import base64url_decode
import requests
import json
import time

# logging config
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.WARN,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# globals
MODULE = "cognito-helper"
PROFILE = "aws-dev"
REGION = "eu-west-2"
COGNITO_CLIENT_ID = "5br85tkg2nmq8nn1v8pk71lkku"
COGNITO_CLIENT_SECRET = "nvob2gmc5qcgak315fncnuau5a25vumhicc8s1m62gkn4q2m4gs"
USER_POOL = {"my-app-pool":"eu-west-2_Kho2P5tX1","pool2":"eu-west-2_KUIn3XkBG"}
AWS_KEYS_URL = 'https://cognito-idp.{}.amazonaws.com/{}/.well-known/jwks.json'.format(REGION, USER_POOL['my-app-pool'])

#load keys
response = requests.get(AWS_KEYS_URL)
jresponse = response.json()
KEYS = jresponse['keys']

def get_scope_auth(scope):
    url = "https://widgets.auth.eu-west-2.amazoncognito.com/oauth2/authorize"
    query_params = {"response_type":"token","client_id":COGNITO_CLIENT_ID,"redirect_uri":"https://localhost/login","scope":scope,"state":"STATE"}
    try:
        r = requests.get(url,params=query_params)
        print(r.status_code)
    except ConnectionError as e:
        result = {"error":e}
        print(e)
    except Exception as e:
        result = {"error": e}
        print(e)
    else:
        print(r)
    print(r.url)
    return


def gen_password(stringLength):
    password_characters = string.ascii_letters + string.digits + string.punctuation
    result = ''.join(random.choice(password_characters) for i in range(stringLength))
    return result


def get_mac_digest(username, clientid, clientsecret):
    data = username + clientid
    key_bytes = bytes(clientsecret,'latin-1')
    data_bytes = bytes (data, 'latin-1')
    dig = hmac.new(key_bytes, msg=data_bytes, digestmod=hashlib.sha256).digest()
    return base64.b64encode(dig).decode()

def decribe_rs(client,poolid,resource):
    _pool_id = USER_POOL[poolid]
    result = {}
    try:
        result = client.describe_resource_server(UserPoolId=_pool_id,Identifier=resource)
    except Exception as e:
        result = {"error": str(e)}
    return result

def admin_signout(client,poolid,username):
    _pool_id = USER_POOL[poolid]
    result = {}
    try:
        result = client.admin_user_global_sign_out(UserPoolId=_pool_id, Username=username)
    except Exception as e:
        result = {"error": str(e)}
    return result



def decode_cognito_token(token):
    result = {}
    '''
    from https://github.com/awslabs/aws-support-tools/blob/master/Cognito/decode-verify-jwt/decode-verify-jwt.py
    '''
    try:
        headers = jwt.get_unverified_headers(token)
    except Exception as e:
        result = {"error": str(e)}
        return result
    else:
        kid = headers['kid']
    # search for the kid in the downloaded public keys
    key_index = -1
    for i in range(len(KEYS)):
        if kid == KEYS[i]['kid']:
            key_index = i
            break
    if key_index == -1:
        logging.error('Public key not found in jwks.json')
        result = {"error":"Public key not found in jwks.json"}
        return result
    # construct the public key
    public_key = jwk.construct(KEYS[key_index])
    # get the last two sections of the token,
    # message and signature (encoded in base64)
    message, encoded_signature = str(token).rsplit('.', 1)
    # decode the signature
    decoded_signature = base64url_decode(encoded_signature.encode('utf-8'))
    # verify the signature
    if not public_key.verify(message.encode("utf8"), decoded_signature):
        logging.error('Signature verification failed')
        result = {"error": "Signature verification failed"}
        return result
    logging.info('Signature successfully verified')
    # since we passed the verification, we can now safely
    # use the unverified claims
    claims = jwt.get_unverified_claims(token)
    # additionally we can verify the token expiration
    if time.time() > claims['exp']:
        logging.error('Token has expired')
        result = {"error": "Token has expired"}
        return result
    # and the Audience  (use claims['client_id'] if verifying an access token)
    if claims['token_use'] == 'id':
        if claims['aud'] != COGNITO_CLIENT_ID:
            logging.error('Token was not issued for this audience')
            result = {"error": "Token was not issued for this audience"}
            return result
    if claims['token_use'] == 'access':
        if claims['client_id'] != COGNITO_CLIENT_ID:
            logging.error('Token was not issued for this audience')
            result = {"error": "Token was not issued for this audience"}
            return result

    result['data'] = claims
    return result



def create_client(region):
    if environ.get('CODEBUILD_BUILD_ID') is not None:
        return boto3.client('cognito-idp', region_name=region)
    else:
        session = boto3.Session(profile_name=PROFILE)
        return session.client('cognito-idp', region_name=region)

def admin_auth(client,clientid,clientsecret,poolid,username,password):
    _pool_id = USER_POOL[poolid]
    auth = "USER_PASSWORD_AUTH"
    hash = get_mac_digest(username,clientid,clientsecret)
    try:
        result = client.initiate_auth(ClientMetadata={"UserPoolId":_pool_id},ClientId=clientid, \
                AuthFlow=auth,AuthParameters={'USERNAME':username,"PASSWORD":password,"SECRET_HASH":hash})
    except Exception as e:
        logging.error('error:{}'.format(e))
        result = {"error": e}
    else:
        logging.info("result:{}".format(result))
    return result


def password_challenge(client,clientid,clientsecret,poolid,username,sessionid):
    _pool_id = USER_POOL[poolid]
    challenge = "NEW_PASSWORD_REQUIRED"
    hash = get_mac_digest(username,clientid,clientsecret)
    password = gen_password(20)
    try:
        result = client.admin_respond_to_auth_challenge(UserPoolId=_pool_id,ClientId=clientid, \
        Session=sessionid, ChallengeName=challenge,ChallengeResponses={'NEW_PASSWORD':password,"USERNAME":username,"SECRET_HASH":hash})
    except Exception as e:
        logging.error('error:{}'.format(e))
        result = {"error": e}
    else:
        print("user:{} new_password:{}".format(username, password))
    return result, password


def login(client,user,password,pool):
    result = admin_auth(client, COGNITO_CLIENT_ID, COGNITO_CLIENT_SECRET, pool, user, password)
    if "ChallengeName" in result:
        if result['ChallengeName'] == "NEW_PASSWORD_REQUIRED":
            try:
                new_pass_result, new_password = password_challenge(client, COGNITO_CLIENT_ID, COGNITO_CLIENT_SECRET, pool, user,result['Session'])
            except Exception as e:
                result = {"error":e}
            else:
                result = admin_auth(client, COGNITO_CLIENT_ID, COGNITO_CLIENT_SECRET, pool, user, new_password)
    return result

def main ():
    print('Running:{}'.format(MODULE))
    cidp = create_client(REGION)
    user = "awsdev.packt1@gmail.com"
    password = r"28DI96D%Rv07X*YvQ`7k"
    login_result = login(cidp,user,password,'my-app-pool')
    print(login_result)
    decode_idresult = decode_cognito_token(login_result['AuthenticationResult']['IdToken'])
    print(decode_idresult)
    decode_accresult = decode_cognito_token("eyJraWQiOiJtQUh5Q1dkc0l0c3MwbnZ2d1NRY0YyQjlTYVpqeGg4WmxHZG4wTGJBRzZFPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiIyNDA4MGNmZC04ZTBkLTQ4ODYtODAzNC01MDExN2M2M2ZmODciLCJjb2duaXRvOmdyb3VwcyI6WyJzZWN1cmUiXSwidG9rZW5fdXNlIjoiYWNjZXNzIiwic2NvcGUiOiJhd3MuY29nbml0by5zaWduaW4udXNlci5hZG1pbiBvcGVuaWQgaHR0cHM6XC9cL2xvY2FsaG9zdFwvYXBpXC9jb250ZW50XC9zZWN1cmUyXC93YXJyYW50eS5yZWFkIHByb2ZpbGUiLCJhdXRoX3RpbWUiOjE1NTE2NTQ3MDcsImlzcyI6Imh0dHBzOlwvXC9jb2duaXRvLWlkcC5ldS13ZXN0LTIuYW1hem9uYXdzLmNvbVwvZXUtd2VzdC0yX0tobzJQNXRYMSIsImV4cCI6MTU1MTY1ODMwNywiaWF0IjoxNTUxNjU0NzA3LCJ2ZXJzaW9uIjoyLCJqdGkiOiJjZmU0MzYwMS1lODllLTQwYzItOGMwMi0xM2FkNWY0OGUwMTIiLCJjbGllbnRfaWQiOiI1YnI4NXRrZzJubXE4bm4xdjhwazcxbGtrdSIsInVzZXJuYW1lIjoiYXdzZGV2LnBhY2t0MUBnbWFpbC5jb20ifQ.ZIH8V-NHBr6tgX0bICK49AGAO5sVwxp44gf7MiU4kr_cMZ6V7bkRuEGknjj72clwoHCzamF2wIY1wgdPHkNEDxGUbJnQ17Qj7-EMJuLaqfZ0jy5QRP509xr1SjqOWy8UYG35U96ykkobAEVe6Wz1I_lZ9aLbNbh_XuEXjz3VA6KRP5Pe6S8Hm2mi1kslaVvQFqyBX1AFpt7TOpC4n8F-_hgvyjsYIJ9r_fYG-DEkSFIam_MLfXe7Li8YqX6CCPDr3gXt4FNncEb7iAOEohNuYcUIj5L06a1POcNa4iVKnpylF3wRKkP2czoOvjihHno5V3jjQ-YXAcNt5Q2TMo6k_A")
    print(decode_accresult)
    pool_result = decribe_rs(cidp,'my-app-pool','https://localhost/api/content/secure2')
    #print(pool_result)
    #print(admin_signout(cidp,'my-app-pool',user))
    get_scope_auth("https://localhost/api/content/secure2/warranty.read")


if __name__ == "__main__":
    main()

