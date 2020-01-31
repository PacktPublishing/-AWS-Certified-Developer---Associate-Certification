#!/usr/bin/env python

from flask import Flask, request,Response
import logging
import os
import json
import cognitoHelper as cog
#logging config
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

#globals
MODULE = "section8"
HOST = "0.0.0.0"
PORT = "8080"
PROFILE = "aws-dev"
REGION = "eu-west-2"
PROFILE = "aws-dev"
REGION = "eu-west-2"
COGNITO_CLIENT_ID = "5br85tkg2nmq8nn1v8pk71lkku"
COGNITO_CLIENT_SECRET = "nvob2gmc5qcgak315fncnuau5a25vumhicc8s1m62gkn4q2m4gs"
USER_POOL = "my-app-pool"

#initiliase flask
app = Flask(__name__)
app.secret_key = os.urandom(24)
cidp = cog.create_client(REGION)


@app.route('/api/<string:version>/auth/login',methods=["POST"])
def loginUser(version):
    result = {}
    headers = {}
    username = request.authorization.username
    password = request.authorization.password
    authObject = cog.login(cidp,username,password,USER_POOL)
    if 'error' in authObject:
        if 'User is disabled' in str(authObject['error']):
            result['error'] = "user disabled"
        else:
            result['error'] = str(authObject['error'])
        status = 401
        result['result'] = 'fail'
    else:
        result['result'] = "ok"
        result['data'] = authObject['AuthenticationResult']
        status = 200
    lresponse = Response(json.dumps(result), status=status, mimetype='application/json',headers=headers)
    if status == 200:
        lresponse.set_cookie("idtoken",authObject['AuthenticationResult']['IdToken'],httponly=True,expires=None)
    return lresponse

@app.route('/api/<string:version>/content/warranty',methods=["POST"])
def secure(version):
    resource_path = request.path
    result = {}
    headers = {}
    idtoken = request.cookies.get("idtoken")
    if request.args.get('accesstoken'):
        access_token = request.args.get('accesstoken')
        try:
            tokenObject = cog.decode_cognito_token(access_token)
        except Exception as e:
            status = 500
            result['error'] = str(e)
        else:
            if 'error' in tokenObject:
                result['error'] = tokenObject['error']
                status = 403
                result['result'] = 'fail'
            else:
                found = 0
                if str(tokenObject['data']['scope']).find(resource_path) == 0:
                    found = 1
                if found == 1:
                    result['result'] = "ok"
                    result['data'] = tokenObject['data']
                    status = 200
                else:
                    status = 403
                    result['resource'] = resource_path
                    result['result'] = 'fail'
                    result['error'] = "Not in scope, scope=" + tokenObject['data']['scope']
    else:
        result['error'] = "no accesstoken specified"
        status = 400
        result['result'] = 'fail'
    lresponse = Response(json.dumps(result), status=status, mimetype='application/json', headers=headers)
    return lresponse

@app.route('/api/<string:version>/auth/whoami',methods=["POST"])
def whoami(version):
    result = {}
    headers = {}
    idtoken = request.cookies.get("idtoken")
    tokenObject = cog.decode_cognito_token(idtoken)
    if 'error' in tokenObject:
        result['error'] = tokenObject['error']
        status = 401
        result['result'] = 'fail'
    else:
        result['result'] = "ok"
        result['data'] = tokenObject
        status = 200
    lresponse = Response(json.dumps(result), status=status, mimetype='application/json',headers=headers)
    return lresponse


def main ():
    print('Running:{}'.format(MODULE))
    app.run(debug=True)
    #app.run(host='0.0.0.0',port=PORT)
    app.logger.info('Running:{}'.format(MODULE))


if __name__ == "__main__":
    main()


