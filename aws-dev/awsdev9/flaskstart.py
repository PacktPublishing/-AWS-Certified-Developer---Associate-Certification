#!/usr/bin/env python

from flask import Flask, request,Response
import logging
import os
import json
import redisHelper as rhelp

#logging config
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

#globals
MODULE = "section9"
HOST = "0.0.0.0"
PORT = "8080"
PROFILE = "aws-dev"
REGION = "eu-west-2"
REDIS_HOST = "my-redis-cluster.wyb0da.ng.0001.euw2.cache.amazonaws.com"
REDIS_PASSWD = ""

#initiliase flask
app = Flask(__name__)
app.secret_key = os.urandom(24)

#initalise redis
r_client = rhelp.create_client(REDIS_HOST,REDIS_PASSWD)



@app.route('/api/<string:version>/warranty/upsell',methods=["POST"])
def add_upsell_numbers(version):cat e
    result = {}
    headers = {}
    seller = request.args.get('seller')
    numsold = request.args.get('numsold')
    if seller is not None and numsold is not None:
        postObject = rhelp.add_total_warrentys(r_client,seller,float(numsold))
        if isinstance(postObject,float):
            result['result'] = "ok"
            result['data'] = {"new_total":postObject}
            status = 200
        else:
            if 'error' in postObject:
                result['error'] = postObject['error']
                status = 500
                result['result'] = 'fail'
            else:
                result['result'] = "ok"
                status = 200
    else:
        result['error'] = "invalid request parameters"
        status = 400
        result['result'] = 'fail'
    lresponse = Response(json.dumps(result), status=status, mimetype='application/json', headers=headers)
    return lresponse

@app.route('/api/<string:version>/warranty/upsell',methods=["GET"])
def get_ranked_upsell(version):
    result = {}
    headers = {}
    topnumber = request.args.get('topnumber')
    if topnumber is not None:
        getObject = rhelp.get_warranty_rank(r_client,int(topnumber))
        if 'error' in getObject:
            result['error'] = getObject['error']
            status = 500
            result['result'] = 'fail'
        else:
            result['result'] = "ok"
            result['data'] = getObject
            status = 200

    else:
        result['error'] = "invalid request parameters"
        status = 400
        result['result'] = 'fail'
    lresponse = Response(json.dumps(result), status=status, mimetype='application/json', headers=headers)
    return lresponse



@app.route('/api/<string:version>/health',methods=["GET"])
def get_health(version):
    result = {}
    headers = {}
    result['result'] = "ok"
    status = 200
    lresponse = Response(json.dumps(result), status=status, mimetype='application/json', headers=headers)
    return lresponse


def main ():
    print('Running:{}'.format(MODULE))
    #app.run(debug=True)
    app.run(host='0.0.0.0',port=PORT)
    app.logger.info('Running:{}'.format(MODULE))


if __name__ == "__main__":
    main()

