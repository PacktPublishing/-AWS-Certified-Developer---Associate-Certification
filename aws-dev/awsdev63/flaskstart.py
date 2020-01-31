#!/usr/bin/env python

from flask import Flask, request,Response
import logging
import os
import json
import dynamodbHelper as db
import timeit

#logging config
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

#globals
MODULE = "section6-video4"
HOST = "0.0.0.0"
PORT = "8080"
PROFILE = "aws-dev"
REGION = "eu-west-2"
TABLE_MAPPER = {"productguides":"productManuals"}
CONTENT_MAPPER = {"product":"pfamily-contenttype-index"}
DEFAULT_TABLE = "productguides"

#initiliase flask
app = Flask(__name__)
app.secret_key = os.urandom(24)
ddb = db.createClient(REGION)

@app.route('/api/<string:version>/guides/<string:mtype>',methods=["GET"])
def getProductGuides(version,mtype):
    """get a list of product manuals and their S3 location"""
    if mtype in TABLE_MAPPER:
        start = timeit.timeit()
        HTTPresponse = db.getTable(ddb,TABLE_MAPPER[mtype])
        stop = timeit.timeit()
        HTTPresponse['timing'] = stop - start
        if 'error' not in HTTPresponse:
            status = 200
            HTTPresponse['request'] = 'ok'
        else:
            status = 500
            HTTPresponse['request'] = 'fail'
    else:
        status = 404
        HTTPresponse = {"error":"invalid resource","request":"fail"}
    return Response(json.dumps(HTTPresponse), status=status, mimetype='application/json')

@app.route('/api/<string:version>/content/guides/<string:ctype>',methods=["GET"])
def getFamilyContent(version,ctype):
    """get a list of conent types for a product family"""
    try:
        family = request.args.get('product-family')
        if ctype in CONTENT_MAPPER:
            start = timeit.timeit()
            HTTPresponse = db.queryIndex(ddb,TABLE_MAPPER[DEFAULT_TABLE],CONTENT_MAPPER[ctype],family)
            stop = timeit.timeit()
            HTTPresponse['timing'] = stop - start
            if 'error' not in HTTPresponse:
                status = 200
                HTTPresponse['request'] = 'ok'
            else:
                status = 500
                HTTPresponse['request'] = 'fail'
        else:
            status = 404
            HTTPresponse = {"error":"invalid resource","request":"fail"}
    except Exception as e:
        status = 400
        HTTPresponse = {"error": "invalid query string", "request": "fail"}
    return Response(json.dumps(HTTPresponse), status=status, mimetype='application/json')



def main ():
    print('Running:{}'.format(MODULE))
    app.run(debug=True)
    #app.run(host='0.0.0.0',port=PORT)
    app.logger.info('Running:{}'.format(MODULE))


if __name__ == "__main__":
    main()


