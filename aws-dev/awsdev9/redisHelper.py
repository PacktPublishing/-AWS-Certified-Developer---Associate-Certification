#!/usr/bin/env python
import logging
import redis
import random
import sys
import json

#logging config
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

#globals
MODULE = "redis helper"
HOST = "127.0.0.1"
HOST = "my-redis-cluster.wyb0da.ng.0001.euw2.cache.amazonaws.com"
PASSWD = ""
ENGINEERS = ["Joe Black","Ferris Beuller","Han Solo","Uncle Buck","Mr Pink"]
ENV = "PROD"
DATA_ADD = 1
ERROR_LIST = {"Error 61":"connection error"}

def obs_error(message,error_list):
    for key,value in error_list.items():
        if key in message:
            return error_list[key]
    return message

def create_client(host,password):
    try:
        if ENV == "LOCAL":
            client = redis.StrictRedis(host=host, port=6379, db=0)
        else:
            client = redis.StrictRedis(host=host, port=6379, db=0, password=password)
    except Exception as e:
        logging.error("module:{} redis connection to {} failed error:{}".format(MODULE, host, str(e)))
        raise e
    else:
        logging.info("created redis server client {}".format(host))
    return client

def add_total_warrentys(client, engineer,score):
    result = {}
    _leader_board_name = "warranty"
    try:
        result = client.zincrby(_leader_board_name, score, engineer)
    except Exception as e:
        result['error'] = obs_error(str(e),ERROR_LIST)
    return result

def get_warranty_rank(client,max):
    result = {}
    _leader_board_name = "warranty"
    try:
        raw_result = client.zrange(_leader_board_name, 0, max, desc=True, withscores=True)
    except Exception as e:
        result['error'] = obs_error(str(e),ERROR_LIST)
    else:
        result = convert_to_string(raw_result)
    return result

def add_bulk_warrantee_data(client,warrantee_data):
    if warrantee_data != None:
        for engineer,value in warrantee_data.items():
            try:
                result = add_total_warrentys(client,engineer,value)
            except Exception as e:
                logging.error("module:{} failed to add {} for engineer {} error:{}".format(MODULE, value, engineer,str(e)))
                raise e
            else:
                print("result for {}:{}".format(engineer,result))
    return

def create_warrantee_data(engineers):
    result = {}
    for engineer in engineers:
        value = random.randint(1, 200)
        result[engineer] = value
    return result

def convert_to_string(redis_rank):
    result = []
    for key, value in redis_rank:
        entry = {key.decode('utf-8'):value}
        result.append(entry)
    return result



def main():
    print("Running {}".format(MODULE))
    ENV = "LOCAL"
    try:
        client = create_client(HOST,PASSWD)
    except Exception as e:
        logging.error("module:{} redis connection to {} failed error:{}".format(MODULE, HOST, str(e)))
    else:
        if DATA_ADD == 1:
            warrentee_list = create_warrantee_data(ENGINEERS)
            try:
                add_bulk_warrantee_data(client,warrentee_list)
            except Exception as e:
                logging.error("module:{} failed to buk add {} failed error:{}".format(MODULE, warrentee_list, str(e)))
                sys.exit(1)
        try:
            top_engineers = get_warranty_rank(client,10)
        except Exception as e:
            logging.error(
                "module:{} failed to get ranking  error:{}".format(MODULE, str(e)))
        else:
            result = {"result":"ok","data":top_engineers}
            print("result:{}".format(json.dumps(result)))

    return


if __name__ == "__main__":
    main()
