#!/usr/bin/env python
"""Using application.py as the filename and providing a callable application object
(the Flask object, in this case) allows Elastic Beanstalk to easily find your application's code.
"""
import logging
import os
from flask import Flask, render_template


#logging config
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.WARN,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

#globals
MODULE = "section-7-elastic-beanstalk-website"
HOST = "0.0.0.0"
PORT = "8080"

# EB looks for an 'application' callable by default.
application = Flask(__name__)
application.secret_key = os.urandom(24)


@application.route("/")
def home():
    return render_template("index.html")

def main():
    print('Running:{}'.format(MODULE))
    application.run(host=HOST,port=PORT,debug=True)


if __name__ == "__main__":
    main()
