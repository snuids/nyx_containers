"""
NYX FORMAT CONVERTER
====================================
Convert a file into another format using open office.
The file to convert must be sent to the queue: */queue/NYX_CONVERTER* encoded in **base 64**.

..  warning::
    Converting docx complex document to pdf can be tricky for open office and can give poor results.


Payload
-------

.. code-block:: json
   :linenos:

   {
       "originalformat":"xls",
        "targetformat":"csv",
        "destination":"/queue/CONVERTED",
        "data":"msg.payload"}
"""

import re
import json
import time
import uuid
import base64
import platform
import traceback
import threading
import traceback
import subprocess 
import os,logging,sys



from datetime import timedelta
from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from dateutil import parser

VERSION="0.0.4"
MODULE="FormatConverter"
QUEUE=["/queue/NYX_CONVERTER"]

################################################################################
def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    #logger.info(message)
    messagejson=json.loads(message)
    #logger.info(message)

    if "targetformat" not in messagejson:
        logger.error("Target format not defined")

    if "originalformat" not in messagejson:
        logger.error("Original format not defined")

    destination=""
    if "destination" not in messagejson:
        logger.error("Destination not defined")

    logger.info("ORIG FORMAT   : "+messagejson["originalformat"])
    logger.info("TARGET FORMAT : "+messagejson["targetformat"])
    logger.info("DESTINATION   : "+messagejson["destination"]) 
    logger.info("FILENAME      : "+messagejson["filename"]) 

    file="./toconvert."+messagejson["originalformat"]
    fbytes = base64.b64decode(messagejson["data"])
    f = open(file, 'wb')
    f.write(fbytes)
    f.close()

    #commandline=os.environ["COMMAND_LINE"].replace("{{FORMAT}}",messagejson["targetformat"]).replace("{{SOURCE}}",file)
    #logger.info("COMMAND   : "+commandline)

    logger.info("Converting.")
    #subprocess.run([commandline],cwd=".")
    subprocess.run(["./convert.sh","toconvert."+messagejson["originalformat"],messagejson["targetformat"]],cwd="./shellscripts")

    logger.info("Conversion done")
    convertedfile="./toconvert."+messagejson["targetformat"]
    fileContent=None
    with open(convertedfile, mode='rb') as file: # b is important -> binary
        fileContent = file.read()
    
    base64data=base64.b64encode(fileContent)
#    logger.info(base64data)
    conn.send_message(messagejson["destination"],base64data,{"file":messagejson["filename"]})
    logger.info("<== "*10)

if __name__ == '__main__':    
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger()


    lshandler=None

    if os.environ["USE_LOGSTASH"]=="true":
        logger.info ("Adding logstash appender")
        lshandler=AsynchronousLogstashHandler("logstash", 5001, database_path='logstash_test.db')
        lshandler.setLevel(logging.ERROR)
        logger.addHandler(lshandler)

    handler = TimedRotatingFileHandler("logs/"+MODULE+".log",
                                    when="d",
                                    interval=1,
                                    backupCount=30)

    logFormatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s')
    handler.setFormatter( logFormatter )
    logger.addHandler(handler)

    logger.info("==============================")
    logger.info("Starting: %s" % MODULE)
    logger.info("Module:   %s" %(VERSION))
    logger.info("==============================")

    #>> AMQC
    server={"ip":os.environ["AMQC_URL"],"port":os.environ["AMQC_PORT"]
                    ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"]
                    ,"heartbeats":(120000,120000),"earlyack":True}
    logger.info(server)                
    conn=amqstompclient.AMQClient(server
        , {"name":MODULE,"version":VERSION,"lifesign":"/topic/NYX_MODULE_INFO"},QUEUE,callback=messageReceived)
    #conn,listener= amqHelper.init_amq_connection(activemq_address, activemq_port, activemq_user,activemq_password, "RestAPI",VERSION,messageReceived)
    connectionparameters={"conn":conn}

    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])

    nextrun=datetime.now()

    SECONDSBETWEENCHECKS=10

    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"transgender"}
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')