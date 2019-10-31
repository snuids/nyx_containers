"""
NYX XLS IMPORTER
====================================
Listens to the queue FILE_UPLOAD_XLS for an base64 encoded XLS file and saves it as an elasticsearch collection.
If the column _index is no specified, the name of the inded is extracted from the Excel file name. (without the extension)
A field mapping is created if it does not already exists. (Using the Excel column types)


Sends:
-------------------------------------

* /topic/NYX_LOG

Listens to:
-------------------------------------

* /queue/FILE_UPLOAD_XLS

VERSION HISTORY
===============

* 25 Oct 2019 0.0.4 **AMA** First version
"""
import json
import time
import uuid
import base64
import threading
import os,logging
import pandas as pd

from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from lib import pandastoelastic as pte


VERSION="0.0.4"
MODULE="XLS_IMPORTER"
QUEUE=["FILE_UPLOAD_XLS"]

def log_message(message):
    global conn

    message_to_send={
        "message":message,
        "@timestamp":datetime.now().timestamp() * 1000,
        "module":MODULE,
        "version":VERSION
    }
    logger.info("LOG_MESSAGE")
    logger.info(message_to_send)
    conn.send_message("/queue/NYX_LOG",json.dumps(message_to_send))

################################################################################
def messageReceived(destination,message,headers):
    global es
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)
    if "file" in headers:
        logger.info("File:%s" %headers["file"])
    log_message("Import of file [%s] started." % headers["file"])        
    xlsbytes = base64.b64decode(message)
    f = open('./tmp/excel.xlsx', 'wb')
    f.write(xlsbytes)
    f.close()

    xlsdf=pd.read_excel('./tmp/excel.xlsx', index_col=None)    
    xlsname=headers["file"]

    if "_index" not in xlsdf:
        xlsdf["_index"]=xlsname.split('.')[0].replace(" ","").lower()        

    request_body = {
        "settings" : {
            "number_of_shards": 5,
            "number_of_replicas": 1
        },

        # 'mappings': {
        #     'doc': {
        #         'properties': {
        #         }}}

        'mappings': {
            
                'properties': {
                }}
    }
    for index,col in enumerate(xlsdf.columns):
        if(col=="_index"):
            continue
        if(col=="_id"):
            continue        
        logger.info(col+" "+str(xlsdf.dtypes[index]))
        typestr=str(xlsdf.dtypes[index])
        finaltype="keyword"
        
        if "int" in typestr:
            finaltype="long"
        elif "float" in typestr:
            finaltype="float"
        elif "date" in typestr:
            finaltype="date"
            
    #    finaltype="keyword"
        #request_body['mappings']['doc']['properties'][col]={'type':finaltype}
        request_body['mappings']['properties'][col]={'type':finaltype}
    #res=es.indices.get(index="nyx_user")
    #res
    for ind in xlsdf["_index"].unique():
        logger.info("CHECKING %s" %ind)
        mapping=None
        try:
            mapping=es.indices.get(index=ind)
        except:
            logger.info("Mapping not found")
        if mapping==None:
            logger.info("Adding %s" %ind)
            es.indices.create(index=ind,body=request_body)
            log_message("Creating index [%s]." % ind)    

            
    pte.pandas_to_elastic(es,xlsdf)
    endtime = time.time()
    log_message("Import of file [%s] finished. Duration: %d Records: %d." % (headers["file"],(endtime-starttime),xlsdf.shape[0]))    

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
                    ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"]}
    logger.info(server)                
    conn=amqstompclient.AMQClient(server
        , {"name":MODULE,"version":VERSION,"lifesign":"/topic/NYX_MODULE_INFO"},QUEUE,callback=messageReceived)
    #conn,listener= amqHelper.init_amq_connection(activemq_address, activemq_port, activemq_user,activemq_password, "RestAPI",VERSION,messageReceived)
    connectionparameters={"conn":conn}

    #>> ELK
    es=None
    logger.info (os.environ["ELK_SSL"])

    if os.environ["ELK_SSL"]=="true":
        host_params = {'host':os.environ["ELK_URL"], 'port':int(os.environ["ELK_PORT"]), 'use_ssl':True}
        es = ES([host_params], connection_class=RC, http_auth=(os.environ["ELK_LOGIN"], os.environ["ELK_PASSWORD"]),  use_ssl=True ,verify_certs=False)
    else:
        host_params="http://"+os.environ["ELK_URL"]+":"+os.environ["ELK_PORT"]
        es = ES(hosts=[host_params])


    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    while True:
        time.sleep(5)
        try:            
            conn.send_life_sign()
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')