import json
import time
import uuid
import copy
import base64
import platform
import threading
import subprocess 
import os,logging
from functools import wraps
from shutil import copyfile
from datetime import datetime
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC


VERSION="1.0.8"
MODULE="NYX_Lambda"
QUEUE=[]

global_message=""
global_destination=""
global_headers=""

elkversion=6

################################################################################
def getELKVersion(es):
    return int(es.info().get('version').get('number').split('.')[0])


################################################################################
def messageReceived(destination,message,headers):
    global es,global_message,global_destination,global_headers

    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info("<== "*10)
    global_message=message
    global_destination=destination
    global_headers=headers

    if destination in lambdasht:
        for lamb in lambdasht[destination]:
            logger.info(">>> Calling %s" %(lamb["function"]))
            lamb["lastrun"]=datetime.now().isoformat()
            starttime=datetime.now()
            try:
                lamb["return"]="CRASH"
                ret=lamb["code"]()
                lamb["return"]=str(ret)
            except:
                logger.error("Lambda "+lamb["function"]+" crashed.",exc_info=True)
                lamb["errors"]+=1
                lamb["return"]="FAILURE"
            finally:
                lamb["runs"]+=1
                duration=(datetime.now()-starttime).total_seconds()
                lamb["duration"]=duration



################################################################################
def loadConfig():
    global lambdas
    global lambdasht
    global path

    lambdas=[]
    lambdasht={}
    files=[]

    for r, d, f in os.walk(path):
        for file in f:
            if '.ipynb' in file and not 'checkpoint' in file:
                files.append(os.path.join(r, file))

    for f in files:
        with open(f, 'r') as content_file:
            print(f)
            content = content_file.read()
            jsoncontent=json.loads(content)
            for cell in jsoncontent["cells"]:
                if cell["cell_type"]=="code":
                    if len(cell["source"])>0 and "#@LISTEN=" in cell["source"][0] and "#@CALLBACK=" in cell["source"][1]:
                        topics= cell["source"][0][9:].strip().split(",")
                        function= cell["source"][1][11:].strip()                    
                        newcode="".join(cell["source"])
                        exec(newcode)
                        lambdas.append({"file":f,"topics":topics,"runs":0,"errors":0,"function":function,"code":eval(function)})

    for lamb in lambdas:
        for topic in lamb["topics"]:
            if topic in lambdasht:
                lambdasht[topic].append(lamb)
            else:
                lambdasht[topic]=[lamb]

################################################################################
def checkIfRequirementsChanged():
    global path,requimentstime

    curf=path+'/requirements.txt'
    prevf=path+'/requirements.txt.prev'

    if not os.path.isfile(curf):
        return False
    
    if not os.path.isfile(prevf):
        copyfile(curf,prevf)
    
    elif requimentstime==0:
        
        with open(curf, 'r') as content_file:            
            contentcur = content_file.read()

        with open(prevf, 'r') as content_file:            
            contentprev = content_file.read()

        if contentcur==contentprev:
            requimentstime=os.path.getmtime(curf)
            return False

    elif os.path.getmtime(curf)==requimentstime:
        return False
    
    logger.info("Must install requirements...")
    copyfile(curf,prevf)
    ret=subprocess.Popen(["pip","install","-r","requirements.txt"],cwd=path, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    #ret=subprocess.Popen([exec.split('/')[-1:][0]],cwd=path, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout = ret.communicate()[0]
    for line in stdout.decode("utf-8").split('\n'):
        logger.info(line)
    logger.info("Shell Return Code:")
    logger.info(ret.returncode)

    return True
    






################################################################################
def checkIfChanged(inconfig):
    global curconfig,path
    
    fileasstr=""
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.ipynb' in file and not 'checkpoint' in file:
                fileasstr+=os.path.join(r, file)+","+str(os.path.getmtime(path))

    if fileasstr==curconfig:
        return False

    curconfig=fileasstr
    return True

logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

lshandler=None
curconfig=""
lambdas=[]
lambdasht={}
path="./notebooks"
requimentstime=0

if __name__ == '__main__': 

    MODULE+="_"+str(os.environ["RUNNER"])

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

    checkIfChanged(curconfig)
    loadConfig()
    for topic in lambdasht:
        QUEUE.append(topic)

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

    elkversion=getELKVersion(es)

       
    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    while True:
        time.sleep(5)
        try:            
            elkversion=getELKVersion(es)

            variables={"platform":"_/_".join(platform.uname()),"icon":"file-code"}
            conn.send_life_sign(variables)
        except Exception as e:
            logger.error("Unable to send life sign.",exc_info=True)
            logger.error(e)
        try:
            if(checkIfChanged(curconfig)):
                logger.info(">>>>> Config changed")
                logger.info(curconfig)
                loadConfig()
                newqueues=",".join([_ for _ in lambdasht])
                if newqueues!=",".join(QUEUE):
                    logger.info(">>>>>>> SUBSCRIPTION changed...QUITTING")
                    conn.disconnect()
                    break
        except Exception as e:
            logger.error("Unable to check changed.",exc_info=True)
            logger.error(e)

        try:
            if(checkIfRequirementsChanged()):
                logger.info(">>>>>>> Requirements changed...QUITTING")
                conn.disconnect()
                break
        except Exception as e:
            logger.error("Unable to check requirements.",exc_info=True)
            logger.error(e)

        try: # SEND lambda updates if any
            bulkbody=""
            for lamb in lambdas:
                if lamb["runs"]>=1:
                    logger.info("Updating lambda stats")
                    upsert={
                        "script" : {
                            "source": "ctx._source.errors += params.errors;ctx._source.runs += params.runs;ctx._source.lastrun = params.lastrun;ctx._source.return = params.return;",
                            "lang": "painless",
                            "params" : {
                                "errors" : lamb["errors"],
                                "runs" : lamb["runs"],
                                "lastrun" : lamb["lastrun"],
                                "return" : lamb["return"]
                            }
                        }
                    }

                    upsert["upsert"]=copy.deepcopy(lamb)
                    lamb["errors"]=0
                    lamb["runs"]=0

                    del upsert["upsert"]["code"]
                    if elkversion<=6:
                        action={ "update" : {"_id" :("R"+str(os.environ["RUNNER"])+"_"+lamb["function"]).lower(), "_index" : "nyx_lambda", "retry_on_conflict" : 1,"_type":"_doc"} }
                    else:
                        action={ "update" : {"_id" :("R"+str(os.environ["RUNNER"])+"_"+lamb["function"]).lower(), "_index" : "nyx_lambda", "retry_on_conflict" : 1} }
                    bulkbody+=json.dumps(action)+"\r\n"
                    bulkbody+=json.dumps(upsert)+"\r\n"
            if len(bulkbody)>0:
                logger.info(bulkbody)
                res=es.bulk(bulkbody)
                logger.info(res)


        except Exception as e:
            logger.error("Unable to update lambda stats.",exc_info=True)
            logger.error(e)



    #app.run(threaded=True,host= '0.0.0.0')