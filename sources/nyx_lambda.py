"""
NYX LAMBDA
====================================

Runs code stored in notebooks using two triggers:

* Interval
* Message received

VERSION HISTORY
===============

* 27 Nov 2019 1.0.16 **AMA** First version
* 30 Nov 2019 1.0.17 **AMA** Common Section added
"""
import re
import json
import time
import uuid
import copy
import base64
import platform
import threading
import traceback
import subprocess 
import os,logging
import humanfriendly
from functools import wraps
from shutil import copyfile
from datetime import datetime
from datetime import timedelta
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC


VERSION="1.0.17"
MODULE="NYX_Lambda"
QUEUE=[]

global_message=""
global_destination=""
global_headers=""

thread_check_intervals = None
intervalfn_lock = threading.RLock()

elkversion=6
logs=[]

################################################################################
def getELKVersion(es):
    return int(es.info().get('version').get('number').split('.')[0])


################################################################################
def messageReceived(destination,message,headers):
    global es,global_message,global_destination,global_headers,logs

    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    
    with intervalfn_lock:
        
        logger.info("<== "*10)

        global_message=message
        global_destination=destination
        global_headers=headers

        if destination in lambdasht:
            for lamb in lambdasht[destination]:
                logs=[]
                logger_info(">>> Calling %s" %(lamb["function"]))
                lamb["lastrun"]=datetime.utcnow().isoformat()
                starttime=datetime.utcnow()
                try:
                    ret=lamb["code"]()
                    lamb["return"]=str(ret)
                except:
                    logger_error("Lambda "+lamb["function"]+" crashed.",exc_info=True)
                    tb = traceback.format_exc()
                    for trace in ["TRACE:"+lamb["function"]+":"+_ for _ in tb.split("\n")]:
                        logger_info(trace)

                    lamb["errors"]+=1
                    lamb["return"]="FAILURE"
                finally:
                    lamb["logs"]=json.dumps(logs)
                    lamb["runs"]+=1
                    duration=(datetime.utcnow()-starttime).total_seconds()*1000
                    lamb["duration"]=duration



################################################################################
def logger_info(log,exc_info=False):
    logger.info(log,exc_info=exc_info)
    logs.append("%s\tINFO\t%s" %(datetime.now().strftime("%d%b%Y %H:%M:%S.%f"),log))

def logger_error(log,exc_info=False):
    logger.error(log,exc_info=exc_info)
    logs.append("%s\tERROR\t%s" %(datetime.now().strftime("%d%b%Y %H:%M:%S.%f"),log))

def logger_debug(log,exc_info=False):
    logger.debug(log,exc_info=exc_info)
    logs.append("%s\tDEBUG\t%s" %(datetime.now().strftime("%d%b%Y %H:%M:%S.%f"),log))

def logger_warn(log,exc_info=False):
    logger.warn(log,exc_info=exc_info)
    logs.append("%s\tWARN\t%s" %(datetime.now().strftime("%d%b%Y %H:%M:%S.%f"),log))

def logger_fatal(log,exc_info=False):
    logger.fatal(log,exc_info=exc_info)
    logs.append("%s\tINFO\t%s" %(datetime.now().strftime("%d%b%Y %H:%M:%S.%f"),log))

################################################################################
def computeNewCode(newcode,common):
    if len(common)>0:
        common2="".join(["    "+_+"\n" for _ in common.split("\n")])
    else:
        return newcode.replace("logger.","logger_")

    startdef=newcode.find("\ndef ")
    enddef=newcode.find("):",startdef)
    if enddef>=0:
        newcode2=newcode[0:enddef+3]+common2+newcode[enddef+3:]
    return newcode2.replace("logger.","logger_")

################################################################################
def loadConfig():
    global lambdas
    global lambdasht
    global path

    lambdas=[]
    lambdasht={}
    files=[]
    common=""
    
    regex_fn = r'(^ *)def (\w+)\(.*?\):'

    for r, d, f in os.walk(path):
        for file in f:
            if '.ipynb' in file and not 'checkpoint' in file:
                files.append(os.path.join(r, file))

# LOAD COMMON
    for f in files:
        with open(f, 'r') as content_file:
            content = content_file.read()
            jsoncontent=json.loads(content)
            for cell in jsoncontent["cells"]:

                if cell["cell_type"]=="code":
                    cell['source'] = [_ for _ in cell['source'] if _ != '\n']                    
                    interval = 0
                    topics = []
                    
                    if len(cell['source']) > 1:                        
                        if "#@COMMON" in  cell['source'][0]:
                            common+="".join(cell["source"])

# LOAD FUNCTIONS
    for f in files:
        with open(f, 'r') as content_file:
            content = content_file.read()
            jsoncontent=json.loads(content)
            for cell in jsoncontent["cells"]:

                if cell["cell_type"]=="code":
                    cell['source'] = [_ for _ in cell['source'] if _ != '\n']                    
                    interval = 0
                    topics = []
                    
                    if len(cell['source']) > 1:
                        if "#@LISTEN=" in  cell['source'][0]:
                            topics = cell["source"][0][9:].strip().split(",")

                        elif "#@CRON=" in  cell['source'][0]:
                            pass

                        elif "#@INTERVAL=" in  cell['source'][0]:
                            interval = humanfriendly.parse_timespan(cell["source"][0][11:].strip())
                        else:
                            continue
                            
                        function = None
                        for i in cell['source'][1:]:
                            res = re.search(regex_fn, i)
                            if res:
                                function = res.groups()[1]
                                break

                        if function is not None:
                            newcode="".join(cell["source"])
                            newcode=computeNewCode(newcode,common)
                            exec(newcode)
                            lambdas.append({"file":f,"topics":topics,"interval":interval,"runs":0,"errors":0,"function":function,"code":eval(function)})
    
    #logger.info(lambdas)

    for lamb in lambdas:
        for topic in lamb["topics"]:
            if topic in lambdasht:
                lambdasht[topic].append(lamb)
            else:
                lambdasht[topic]=[lamb]

################################################################################
def checkIfRequirementsChanged():
    global path,requirementstime

    curf=path+'/requirements.txt'
    prevf=path+'/requirements.txt.prev'
    if os.path.isfile('./firstrun.txt'):

        if not os.path.isfile(curf):
            return False
        
        if not os.path.isfile(prevf):
            copyfile(curf,prevf)
        
        elif requirementstime==0:
            
            with open(curf, 'r') as content_file:            
                contentcur = content_file.read()

            with open(prevf, 'r') as content_file:            
                contentprev = content_file.read()

            if contentcur==contentprev:
                requirementstime=os.path.getmtime(curf)
                return False

        elif os.path.getmtime(curf)==requirementstime:
            return False
    else:
        logger.info("FIRST RUN")
        f = open("firstrun.txt", "a")
        f.write("FIRST RUN")
        f.close()
    
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




################################################################################
def check_intervals_and_cron():
    global thread_check_intervals, lambdas,logs

    while thread_check_intervals:
        # logger.info('check_intervals_and_cron')
        
        for lamb in lambdas:
            # logger.info(lamb)

            if 'interval' in lamb and lamb['interval'] > 0:
                starttime=datetime.utcnow()
                
                if 'nextrun' not in lamb:
                    lamb['nextrun'] = starttime


                if lamb['nextrun'] <= starttime:

    
                    logger.info("==> "*10)
                    logger.info('RUN: '+ lamb['function'] +' - interval: '+str(lamb['interval']))
                    with intervalfn_lock:
                        logger.info("<== "*10)
                        logs=[]
                        logger_info(">>> Calling %s" %(lamb["function"]))
                        lamb['nextrun'] += timedelta(seconds=lamb['interval'])
                        lamb["lastrun"]=starttime.isoformat()

                        try:
                            ret=lamb["code"]()
                            lamb["return"]=str(ret)
                        except:
                            logger_error("Lambda "+lamb["function"]+" crashed.",exc_info=True)
                            tb = traceback.format_exc()
                            for trace in ["TRACE:"+lamb["function"]+":"+_ for _ in tb.split("\n")]:
                                logger_info(trace)
                            lamb["errors"]+=1
                            lamb["return"]="FAILURE"
                        finally:
                            lamb["logs"]=json.dumps(logs)
                            lamb["runs"]+=1
                            duration=(datetime.utcnow()-starttime).total_seconds()*1000
                            lamb["duration"]=duration
 
        time.sleep(1)


logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

lshandler=None
curconfig=""
lambdas=[]
lambdasht={}
path="./notebooks"
requirementstime=0

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
    conn=amqstompclient.AMQClient(server
        , {"name":MODULE,"version":VERSION,"lifesign":"/topic/NYX_MODULE_INFO","heartbeats":(120000,120000),"earlyack":True},QUEUE,callback=messageReceived)
    
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

    if thread_check_intervals== None:
        logger.info("Creating thread.")
        thread_check_intervals = threading.Thread(target = check_intervals_and_cron)
        thread_check_intervals.start()
       
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
                    thread_check_intervals = None
                    break
        except Exception as e:
            logger.error("Unable to check changed.",exc_info=True)
            logger.error(e)

        try:
            if(checkIfRequirementsChanged()):
                logger.info(">>>>>>> Requirements changed...QUITTING")
                conn.disconnect()
                thread_check_intervals = None
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
                            "source": "ctx._source.errors += params.errors;ctx._source.runs += params.runs;ctx._source.lastrun = params.lastrun;ctx._source.return = params.return;ctx._source.duration = params.duration;ctx._source.logs = params.logs;",
                            "lang": "painless",
                            "params" : {
                                "errors" : lamb["errors"],
                                "runs" : lamb["runs"],
                                "lastrun" : lamb["lastrun"],
                                "return" : lamb["return"],
                                "duration" : lamb["duration"],
                                "logs" : lamb["logs"]
                            }
                        }
                    }

                    upsert["upsert"]=copy.deepcopy(lamb)
                    if 'nextrun' in lamb:
                        upsert['script']['params']['nextrun'] = lamb['nextrun'].isoformat()
                        upsert['script']['source'] += "ctx._source.nextrun = params.nextrun;"

                        upsert["upsert"]['nextrun'] = lamb['nextrun'].isoformat()

                    lamb["errors"]=0
                    lamb["runs"]=0

                    # logger.info(upsert)


                    del upsert["upsert"]["code"]
                    if elkversion<=6:
                        action={ "update" : {"_id" :("R"+str(os.environ["RUNNER"])+"_"+lamb["function"]).lower(), "_index" : "nyx_lambda", "retry_on_conflict" : 1,"_type":"_doc"} }
                    else:
                        action={ "update" : {"_id" :("R"+str(os.environ["RUNNER"])+"_"+lamb["function"]).lower(), "_index" : "nyx_lambda", "retry_on_conflict" : 1} }
                    bulkbody+=json.dumps(action)+"\r\n"
                    bulkbody+=json.dumps(upsert)+"\r\n"
            if len(bulkbody)>0:
                # logger.info(bulkbody)
                res=es.bulk(bulkbody)
                # logger.info(res)


        except Exception as e:
            logger.error("Unable to update lambda stats.",exc_info=True)
            logger.error(e)



    #app.run(threaded=True,host= '0.0.0.0')