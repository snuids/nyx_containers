"""
NYX REPORT SCHEDULER MODULE
====================================
Trigger periodic jobs that are executed by the Report Runner Module.
Jobs are push in the queue /queue/NYX_REPORT_STEP1.

VERSION HISTORY
===============

* 19 Jun 2019 0.0.3 **AMA** Fix an UTC issue
* 09 Jul 2019 0.0.4 **AMA** Mail subjects and attachments can be customized
* 01 Apr 2020 1.0.1 **AMA** Use cron to compute next runs
* 12 May 2020 1.0.2 **AMA** Use amqstompclient 2.0.0
* 14 May 2020 1.0.3 **AMA** Fix issue with amqstompclient connection parameters
* 29 Jan 2021 1.0.4 **AMA** Fix issue with date parameters* 
* 03 Jan 2026 1.0.7 **AMA** Fix a report parameter issue
 
"""
import re
import json
import time
import uuid
import tzlocal
import platform
import traceback
import traceback
import os,logging


from crontab import CronTab
from datetime import timedelta
from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from dateutil import parser

VERSION="1.0.7"
MODULE="ReportScheduler"
QUEUE=[]

def resolveDateString(name,adate):
    """
    Resolves a string formatted as follows:  "Myfile-${DATE:%d%B%Y:now-30d}-biac"
    Into: Myfile-09July2019-biac
    """
    regex = r"(.*)\$\{DATE:([^}]*)\}(.*)"
    result = re.sub(regex, "\\2", name, 0, re.MULTILINE)

    if result:    
        parts=result.split(":")
        if len(parts)>1:
            result1=computeDefaultValue(adate,parts[1]).strftime(parts[0])
        else:
            result1=adate.strftime(result)
        print(result1)
        result=re.sub(regex, "\\1REPLACEMENT\\3", name, 0, re.MULTILINE)
        result=result.replace("REPLACEMENT",result1)
        
    
    return  result

def computeCronOfTask(taskin,curtime):
    triggertype=taskin["trigger"]["type"]
    if triggertype=="daily":
        triggertime=taskin["trigger"]["time"].split(":")
        days=[str((_+1)%7) for _ in taskin["trigger"]["days"]]
        days.sort()
        cron=str(int(triggertime[1]))+" "+str(int(triggertime[0]))+" * * "+",".join(days)
        
    if triggertype=="monthly":
        triggertime=taskin["trigger"]["time"].split(":")
        days=[str((_)) for _ in taskin["trigger"]["days"]]    
        days.sort()
        cron=str(int(triggertime[1]))+" "+str(int(triggertime[0]))+" "+",".join(days)+" * *"
        
    crontab=CronTab(cron)
    nextrun=curtime+timedelta(seconds=crontab.next(curtime,default_utc=True))
    
    return cron,nextrun


def execute_report(duetime,task):
    """
    Executes a task for the given time.
    
    Parameters
    ----------
    duetime
        The generation date that must be used by the report.
    message
        The task that describesthe job.        
    """
    logger.info(">>>>>>>>>>> Executing task..............")
    logger.info(task)
    try:
        report=es.get(doc_type="_doc",index="nyx_reportdef",id=task["report"])
    except:
        report=es.get(index="nyx_reportdef",id=task["report"])


    if "parameters" in task:
        report["_source"]["parameters"] = task["parameters"]
#    report["_source"]["parameters"] = task["_source"]["parameters"]
    logger.info(report)
    for parameter in report["_source"]["parameters"]:
        logger.info(parameter)

        if parameter["type"]=="interval":
            start=computeDefaultValue(duetime,parameter["value"].split(":")[0])
            end=computeDefaultValue(duetime,parameter["value"].split(":")[1])
            logger.info(start)
            logger.info(end)
            parameter["value"]=[start.isoformat(),end.isoformat()]

        if parameter["type"]=="date":
            start=computeDefaultValue(duetime,parameter["value"].split(":")[0])
            logger.info(start)
            parameter["value"]=start.isoformat()

    
    creds={"token":"reportscheduler",
      "user":{"firstname":"Report",
              "lastname":"Scheduler",
              "id":"ReportScheduler",
              "login":"ReportScheduler",
              "user":"ReportScheduler",
              "language":"en",
              "privileges": ["admin"]
              
             }}

    message={
            "id":"id_" + str(uuid.uuid4()),
            "creds":creds,
            "report":report["_source"],
            "privileges":["admin"],
            "task":task
    }

    if "attachmentName" in task:
        message["mailAttachmentName"]=resolveDateString(task["attachmentName"],duetime)
    if "mailSubject" in message:
        message["mailSubject"]=resolveDateString(task["mailSubject"],duetime)
    else:
        message["mailSubject"]="NA"
        
    logger.info(json.dumps(message))

    conn.send_message("/queue/NYX_REPORT_STEP1",json.dumps(message))


##############################################
# Resolve default parameter
##############################################

def computeDefaultValue(duetime,formula):
    """
    Computes a time formula based on the given date.

    Parameters
    ----------
    duetime
        The generation date that must be used by the formual.
    message
        The formulas as a string. Example: (now-1d)        
    """
    logger.info("ComputeDefaultValue Formula:"+formula)
    logger.info("ComputeDefaultValue Formula:"+str(duetime))
    duetime=int(duetime.timestamp())

    formula=formula.replace("now",str(duetime))

    regexh = r"([0-9]*)(m)"
    subst = "(\\1*60)"
    formula = re.sub(regexh, subst, formula, 10)

    regexh = r"([0-9]*)(h)"
    subst = "(\\1*3600)"
    formula = re.sub(regexh, subst, formula, 10)

    regexh = r"([0-9]*)(d)"
    subst = "(\\1*3600*24)"
    formula = re.sub(regexh, subst, formula, 10)

    logger.info(formula)
    
    finalval=eval(formula)
    exittime=datetime.fromtimestamp(finalval)
    logger.info(exittime)
    return exittime

################################################################################
def checkTasks():
    """
    Called periodically in order to check if a task must be executed.        
    """
    logger.info("Checking tasks....")
    docs=es.search(index="nyx_reportperiodic",size=10000)
    
    containertimezone=tzlocal.get_localzone()

    for task in docs["hits"]["hits"]:
        #logger.info("TASK-"*10)
        #logger.info(task)
        taskin=task["_source"]
        
        if("nextRun" in taskin):
            try:
                nextrun=parser.parse(taskin["nextRun"]).astimezone(containertimezone)#.replace(tzinfo=None) #.replace(tzinfo=pytz.timezone(tzlocal.get_localzone().zone))
                cron,nextrun2=computeCronOfTask(taskin,nextrun+timedelta(hours=1))

                if(nextrun<datetime.now().astimezone(containertimezone)):
                    logger.info(">>>>>>>>>>>>>>> NEXT RUN %s" %(nextrun))
                    logger.info("NEXT RUN2 %s" %(nextrun2))
                    logger.info("CRON %s" %(cron))

                    logger.info("===> Compute next run")
                    triggertype=taskin["trigger"]["type"]
                    
                    task["_source"]["nextRun"]=nextrun2.isoformat()
                    try:
                        resind=es.index(index=task["_index"],doc_type="_doc",id=task["_id"],body=task["_source"])
                    except:
                        resind=es.index(index=task["_index"],id=task["_id"],body=task["_source"])

                    execute_report(nextrun,task["_source"])


            except Exception as e:
                logger.error("Unable to process task")
                logger.error(e)        
                logger.error( traceback.format_exc())                    

    logger.info("Finished..........")

################################################################################
def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(message)
    status="Finished"
    errormessage=""
    messagejson=json.loads(message)

    
    logger.info("<== "*10)


#START
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

    #>> ELK
    es=None
    logger.info("ELK_URL          :"+os.environ["ELK_URL"])
    logger.info("ELK_PORT         :"+os.environ["ELK_PORT"])
    logger.info("ELK_LOGIN        :"+os.environ["ELK_LOGIN"])
    logger.info("ELK_PASSWORD     :********")   
    logger.info (os.environ["ELK_SSL"])

    if os.environ["ELK_SSL"]=="true":    
        # Support full URLs with subdomains and paths
        elk_url = os.environ["ELK_URL"]
        if elk_url.startswith('http://') or elk_url.startswith('https://'):
            host_params = elk_url
        else:
            host_params = "https://" + elk_url + ":" + os.environ["ELK_PORT"]
        logger.info("ELK Host Params:"+str(host_params))
        es = ES([host_params], connection_class=RC, http_auth=(os.environ["ELK_LOGIN"], os.environ["ELK_PASSWORD"]),  use_ssl=True ,verify_certs=False)
    else:
        elk_url = os.environ["ELK_URL"]
        if elk_url.startswith('http://') or elk_url.startswith('https://'):
            host_params = elk_url
        else:
            host_params = "http://" + elk_url + ":" + os.environ["ELK_PORT"]
        es = ES(hosts=[host_params])


    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])

    nextrun=datetime.now()

    SECONDSBETWEENCHECKS=10

    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"calendar-alt"}
            conn.send_life_sign(variables=variables)

            if (datetime.now() > nextrun):
                try:
                    nextrun=datetime.now()+timedelta(seconds=SECONDSBETWEENCHECKS)
                    checkTasks()
                except Exception as e2:
                    logger.error("Unable to load tasks.")
                    logger.error(e2)

        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')