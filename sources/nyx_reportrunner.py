"""
NYX REPORT RUNNER MODULE
====================================
Generates reports and store them in a local directory.
The jobs are received via the NYX_REPORT_STEP2 queue from the node red workflow.
Jobs sent by the report scheduler are forwarded to /queue/NYX_REPORT_STEP3 and captured by the report scheduler workflow of node red.
PDE

Sends:
-------------------------------------

* /queue/NYX_REPORT_STEP3

Listens to:
-------------------------------------

* /queue/NYX_REPORT_STEP2

VERSION HISTORY
===============

* 06 Aug 2019 1.1.0 **AMA** Build with version 1.1.0.0 of opendistro JDBC
* 03 Sep 2019 1.2.0 **AMA** elastic-helper dependency added
* 22 Nov 2019 1.3.1 **AMA** Compatible with ES 7. Added notebook reporting.
* 28 Nov 2019 1.4.0 **AMA** Packaged with libre office 6.3
"""
import json
import time
import uuid
import base64
import threading
import subprocess 
import traceback
import os,logging,sys


from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler


VERSION="1.4.0"
QUEUE=["/queue/NYX_REPORT_STEP2"]

################################################################################
def messageReceived(destination,message,headers):
    """
    Generates a report and store it locally on the drive. There are two types of report:

    * **JASPER**

    The report is generated via a jasper creator java executable. Parameters are pushed to the report.

    * **PYTHON**    

    The report is generated via a python command line.

    Parameters
    ----------
    destination
        The incoming message queue or topic.
    message
        A json formatted text that represent the job.        
    headers
        The message headers.                
    """

    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(message)
    status="Finished"
    errormessage=""
    messagejson=json.loads(message)

    messagejson["treatment"]["start"]=datetime.now().isoformat()
    starttime=datetime.now()
    messagejson["report"]=json.loads(messagejson["report"])
    cwd=os.getcwd()

    messagejson["output"]=cwd+"/generated/"+messagejson["id"]

    logger.info("===-"*10)
    logger.info(messagejson)
    
    reporttype=messagejson["report"].get("reportType","python")

    logger.info("Report Type:"+reporttype)

    if reporttype=="jasper":
        if "jasper" not in messagejson["report"]:
            status="Error"
            errormessage="Jasper not defined"
        else:
            jasper=messagejson["report"]["jasper"]
            logger.info("Checkin path:"+jasper)
            if not os.path.exists(jasper):
                logger.error("Jasper file "+jasper+" does not exist.")
                status="Error"
                errormessage="Jasper file "+jasper+" does not exist."
    else:
        if "exec" not in messagejson["report"]:
            status="Error"
            errormessage="exec not defined"
        elif reporttype=="notebook":
            word="./reports/notebooks/"+messagejson["report"]["notebook"]+".docx"
            logger.info("Checkin path:"+word)
            if not os.path.exists(word):
                logger.error("Doc file "+word+" does not exist.")
                status="Error"
                errormessage="Doc file "+word+" does not exist."
            noteb="./reports/notebooks/"+messagejson["report"]["notebook"]+".ipynb"
            logger.info("Checkin path:"+noteb)
            if not os.path.exists(noteb):
                logger.error("Notebook file "+noteb+" does not exist.")
                status="Error"
                errormessage="Notebook file "+noteb+" does not exist."
        else:
            exec=messagejson["report"]["exec"]
            logger.info("Checkin path:"+exec)
            if not os.path.exists(exec):
                logger.error("Python file "+exec+" does not exist.")
                status="Error"
                errormessage="Python file "+exec+" does not exist."

    if errormessage=="":        
        

        if reporttype=="jasper":
            path='/'.join(jasper.split('/')[0:-1])
            logger.info("PATH="+path)

            todo_params=[]
            for param in messagejson["report"]["parameters"]:   
                todo_params.append(param["name"]+"="+param["value"])
            

            logger.info("Preparing Jasper TODO...")
            todo_exports=""

            for export in messagejson["report"]["output"]:   
                logger.info(export)
                todo_exports+=export.upper()+","

            todo_exports=todo_exports.strip(",")

            todo="Jasper=../"+messagejson["report"]["jasper"]+"\r\n"
            todo+="Parameters="+"&".join(todo_params)+"\r\n"
            todo+="Export="+todo_exports+"\r\n"
            todo+="DataSource="+os.environ["JDBC_DS"]+"\r\n"
            todo+="Driver=com.amazon.opendistroforelasticsearch.jdbc.Driver\r\n"
            todo+="DBUser="+os.environ["ELK_LOGIN"]+"\r\n"
            todo+="DBPassword="+os.environ["ELK_PASSWORD"]+"\r\n"
            todo+="Output="+messagejson["output"]+"\r\n"

            jobtodopath="./jaspergenerator/job2todo"+os.environ["RUNNER"]
            logger.info(jobtodopath)
            f = open(jobtodopath+".txt",'w')
            f.write(todo)
            f.close()

            ret=subprocess.Popen(["java","-jar","JasperReportGenerator.jar","job2todo"+os.environ["RUNNER"]+".txt"],cwd="./jaspergenerator", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            stdout = ret.communicate()[0]
            for line in stdout.decode("utf-8").split('\n'):
                logger.info(line)
            logger.info("Java Return Code:")
            logger.info(ret.returncode)
        elif reporttype=="notebook":            
            path="."
            logger.info("PATH="+path)
            ret=subprocess.Popen(["python3","nyx_buildreport.py",json.dumps(messagejson)],cwd=path, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            #ret=subprocess.Popen([exec.split('/')[-1:][0]],cwd=path, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            stdout = ret.communicate()[0]
            for line in stdout.decode("utf-8").split('\n'):
                logger.info(line)
            logger.info("Shell Return Code:")
            logger.info(ret.returncode)
            if ret.returncode !=0:
                status="Error"
                errormessage="Python file "+"nyx_buildreport.py"+" crashed. Return code="+str(ret.returncode)
        else:
            path='/'.join(exec.split('/')[0:-1])
            logger.info("PATH="+path)
            ret=subprocess.Popen(["python3",exec.split('/')[-1:][0],json.dumps(messagejson)],cwd=path, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            #ret=subprocess.Popen([exec.split('/')[-1:][0]],cwd=path, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            stdout = ret.communicate()[0]
            for line in stdout.decode("utf-8").split('\n'):
                logger.info(line)
            logger.info("Shell Return Code:")
            logger.info(ret.returncode)
            if ret.returncode !=0:
                status="Error"
                errormessage="Python file "+exec+" crashed. Return code="+str(ret.returncode) 

        webs=[]
        if errormessage=="":
            if "generatePDF" in messagejson["report"] and messagejson["report"]["generatePDF"]:
                logger.info("Converting outputs to PDF.")
                for output in messagejson["report"]["output"]:   
                    if output=="pdf":
                        continue

                    p2convert=messagejson["output"]+"."+output
                    p2convertoutput="/".join(p2convert.split("/")[0:-1])
                    logger.info("Converting:"+p2convert)
                    subprocess.run(["./converttopdsf.sh",p2convert,p2convertoutput],cwd="./shellscripts")
                    logger.info("PDF done")

            for output in messagejson["report"]["output"]:
                
                p2check=messagejson["output"]+"."+output
                logger.info("Check:"+p2check)
                if not os.path.exists(p2check):
                    logger.error("Output file "+p2check+" does not exist. Check the runner logs.")
                    status="Error"
                    errormessage="Output file "+p2check+" does not exist. Check the runner logs."
                    break
                
                webs.append({"extension":output,"url":os.environ["REPORT_URL"]+"/"+messagejson["id"]+"."+output})
            
        messagejson["downloads"]=webs

    



    #messagejson["report"]=json.dumps(messagejson["report"])
    messagejson["treatment"]["end"]=datetime.now().isoformat()
    messagejson["treatment"]["duration"]=(datetime.now()-starttime).total_seconds()
    messagejson["treatment"]["status"]=status
    messagejson["treatment"]["error"]=errormessage
    logger.info(json.dumps(messagejson))
    conn.send_message("/queue/NYX_REPORT_STEP3",json.dumps(messagejson))
    logger.info("<== "*10)

if __name__ == '__main__':    
    MODULE="ReportRunner_"+os.environ["RUNNER"]
        
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