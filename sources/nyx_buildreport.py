
import os
import re
import sys
import json
import pytz
import logging
import tzlocal
import datetime
import traceback
import cachetools
import matplotlib
import collections
import numpy as np
import pandas as pd
matplotlib.use('TkAgg')
from docx import Document
from datetime import timezone
from datetime import datetime
from docx.shared import Inches
import matplotlib.pyplot as plt
from dateutil.parser import parse
from elastic_helper import es_helper
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#######################################################################################
# save_log
#######################################################################################
def save_log():
    logger.info("Saving log")
    body={"@timestamp":datetime.now().isoformat(),"logs":logs}
    es.index("nyx_reportlog",id=report["id"],doc_type="doc",body=body)
    logger.info("Saved")


#######################################################################################
# move_table_after
#######################################################################################
def move_table_after(table, paragraph):
    tbl, p = table._tbl, paragraph._p
    p.addnext(tbl)

#######################################################################################
# create_table
#######################################################################################
def create_table( paragraph,df, style=None,title=""):
    global template
    table = template.add_table(df.shape[0]+1, df.shape[1])

    if style:
        table.style = style
        
    table_cells = table._cells

    for i in range(0,df.shape[0]+1):
        row_cells = table_cells[i*df.shape[1]:(i+1)*df.shape[1]]

        if i==0:

            for idx, col in enumerate(df.columns):
                row_cells[idx].text = col
        else:
            for idx, col in enumerate(df.columns):
                row_cells[idx].text = str(df.loc[(int(i-1)), col])


    table.autofit = True
    paragraph.text=title
    move_table_after(table, paragraph)

    return table

#######################################################################################
# replaceText
#######################################################################################
def replaceText(replacementHT,text):
    for keypair in replacementHT:
        try:
            text=text.replace("${"+keypair+"}",str(replacementHT[keypair])).replace('.0 %',' %')
        except:
            pass
    return text

#######################################################################################
# Start
#######################################################################################

print("Starting v0.2")
print(datetime.now(timezone.utc).isoformat())
localmode=False
try:
    os.environ["LOCAL_MODE"]
    localmode=True
except:
    pass

try:
    report=json.loads(sys.argv[1])
except:
    report={"id": "id_19485666_27963523", "creds": {"token": "933921e1-58ae-4948-a941-037e9ce2e915", "user": {"filters": [], "firstname": "Arnaud", "id": "amarchand@icloud.com", "language": "en", "lastname": "Marchand", "login": "amarchand", "password": "", "phone": "0033497441962", "privileges": ["admin"], "user": "amarchand@icloud.com"}}, "report": {"description": "My First Report", "exec": "report1", "generatePDF": True, "icon": "file", "output": ["txt", "pdf"], "parameters": [{"name": "param1", "title": "Param1", "type": "text", "value": "test"}, {"name": "param2", "title": "Param2", "type": "interval", "value": ["2019-11-20T23:00:00.000Z", "2019-11-21T23:00:00.000Z"]}], "privileges": [], "reportType": "python", "title": "My First Report"}, "privileges": ["admin"], "treatment": {"status": "Waiting", "creation": "2019-11-22T08:24:03.288Z", "start": "2019-11-22T09:24:03.293651"}, "@timestamp": "2019-11-22T08:24:03.288Z", "output": "/opt/sources/generated/id_19485666_27963523"}


logs=[]
print("==== Args")
print(report)
print("==== Args")
print(report["report"]["parameters"])

params={}

containertimezone=pytz.timezone(tzlocal.get_localzone().zone)

for param in report["report"]["parameters"]:
    if param["type"]=="interval":
        params[param["name"]+"_start"]=parse(param["value"][0]).astimezone(containertimezone)
        params[param["name"]+"_end"]=parse(param["value"][1]).astimezone(containertimezone)
    elif param["type"]=="date":
        params[param["name"]]=parse(param["value"]).astimezone(containertimezone)
    else:
        params[param["name"]]=param["value"]

prepath = "./reports/notebooks/"

# ELASTIC SEARCH

if not localmode:
    host_params="http://elasticsearch:9200"
    es = ES(hosts=[host_params])
    print(es.info())
else:        
    print("Connect to elastic via password")
    host_params = {'host':os.environ["ELK_URL"], 'port':int(os.environ["ELK_PORT"]), 'use_ssl':True}
    
    es = ES([host_params], connection_class=RC, http_auth=(os.environ["ELK_LOGIN"], os.environ["ELK_PASSWORD"]),  use_ssl=True ,verify_certs=False)
    print(es.info())

ipynbpath=prepath+report["report"]["notebook"]+".ipynb"


# PYTHON
if os.path.isfile(ipynbpath) :
    print("IPYNB (%s) found." %(ipynbpath))
else:
    print("ERROR IPYNB (%s) found." %(ipynbpath))
    sys.exit(1) 



replacementHT={}
replacementHT.update(params)

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
def computeNewCode(newcode):
    return newcode.replace("logger.","logger_")

reportfunctions={}

with open(ipynbpath, 'r') as content_file:
    content = content_file.read()
    jsoncontent=json.loads(content)
    for cell in jsoncontent["cells"]:
        if cell["cell_type"]=="code":
            if len(cell["source"])>0 and "#@ONLOAD" in cell["source"][0]:
                newcode="".join(cell["source"])
                try:
                    exec(computeNewCode(newcode))
                except:
                    logger_error("Common part crashed.",exc_info=True)
                    tb = traceback.format_exc()
                    for trace in ["TRACE:"+_ for _ in tb.split("\n")]:
                        logger_info(trace)

                        if "<string>"  in trace and "<module>" in trace:
                           try:
                                linenbr=int(trace.split("line ")[1].split(",")[0])
                                linecodes=newcode.split("\n")
                                for i in range(linenbr-5,linenbr+4):
                                    if i>=0 and i<len(linecodes):
                                        if i==linenbr-1:
                                          logger_error(">>> CODE:"+linecodes[i])
                                        else:
                                          logger_info(">>> CODE:"+linecodes[i])

                           except e:
                                logger_error("ERROR while decoding error",exc_info=True)

                    save_log()
                    raise Exception("ERROR COMMON")
            if len(cell["source"])>0 and "#@PARAGRAPH=" in cell["source"][0]:
                newcode="".join(cell["source"])
                reportfunctions["${"+cell["source"][0].replace("#@PARAGRAPH=","").strip()+"}"]=computeNewCode(newcode)
            


save_log()
