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
* 11 Feb 2020 1.1.0  **AMA** Linked with elastic helper that destroys scroll ids
* 20 Feb 2020 1.2.7  **AMA** Better logs
* 21 Feb 2020 1.3.0  **AMA** Result_icon and type_icon added
* 24 Feb 2020 1.3.3  **AMA** Better compile error log
* 25 Feb 2020 1.3.4  **AMA** Success green icon changed to green
* 26 Feb 2020 1.3.23 **AMA** Input saved + lambda commands
* 02 Mar 2020 1.3.27 **AMA** Fixed outputs
* 03 Mar 2020 1.3.30 **AMA** Fixed restart issues
* 09 Mar 2020 1.3.31 **AMA** CRON keyword added
"""
import re
import json
import time
import uuid
import copy
import uuid
import redis
import base64
import platform
import threading
import traceback
import subprocess 
import os,logging
import humanfriendly
import dateutil.parser
from functools import wraps
from crontab import CronTab
from shutil import copyfile
from datetime import datetime
from datetime import timedelta
from datetime import datetime, timezone
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC


VERSION="1.3.31"
MODULE="NYX_Lambda"
QUEUE=["/topic/NYX_LAMBDA_COMMAND"]

global_message=""
global_destination=""
global_headers=""
global_icon="question"

thread_check_intervals = None
intervalfn_lock = threading.RLock()

elkversion=6
logs=[]

################################################################################
def getELKVersion(es):
    return int(es.info().get('version').get('number').split('.')[0])

################################################################################
def createNotebook(nbname,functionname,nbtype,nbparameter):
    logger.info("Create Notebook=%s Function=%s Type=%s Parameter=%s" %(nbname,functionname,nbtype,nbparameter))

    filepath=path+"/"+nbname

    update=os.path.exists(filepath)

    newnb={}
    if update:
        with open(filepath, 'r') as content_file:
            content = content_file.read()
            newnb=json.loads(content)
        
    if not update:
        newnb={"metadata": {
                    "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                    },
                    "language_info": {
                    "codemirror_mode": {
                        "name": "ipython",
                        "version": 3
                    },
                    "file_extension": ".py",
                    "mimetype": "text/x-python",
                    "name": "python",
                    "nbconvert_exporter": "python",
                    "pygments_lexer": "ipython3",
                    "version": "3.7.3"
                    }
                },
                "nbformat": 4,
                "nbformat_minor": 2
            }



        header=["## "+nbname+"\n"
                ,"Common code. (**#@COMMON** tag) All functions defined includes the cells marked with this tag.\n"
            ,"- Use logger.info/warning/error() to log in the lambda.\n"]


        newnb["cells"]=[{
        "cell_type": "markdown",
        "metadata": {},    
        "source": header
        }]

        headersource=[
            "#@COMMON\n",
            "\n",    
            "import math\n"]


        newnb["cells"].append({
        "cell_type": "code",
        "metadata": {},
        "outputs": [],
        "execution_count": None,
        "source": headersource
        })


    if nbtype=="message":
        headerfunction=["## Functions\n"
                        ,"Event based function(**#@LISTEN=/TYPE/DESTINATION** tag) Where **TYPE** is queue or topic and **DESTINATION** is the broker destination.\n "
                        ,"Multi destinations can be separated by a comma.\n "
                        ,"- Use es to access elastic search.\n"
                        ,"- Add a return value to the functions for monitoring.\n"
                        ,"- Use the global_icon variable to set a succcess icon. Format: ICON>COLOR example: **vial>red**\n"]


        newnb["cells"].append({
           "cell_type": "markdown",
           "metadata": {},           
           "source": headerfunction
          })

        headerfunctionsource=[
            "#@LISTEN="+nbparameter+"\n",
            "\n",    
            "def "+functionname+"():\n",
            "    global es                 # ELASTIC \n",
            "    global global_message     # The message body \n",
            "    global global_headers     # The message headers \n",
            "    global global_icon        # The result icon \n",            
            "\n",    
            "    global_icon=\"bug>orange\"\n",
            "    logger.info(\"Hello\")\n",
            "\n",    
            "    return \"Hello\"\n"

        ]


        newnb["cells"].append({
           "cell_type": "code",
           "metadata": {},
           "outputs": [],
           "execution_count": None,
           "source": headerfunctionsource
          })
        
    elif nbtype=="cron":
        headerfunction=["## Functions\n"
                        ,"Event based function(**#@INTERVAL=DURATION+UNIT** tag) Where **DURATION** is number or topic and **UNIT** is one of the following.\n"
                        ,"- **s** for seconds.\n"
                        ,"- **m** for minutes.\n"                        
                        ,"- **h** for hours.\n"   
                        ,"\n"   
                        ,"Example: 5s for every 5 seconds\n"                        
                        ,"## Parameters\n"
                        ,"- Use es to access elastic search.\n"
                        ,"- Add a return value to the functions for monitoring.\n"
                        ,"- Use the global_icon variable to set a succcess icon. Format: ICON>COLOR example: **vial>red**\n"]


        newnb["cells"].append({
           "cell_type": "markdown",
           "metadata": {},           
           "source": headerfunction
          })

        headerfunctionsource=[
            "#@INTERVAL="+nbparameter+"\n",
            "\n",    
            "def "+functionname+"():\n",
            "    global es                 # ELASTIC \n",
            "    global global_message     # The message body \n",
            "    global global_headers     # The message headers \n",
            "    global global_icon        # The result icon \n",
            "\n",    
            "    global_icon=\"bug>orange\"\n",
            "    logger.info(\"Hello\")\n",
            "\n",    
            "    return \"Hello\"\n"

        ]


        newnb["cells"].append({
           "cell_type": "code",
           "metadata": {},
           "outputs": [],
           "execution_count": None,
           "source": headerfunctionsource
          })
        
    try:
        os.remove(filepath)
    except:
        logger.info("Unable to remove file...")

    file = open(filepath,"w") 
    file.write(json.dumps(newnb)) 
    file.close()
    os.chmod(filepath,0o666)

################################################################################

def handleCommand(message):
    logger.info("Handling message:"+message)
    mobj=json.loads(message)

    if str(mobj["runner"])!=str(os.environ["RUNNER"]):
        logger.info("Ignoring command.")
    else:
        if mobj["action"]=="add":
            createNotebook(mobj["notebook"],mobj["function"],mobj["type"],mobj["parameters"][0])
        

################################################################################
def messageReceived(destination,message,headers):
    global es,global_message,global_destination,global_headers,logs,global_icon

    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    
    if destination=="/topic/NYX_LAMBDA_COMMAND":
        return handleCommand(message)

    with intervalfn_lock:
        
        logger.info("<== "*10)

        global_message=message
        global_destination=destination
        global_headers=headers

        if destination in lambdasht:
            for lamb in lambdasht[destination]:
                logs=[]
                logger_info(">>> Calling %s" %(lamb["function"]))
                lamb["lastrun"]=datetime.now(timezone.utc).isoformat()

                lamb["crashed"]=0
                lamb["type_icon"]="regular/envelope"
                lamb["type"]="message"
                global_icon="check>green"

                starttime=datetime.now(timezone.utc)
                
                try:
                    if isinstance(lamb["code"] ,str):
                        for trace in [_ for _ in lamb["tb"].split("\n")]:
                            logger_error(trace)

                        logger_info(">"*30)
                        for code in lamb["code"].split("\n"):
                            logger_info(code)
                        lamb["return"]="COMPILE ERROR"
                        lamb["crashed"]=1
                        global_icon="code>red"

                    else:
                        ret=lamb["code"]()
                        lamb["return"]=str(ret)
#                    ret=lamb["code"]()
#                    lamb["return"]=str(ret)
                    
                except:
                    logger_error("Lambda "+lamb["function"]+" crashed.",exc_info=True)
                    tb = traceback.format_exc()
                    for trace in ["TRACE:"+lamb["function"]+":"+_ for _ in tb.split("\n")]:
                        logger_info(trace)

                    lamb["errors"]+=1
                    lamb["return"]="FAILURE"
                    lamb["crashed"]=1
                    global_icon="times>red"
                finally:
                    #lamb["logs"]=json.dumps(logs)
                    lamb["runs"]+=1
                    duration=(datetime.now(timezone.utc)-starttime).total_seconds()*1000
                    lamb["duration"]=round(duration,2)
                    lamb["result_icon"]=global_icon

                l_uuid=str(uuid.uuid1())
                save_log(logs,l_uuid,lamb['function'],str(os.environ["RUNNER"]),lamb,message=message,headers=headers)
                lamb["log_uuid"]=l_uuid



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

#######################################################################################
# save_log
#######################################################################################
def save_log(logs,guid,lambdaname,runner,lamb,message=None,headers=None):
    logger.info("Saving log")
    body={"@timestamp":datetime.utcnow().isoformat(),"logs":logs,"uuid":guid
                ,"orgfunction":lamb["orgfunction"],"function":lambdaname,"runner":runner,"errors":lamb["errors"]
                ,"return":lamb["return"],"runs":lamb["runs"],"duration":lamb["duration"]
                ,"crashed":lamb["crashed"]}

    if message!=None:
        body["message"]=message[0:512]

        jsonheaders=json.dumps(headers)
        
        file = open(path+"/inputs/"+guid+".txt","w") 
        file.write(jsonheaders+"\r\n") 
        file.write(message) 
        file.close() 
  
        body["headers"]=jsonheaders
        body["inputuuid"]=guid

        logger.info("Delete files older than 3 days.")
        resdelete=os.system("find "+path+"/inputs/"+" -mtime +3 -delete")
        logger.info("Res="+str(resdelete))

    if elkversion <= 6:
        es.index("nyx_lambdalog", id = guid, doc_type = "doc", body = body)
    else:
        es.index("nyx_lambdalog", id = guid, body = body)

    logger.info("Saved")


################################################################################
def computeNewCode(notebook,newcode,common):
    allowedchards=("".join([chr(_) for _ in range(65,65+26)])+"".join([chr(_) for _ in range(48,58)])).lower()
    notebook="".join([_ for _  in notebook.lower() if _ in allowedchards])

    newfunc="UNABLETOFINDFUNC"
    newcode2="NOFUNC"

    if len(common)>0:
        common2="".join(["    "+_+"\n" for _ in common.split("\n")])
    else:
        common2=""

    startdef=newcode.find("\ndef ")
    if startdef>=0:
        enddef=newcode.find("):",startdef)

        if enddef>=0:
            newfunc=(notebook+"_"+newcode[startdef+5:enddef+3]).split("(")[0]
            newcode2=newcode[0:startdef+5]+notebook+"_"+newcode[startdef+5:enddef+3]+common2+newcode[enddef+3:]

    
    return newcode2.replace("logger.","logger_"),newfunc

################################################################################
def loadConfig():
    global lambdas
    global lambdasht
    global path
    global es

    lambdas=[]
    lambdasht={}
    files=[]
    common={}
    
    regex_fn = r'(^ *)def (\w+)\(.*?\):'

    for r, d, f in os.walk(path):
        for file in f:
            if file.endswith('.ipynb') and not 'checkpoint' in file:
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
                            if f not in common:
                                common[f]=""
                            common[f]+="".join(cell["source"])

# LOAD FUNCTIONS
    for f in files:
        with open(f, 'r') as content_file:
            content = content_file.read()
            jsoncontent=json.loads(content)
            for cell in jsoncontent["cells"]:

                if cell["cell_type"]=="code":
                    cell['source'] = [_ for _ in cell['source'] if _ != '\n']                    
                    interval = 0
                    crontab=None
                    topics = []

                    
                    if len(cell['source']) > 1:
                        if "#@LISTEN=" in  cell['source'][0]:
                            topics = cell["source"][0][9:].strip().split(",")

                        elif "#@CRON=" in  cell['source'][0]:
                            logger.info("Decoding CRON:<"+cell["source"][0][7:].strip()+">")
                            try:   

                                crontab=CronTab(cell["source"][0][7:].strip())
                            except:                                
                                logger.error("Unable to decode cron.")
                                logger.info(cell["source"][0])
                                continue

                            pass

                        elif "#@INTERVAL=" in  cell['source'][0]:
                            try:                            
                                interval = humanfriendly.parse_timespan(cell["source"][0][11:].strip())
                            except:                                
                                logger.error("Unable to decode interval.")
                                logger.info(cell["source"][0])
                                continue                                
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
                            newcode,realfunction=computeNewCode(f,newcode,common.get(f,""))
                            try:
                                exec(newcode)
                                lambdas.append({"runner":str(os.environ["RUNNER"]),"file":f.replace(path+"/",""),"topics":topics,"crontab":crontab,"interval":interval,"runs":0,"errors":0,"orgfunction":function
                                                ,"function":realfunction,"code":eval(realfunction)})
                            except:
                                tb = traceback.format_exc()
                                for trace in ["TRACE:"+function+":"+_ for _ in tb.split("\n")]:
                                    logger_info(trace)
                                logger.error("CODE:"+newcode)
                                lambdas.append({"runner":str(os.environ["RUNNER"]),"file":f.replace(path+"/",""),"topics":topics,"interval":interval,"runs":0,"errors":0,"function":function
                                                ,"code":newcode,"tb":tb})

                            
    
    #logger.info(lambdas)

    for lamb in lambdas:
        for topic in lamb["topics"]:
            if topic in lambdasht:
                lambdasht[topic].append(lamb)
            else:
                lambdasht[topic]=[lamb]

    #action={ "update" : {"_id" :("R"+str(os.environ["RUNNER"])+"_"+lamb["function"]).lower(), "_index" : "nyx_lambda", "retry_on_conflict" : 1} } 

    for lamb in lambdas:
        nlamb=copy.copy(lamb)
        del nlamb["code"]

        if "topics" in lamb and len(lamb["topics"])>0:
            nlamb["type_icon"]="regular/envelope"
            nlamb["type"]="message"
        else:                                      
            nlamb["type_icon"]="regular/clock"      
            nlamb["type"]="cron"
        nlamb["result_icon"]="question>orange"

        myid=("R"+str(os.environ["RUNNER"])+"_"+lamb["function"]).lower()    
        try:
            if elkversion<=6:
                es.index(index="nyx_lambda",id=myid,doc_type="_doc",body=json.dumps(nlamb),op_type="create")
            else:
                es.index(index="nyx_lambda",id=myid,body=json.dumps(nlamb),op_type="create")
        except:
            logger.info("Record exists.")

    # RESET NEXT RUN FROM REDIS
    
    for lamb in lambdas:

        if lamb["file"] in lambdaschanged:
            logger.info("Function changed %s, do not restore next run." %(lamb["function"]))
        else: # restore time                         
            redtime=redisserver.get("lambda_"+str(os.environ["RUNNER"])+"_"+lamb["function"]+"_nextrun")
            if 'crontab' in lamb and lamb['crontab']!=None:
                logger.info("Ignoring crontab.")
            elif redtime!=None:                
                lamb["nextrun"]=dateutil.parser.parse(redtime.decode("ascii"))
                if lamb["nextrun"]<datetime.now(timezone.utc):
                    lamb["nextrun"]=datetime.now(timezone.utc)

                logger.info("Restoring next run to:%s" %(lamb["nextrun"]))

    pass

            


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
    global curconfig,path,lambdaschanged
    
    fileasstr=""
    lambdaschanged={}
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.ipynb' in file and not 'checkpoint' in file and not '.swp' in file:
                cleanp=os.path.join(r, file).replace("/","_").replace(".","_")

                old=redisserver.get("file_"+cleanp)
                curt=str(os.path.getmtime(os.path.join(r, file)))
                fileasstr+=os.path.join(r, file)+","+curt
                if old!=None and old.decode("ascii") != curt:
                    logger.info(">>>>>>>>>> File %s changed." %file)
                    lambdaschanged[file]=True
                redisserver.set("file_"+cleanp,curt,86400*30)

    if fileasstr==curconfig:
        return False

    curconfig=fileasstr
    return True




################################################################################
def check_intervals_and_cron():
    global thread_check_intervals, lambdas,logs,global_icon

    while thread_check_intervals:
        # logger.info('check_intervals_and_cron')
        
        for lamb in lambdas:
            # logger.info(lamb)

            if ('interval' in lamb and lamb['interval'] > 0) or ('crontab' in lamb and lamb['crontab']!=None):
                starttime=datetime.now(timezone.utc)
                
                if 'nextrun' not in lamb:

                    if 'crontab' in lamb and lamb['crontab']!=None:
                        lamb['nextrun'] = datetime.now(timezone.utc)+timedelta(seconds=lamb['crontab'].next(default_utc=True))
                    else:
                        lamb['nextrun'] = starttime


                if lamb['nextrun'] <= starttime:

    
                    logger.info("==> "*10)
                    logger.info('RUN: '+ lamb['function'] +' - interval: '+str(lamb['interval']))
                    with intervalfn_lock:
                        logger.info("<== "*10)
                        logs=[]
                        logger_info(">>> Calling %s" %(lamb["function"]))
                        if 'crontab' in lamb and lamb['crontab']!=None:
                            lamb['nextrun'] = datetime.now(timezone.utc)+timedelta(seconds=lamb['crontab'].next(default_utc=True))
                        else:
                            lamb['nextrun'] += timedelta(seconds=lamb['interval'])
                            redisserver.set("lambda_"+str(os.environ["RUNNER"])+"_"+lamb["function"]+"_nextrun",lamb['nextrun'].isoformat(),86400)

                        lamb["lastrun"]=starttime.isoformat()

                        lamb["crashed"]=0
                        lamb["type_icon"]="regular/clock"
                        lamb["type"]="cron"

                        global_icon="check>green"

                        try:
                            if isinstance(lamb["code"] ,str):
                                for trace in [_ for _ in lamb["tb"].split("\n")]:
                                    logger_error(trace)

                                logger_info(">"*30)
                                for code in lamb["code"].split("\n"):
                                    logger_info(code)
                                lamb["return"]="COMPILE ERROR"
                                lamb["crashed"]=1
                                global_icon="code>red"

                            else:
                                ret=lamb["code"]()
                                lamb["return"]=str(ret)
                        except:
                            logger_error("Lambda "+lamb["function"]+" crashed.",exc_info=True)
                            tb = traceback.format_exc()
                            for trace in ["TRACE:"+lamb["function"]+":"+_ for _ in tb.split("\n")]:
                                logger_info(trace)
                            lamb["errors"]+=1
                            lamb["return"]="FAILURE"
                            lamb["crashed"]=1
                            global_icon="times>red"
                        finally:
                            lamb["logs"]=json.dumps(logs)
                            lamb["runs"]+=1
                            duration=(datetime.now(timezone.utc)-starttime).total_seconds()*1000
                            lamb["duration"]=round(duration,2)
                        
                        l_uuid=str(uuid.uuid1())
                        lamb["log_uuid"]=l_uuid
                        lamb["result_icon"]=global_icon
                        save_log(logs,l_uuid,lamb['function'],str(os.environ["RUNNER"]),lamb)
                        

                        
 
        time.sleep(1)


logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

lshandler=None
curconfig=""
lambdas=[]
lambdasht={}
lambdaschanged={}
path="./notebooks"
requirementstime=0

es=None
redisserver=None

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

    logger.info("Starting redis connection")
    logger.info("IP=>"+os.environ["REDIS_IP"]+"<")
    redisserver = redis.Redis(host=os.environ["REDIS_IP"], port=6379, db=0)


    try:
        os.mkdir( path+"/inputs")
    except:
        logger.info("/inputs already exists")

    #>> ELK
    logger.info (os.environ["ELK_SSL"])

    if os.environ["ELK_SSL"]=="true":
        host_params = {'host':os.environ["ELK_URL"], 'port':int(os.environ["ELK_PORT"]), 'use_ssl':True}
        es = ES([host_params], connection_class=RC, http_auth=(os.environ["ELK_LOGIN"], os.environ["ELK_PASSWORD"]),  use_ssl=True ,verify_certs=False)
    else:
        host_params="http://"+os.environ["ELK_URL"]+":"+os.environ["ELK_PORT"]
        es = ES(hosts=[host_params])

    elkversion=getELKVersion(es)


    checkIfChanged(curconfig)
    loadConfig()
    for topic in lambdasht:
        QUEUE.append(topic)

    QUEUE.sort()
    

    #>> AMQC
    server={"ip":os.environ["AMQC_URL"],"port":os.environ["AMQC_PORT"]
                    ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"]}
    conn=amqstompclient.AMQClient(server
        , {"name":MODULE,"version":VERSION,"lifesign":"/topic/NYX_MODULE_INFO","heartbeats":(120000,120000),"earlyack":True},QUEUE,callback=messageReceived)
    
    connectionparameters={"conn":conn}


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
            with intervalfn_lock:
                if(checkIfChanged(curconfig)):
                    logger.info(">>>>> Config changed")
                    logger.info(curconfig)
                    loadConfig()
                    q=[_ for _ in lambdasht]+["/topic/NYX_LAMBDA_COMMAND"]
                    q.sort()
                    newqueues=",".join(q)
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
                    if not "log_uuid" in lamb:
                        lamb["log_uuid"]="NA"

                    if not "type_icon" in lamb:
                        lamb["type_icon"]="question"                        

                    if not "result_icon" in lamb:
                        lamb["result_icon"]="question"

                    upsert={
                        "script" : {
                            "source": "ctx._source.errors += params.errors;ctx._source.runs += params.runs;ctx._source.lastrun = params.lastrun;ctx._source.return = params.return;ctx._source.duration = params.duration;ctx._source.log_uuid = params.log_uuid;ctx._source.type_icon = params.type_icon;ctx._source.result_icon = params.result_icon;",
                            "lang": "painless",
                            "params" : {                                
                                "errors" : lamb["errors"],
                                "runs" : lamb["runs"],
                                "lastrun" : lamb["lastrun"],
                                "return" : lamb["return"],
                                "duration" : lamb["duration"],
                                "log_uuid": lamb["log_uuid"],
                                "type_icon": lamb["type_icon"],
                                "result_icon": lamb["result_icon"]
                            }
                        }
                    }

                    upsert["upsert"]=copy.deepcopy(lamb)
                    upsert["upsert"]["crontab"]=None
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