import sys
import json
import time
import logging
import datetime
import traceback
import collections
import pandas as pd
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC

logger=logging.getLogger()

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
            
        elif isinstance(o, datetime.time):
            return o.isoformat()

#        elif isinstance(o, dttime):
#            return o.isoformat()

        return json.JSONEncoder.default(self, o)

def pandas_to_elastic(es, df):
    logger = logging.getLogger(__name__)

    logger.info("LOADING DATA FRAME")
    logger.info("==================")
    starttime = time.time()

    if len([item for item, count in collections.Counter(df.columns).items() if count > 1])>0:

        logger.error("NNOOOOOOOOBBBB DUPLICATE COLUMN FOUND "*10)
    
    reserrors=[]

    try:
        if len(df) == 0:
            logger.info('dataframe empty')
        else:
            logger.info("Loading data frame. Rows:" +
                        str(df.shape[1]) + " Cols:" + str(df.shape[0]))
            logger.info("Loading data frame")

        bulkbody = ""

        for index, row in df.iterrows():
            action = {}

            action["index"] = {"_index": row["_index"],
                               "_type": "_doc"}
            if "_id" in row:
                action["index"]["_id"]=row["_id"]
            
            bulkbody += json.dumps(action, cls=DateTimeEncoder) + "\r\n"
            #logger.info(action)
            #print(json.dumps(action, cls=DateTimeEncoder))
            obj = {}

            for i in df.columns:

                if((i != "_index") and (i != "_timestamp")and (i != "_id")):                    
                    if not (type(row[i]) == str and row[i] == 'NaT') and \
                       not (type(row[i]) == pd._libs.tslibs.nattype.NaTType):
                        obj[i] = row[i]
                elif(i == "_timestamp"):
                    if type(row[i]) == int:
                        obj["@timestamp"] = int(row[i])
                    else:    
                        obj["@timestamp"] = int(row[i].timestamp()*1000)

            bulkbody += json.dumps(obj, cls=DateTimeEncoder) + "\r\n"
            #print(json.dumps(obj, cls=DateTimeEncoder))
            

            if len(bulkbody)>512000:
                logger.info("BULK READY:" + str(len(bulkbody)))
                #print(bulkbody)
                bulkres = es.bulk(bulkbody,request_timeout=30)
                logger.info("BULK DONE")
                currec=0
                bulkbody=""
            
                if(not(bulkres["errors"])):     
                    logger.info("BULK done without errors.")   
                else:
                    for item in bulkres["items"]:
                        if "error" in item["index"]:
                            #logger.info(item["index"]["error"])
                            reserrors.append({"error":item["index"]["error"],"id":item["index"]["_id"]})


        if len(bulkbody)>0:
            logger.info("BULK READY FINAL:" + str(len(bulkbody)))
            bulkres = es.bulk(bulkbody)
            #print(bulkbody)
            logger.info("BULK DONE FINAL")

            if(not(bulkres["errors"])):     
                logger.info("BULK done without errors.")   
            else:
                for item in bulkres["items"]:
                    if "error" in item["index"]:
                        #logger.info(item["index"]["error"])
                        reserrors.append({"error":item["index"]["error"],"id":item["index"]["_id"]})

        if len(reserrors) > 0:
            logger.info(reserrors)

    except:
        logger.error("Unable to store data in elastic",exc_info=True)
        