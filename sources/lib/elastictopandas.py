import sys
import json
import time
import pytz
import logging
import tzlocal
import datetime
import traceback
import pandas as pd
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC

#logger=logging.getLogger()

containertimezone=pytz.timezone(tzlocal.get_localzone().zone)


def genericIntervalSearch(es,index,query="*",start=None,end=None,doctype="doc",sort=None,timestampfield="@timestamp",_source=[] ,datecolumns=[]):
    logger = logging.getLogger()  
    array=[]
    recs=[]
    
    try:        
        finalquery={
            "_source": _source,
            "query": {
              "bool": {
                "must": [
                  {
                    "query_string": {
                      "query": query,
                      "analyze_wildcard": True
                    }
                  }
                ]
              }
            }
        }
        if start !=None:
            finalquery["query"]["bool"]["must"].append({
                "range": {
                  
                }
              }); 
            print (finalquery)
            finalquery["query"]["bool"]["must"][len(finalquery["query"]["bool"]["must"])-1]["range"][timestampfield]={
                    "gte": int(start.timestamp())*1000,
                    "lte": int(end.timestamp())*1000,
                    "format": "epoch_millis"
                  }
            print (finalquery)
            

        if sort !=None:
            finalquery["sort"]=sort
        
        logger.info(finalquery)

        res=es.search(index=index
        ,doc_type=doctype
        ,size=10000        
        ,scroll = '2m'
        ,body=finalquery
        )

#        logger.info(res)    

        sid = res['_scroll_id']
        scroll_size = res['hits']['total']        

        array=[]
        for res2 in res["hits"]["hits"]:
            res2["_source"]["_id"]=res2["_id"]
            res2["_source"]["_index"]=res2["_index"]
             
            array.append(res2["_source"])

        recs=len(res['hits']['hits'])

        while (scroll_size > 0):
            logger.info ("Scrolling..."+str(scroll_size))
            res = es.scroll(scroll_id = sid, scroll = '2m')
            # Update the scroll ID
            sid = res['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scroll_size = len(res['hits']['hits'])
            logger.info ("scroll size: " + str(scroll_size))
            logger.info ("Next page:"+str(len(res['hits']['hits'])))
            recs+=len(res['hits']['hits'])

            for res2 in res["hits"]["hits"]:
                res2["_source"]["_id"]=res2["_id"]
                res2["_source"]["_index"]=res2["_index"]
                
                array.append(res2["_source"])
            

    except Exception as e:
        logger.error("Unable to load data")
        logger.error(e)
    df=pd.DataFrame(array)
    if len(datecolumns)>0 and len(df)>0:
      print("=======>")
      for col in datecolumns:
        print("Converting %s" %(col))
        df[col] =  pd.to_datetime(df[col],utc=True).dt.tz_convert(containertimezone)
        
    return df