#!/bin/sh
echo "STARTING SKELETON"
echo "================="

export AMQC_URL="test2.nyx-ds.com"
export AMQC_LOGIN="admin"
export AMQC_PASSWORD="***REMOVED***"
export AMQC_PORT=61613

export ELK_URL="test2.nyx-ds.com"
export ELK_LOGIN="user"
export ELK_PASSWORD="***REMOVED***"
export ELK_PORT=9200
export ELK_SSL=true

export KIZEO_USER="BAC_KPI_IT"
export KIZEO_PASSWORD="***REMOVED***"
export KIZEO_COMPANY="cofely38"


export USE_LOGSTASH=false

echo "Variables SET"
python nyx_xlsimporter.py 