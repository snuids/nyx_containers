#!/bin/sh
echo "STARTING SKELETON"
echo "================="

export AMQC_URL="airnyx.cofelygtc.com"
export AMQC_LOGIN="admin"
export AMQC_PASSWORD="***REMOVED***"
export AMQC_PORT=61613

export ELK_URL="airnyx.cofelygtc.com"
export ELK_LOGIN="user"
export ELK_PASSWORD="***REMOVED***"
export ELK_PORT=9200
export ELK_SSL=true

export USE_LOGSTASH=false
export RUNNER=2
export REPORT_URL="https://airnyx.cofelygtc.com/generatedreports"

echo "Variables SET"
python nyx_reportscheduler.py 
