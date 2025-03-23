#!/bin/sh
echo "STARTING SKELETON"
echo "================="

export AMQC_URL="10.0.10.161"
export AMQC_LOGIN="admin"
export AMQC_PASSWORD="BagStage01!"
export AMQC_PORT=61613

export ELK_URL="localhost"
export ELK_LOGIN=""
export ELK_PASSWORD=""
export ELK_PORT=9200
export ELK_SSL=false

export USE_LOGSTASH=false
export RUNNER=2
export REPORT_URL="https://airnyx.cofelygtc.com/generatedreports"

echo "Variables SET"
python nyx_reportrunner.py 
