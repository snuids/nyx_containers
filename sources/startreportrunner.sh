#!/bin/sh
echo "STARTING SKELETON"
echo "================="

export AMQC_URL="localhost"
export AMQC_LOGIN="admin"
export AMQC_PASSWORD="${AMQC_PASSWORD:?Set AMQC_PASSWORD in the environment}"
export AMQC_PORT=61613

export ELK_URL="https://localhost/eee"
export ELK_LOGIN=""
export ELK_PASSWORD=""
export ELK_PORT=9200
export ELK_SSL=false

export USE_LOGSTASH=false
export RUNNER=2
export REPORT_URL="https://localhost/generatedreports"

echo "Variables SET"
python nyx_reportrunner.py 
