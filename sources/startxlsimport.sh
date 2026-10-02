#!/bin/sh
echo "STARTING SKELETON"
echo "================="

export AMQC_URL="test2.nyx-ds.com"
export AMQC_LOGIN="admin"
export AMQC_PASSWORD="${AMQC_PASSWORD:?Set AMQC_PASSWORD in the environment}"
export AMQC_PORT=61613

export ELK_URL="test2.nyx-ds.com"
export ELK_LOGIN="user"
export ELK_PASSWORD="${ELK_PASSWORD:?Set ELK_PASSWORD in the environment}"
export ELK_PORT=9200
export ELK_SSL=true

export USE_LOGSTASH=false

echo "Variables SET"
python nyx_xlsimporter.py 