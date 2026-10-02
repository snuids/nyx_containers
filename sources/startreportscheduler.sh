#!/bin/sh
echo "STARTING SKELETON"
echo "================="

export AMQC_URL="energy.marmar.ovh"
export AMQC_LOGIN="admin"
export AMQC_PASSWORD="${AMQC_PASSWORD:?Set AMQC_PASSWORD in the environment}"
export AMQC_PORT=61613

export ELK_URL="https://localhost/eee"
export ELK_LOGIN=""
export ELK_PASSWORD=""
export ELK_PORT=443
export ELK_SSL=true

export USE_LOGSTASH=false
export RUNNER=2
export REPORT_URL="https://localhost/generatedreports"

export SMTP_USER=marmar@snuids.ovh
export SMTP_PASSWORD="${SMTP_PASSWORD:?Set SMTP_PASSWORD in the environment}"
export SMTP_ADDRESS=ssl0.ovh.net
export SMTP_FROM=noreply@snuids.ovh
export SMTP_SSL=true
export SMTP_TLS=false


echo "Variables SET"
python nyx_reportscheduler.py 
