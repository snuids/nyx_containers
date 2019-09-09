# NYX Containers

![badge](https://img.shields.io/badge/made%20with-python-blue.svg?style=flat-square)
![badge](https://img.shields.io/github/languages/code-size/snuids/nyx_containers)
![badge](https://img.shields.io/github/last-commit/snuids/nyx_containers)

NYX Containers provide the code (via the container skeleton) to build a container that can :
* Interact with the 2 databases (Read and/or Write), PostgreSQL and Elasticsearch 
* Consume or Publish messages on the ActiveMQ message broker
* Interact with the Redis (key/value store) container

NYX docker containers. This repository contains the code used to build:
* The **nyx_reportrunner** (Used to generate reports)
* The **nyx_formatconverter** container (Used to convert a file format to another format)
* The **nyx_reportscheduler** (Used to schedule reports)
* The **nyx_xlsimporter** (Used to import xls files into Elasic Search)
* The **nyx_skeleton.py** (Example code, can be used as start point for a new container)

# DOCS
* Read The Docs (https://nyx-containers.readthedocs.io/en/latest/?)

# BUILDING

All the containers can be build using the following command:

```
docker build .
```

The report runner container must be built using the following command:

```
docker build . -f Dockerfileoo
```

# Run

create a startrest.sh file with the following content:

```
#!/bin/sh
echo "STARTING NYX API"
echo "================"

export REDIS_IP="localhost"
export AMQC_URL="YOUR_NYX_SERVER"
export AMQC_LOGIN="admin"
export AMQC_PASSWORD="activemq_pass"
export AMQC_PORT=61613

export ELK_URL="YOUR_NYX_SERVER"
export ELK_LOGIN="user"
export ELK_PASSWORD="ELK_PASS"
export ELK_PORT=9200
export ELK_SSL=true

export USE_LOGSTASH=false


echo "Variables SET"
python container_code.py 
```
