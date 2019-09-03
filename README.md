# NYX Containers
NYX docker containers. This repository contains the code used to build:

* The nyx_reportrunner.py	Save point (Used to generate reports)
* The nyx_formatconverter container (Used to convert a file format to another format)
* The nyx_reportscheduler.py (Used to schedule reports)
* The nyx_xlsimporter.py (Used to import xls files into Elasic Search)
* The nyx_skeleton.py	Save point (Example code, cab be used as start point for a new container)

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
