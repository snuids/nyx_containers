# NYX Containers

![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/snuids/nyx_containers/badges/coverage.json&style=flat-square)
![CI](https://img.shields.io/github/actions/workflow/status/snuids/nyx_containers/ci-requirements7.yml?style=flat-square&label=tests)

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

# TESTS

```shell
python -m unittest discover -s tests   # needs the dependencies of sources/requirements7.txt
sh scripts/ci_test.sh                  # what the CI runs: coverage, and a floor under it
```

The scripts here are containers: everything that talks to the cluster, the broker or Redis lives
under `if __name__ == '__main__'`, so the module imports but its functions expect the globals that
block creates (`logger`, `es`, `conn`, `redisserver`). `tests/harness.py` imports a script and
hands it those globals as doubles that record what would have left for the cluster or the broker;
the tests assert on that. No container, no cluster, no broker is needed — the suite runs on a bare
interpreter that has the requirements of `sources/requirements7.txt`.

Coverage is measured by `scripts/ci_test.sh` and gated at 75 % (measured on 2026-11-09, from no
test at all before that). Raise the floor as tests land, never lower it. What the suite holds
today:

| Module | Coverage | What it pins down |
| --- | --- | --- |
| `lib/redact.py` | 100 % | what a container may log: token, phone, `jdbc_password`, at any depth |
| `lib/elastictopandas.py` | 100 % | the report query, its time range, and the scroll that fills the frame |
| `lib/pandastoelastic.py` | 90 % | the bulk body: action/document pair, `@timestamp`, `NaT`, the 500 kB flush |
| `nyx_buildreport.py` | 89 % | the python/xlsx builder: parameters, `#@ONLOAD` cells, the report log |
| `nyx_buildreport_doc.py` | 81 % | the docx builder: placeholders, table cells, the document it writes |
| `nyx_reportrunner.py` | 74 % | the job itself: the four report types, the PDF conversion, the downloads, the always-sent step 3 |
| `nyx_lambda.py` | 72 % | `loadConfig` (topics, cron, interval, restapi), dispatch, run logs, stats, requirements |
| `nyx_reportscheduler.py` | 70 % | `now-30d` formulas, the UI weekdays to cron, `checkTasks`: due, not due, late, broken |
| `nyx_xlsimporter.py` | 67 % | column types to mapping, index naming, the log messages |
| `nyx_monitordocker.py` | 61 % | restart/stop/start of one named container, the `docker_status` documents |
| `nyx_formatconverter.py` | 60 % | the contract around `convert.sh`: files, destination, original name |

`.coveragerc` leaves one file out of the measure: `nyx_skeleton.py`, the example copied into a new
container. Nothing that runs is omitted — in particular the `if __name__ == '__main__'` blocks stay
in the denominator, because excluding them would raise the percentage without a test.

The two report builders and the lambda used to run their whole generation at import, which is why
they could not be tested at all: each now exposes a `main(argv=None, client=None)` — the runner
still calls it from the command line — and a test can hand in a cluster double.

Not covered yet: the containers' `__main__` blocks (clients, loops, life signs), and the remaining
branches of `nyx_xlsimporter`, `nyx_monitordocker` and `nyx_formatconverter`.

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
