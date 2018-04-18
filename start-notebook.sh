#!/bin/sh
export SERVICE_HOME="$(cd "`dirname "$0"`"; pwd)"
export PYTHONPATH=${PYTHONPATH}:/${SERVICE_HOME}/app
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --port 8080"

pyspark \
	--name Jupyter_Spark \
	--master local[*]