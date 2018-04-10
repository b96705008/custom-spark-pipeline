#!/bin/sh
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --port 8080"

pyspark \
	--name Jupyter_Spark \
	--master local[*]