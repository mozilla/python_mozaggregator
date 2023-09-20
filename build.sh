#!/bin/bash
git diff main -- bin/export-avro.sh mozaggregator/bigquery.py > patch
IMAGE=gcr.io/moz-fx-data-airflow-prod-88e0/python_mozaggregator:latest
docker build -t $IMAGE .
# docker push $IMAGE
