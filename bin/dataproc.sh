#!/bin/bash

# A testing script for verifying the spark-bigquery connector with the existing
# mozaggregator code. This requires `gcloud` to be configured to point at a
# sandbox project for reading data from `payload_bytes_decoded`.

set -e

REGION=us-west1
MODULE="python_mozaggregator"
NUM_WORKERS=${NUM_WORKERS:-1}


function bootstrap() {
    local bucket=$1

    # create the package artifacts
    rm -rf dist build
    python setup.py bdist_egg
    gsutil cp "dist/${MODULE}*.egg" "gs://${bucket}/bootstrap/${MODULE}.egg"

    # create the initialization script and runner
    mkdir -p bootstrap
    cd bootstrap
    echo "apt install --yes python-dev" > install-python-dev.sh
    tee mozaggregator-runner.py >/dev/null << EOF
# This runner has been auto-generated from mozilla/python_mozaggregator/bin/dataproc.sh.
# Any changes made to the runner file will be over-written on subsequent runs.
from mozaggregator import cli

try:
    cli.entry_point(auto_envvar_prefix="MOZETL")
except SystemExit:
    # avoid calling sys.exit() in databricks
    # http://click.palletsprojects.com/en/7.x/api/?highlight=auto_envvar_prefix#click.BaseCommand.main
    pass
EOF
    cd ..
    gsutil cp bootstrap/* "gs://${bucket}/bootstrap/"
}


function delete_cluster() {
    local cluster_id=$1
    gcloud dataproc clusters delete ${cluster_id} --region=${REGION}
}


function create_cluster() {
    local cluster_id=$1
    local bucket=$2
    requirements=$(tr "\n" " " < requirements/build.txt)

    function cleanup {
        delete_cluster ${cluster_id}
    }
    trap cleanup EXIT

    gcloud dataproc clusters create ${cluster_id} \
        --image-version 1.3 \
        --num-preemptible-workers ${NUM_WORKERS} \
        --properties ^#^spark:spark.jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar#spark:spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}#spark:spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
        --metadata "PIP_PACKAGES=${requirements}" \
        --initialization-actions \
            gs://${bucket}/bootstrap/install-python-dev.sh,gs://dataproc-initialization-actions/python/pip-install.sh \
        --region=${REGION}
}


function submit() {
    cluster_id=$1
    bucket=$2
    # pass the rest of the parameters from the main function
    shift 2
    gcloud dataproc jobs submit pyspark \
        gs://${bucket}/bootstrap/mozaggregator-runner.py \
        --cluster ${cluster_id} \
        --region ${REGION} \
        --py-files=gs://${bucket}/bootstrap/${MODULE}.egg \
        -- "$@"
}


function main() {
    cd "$(dirname "$0")/.."
    bucket=$(gcloud config get-value project)
    cluster_id=test-mozaggregator
    bootstrap $bucket
    create_cluster $cluster_id $bucket
    submit $cluster_id $bucket "$@"
}


if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
