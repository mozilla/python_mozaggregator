# `python_mozaggregator/bin`

This folder contains script to execute the Docker containers. It also contains
scripts for managing workflows.

## `wait-for-it`

The "wait-for-it" shell script comes from https://github.com/vishnubob/wait-for-it

To update this file execute the following command:

```bash
wget https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh -O bin/wait-for-it.sh
```

## `dataproc` testing harness

This script is used to spin up a DataProc cluster with the necessary jars for
connecting to BigQuery. Clusters will automatically delete themselves after 10
minutes of idle. Any command defined in `python_mozaggregator` can be run using
this harness.

```bash
NUM_WORKERS=5 bin/dataproc.sh \
    mobile \
    --output gs://amiyaguchi-dev/mozaggregator/mobile_test/nonprod/20191101/ \
    --num-partitions 200 \
    --date 20191101 \
    --source avro \
    --avro-prefix gs://amiyaguchi-dev/avro-mozaggregator/moz-fx-data-shar-nonprod-efed
```

## `export-avro`

This script can be run locally or through the docker image. You will need to
have access to a sandbox account with access to the
`moz-fx-data-shar-nonprod-efed` or `moz-fx-data-shared-prod` projects.

```bash
export PROJECT_ID=...

bin/export-avro.sh \
    moz-fx-data-shar-nonprod-efed \
    ${PROJECT_ID}:avro_export \
    gs://${PROJECT_ID}/avro-mozaggregator \
    "main_v4" \
    "'nightly', 'beta'" \
    2019-12-15
```

This can also be run through the docker image:

```bash
export PROJECT_ID=...
export GOOGLE_APPLICATION_CREDENTIALS=...

docker run \
    --entrypoint bash \
    -v $GOOGLE_APPLICATION_CREDENTIALS:/tmp/credentials \
    -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/credentials \
    -it mozilla/python_mozaggregator:latest \
        gcloud auth activate-service-account --key-file /tmp/credentials && \
        bin/export-avro.sh \
            moz-fx-data-shar-nonprod-efed \
            ${PROJECT_ID}:avro_export \
            gs://${PROJECT_ID}/avro-mozaggregator \
            "main_v4" \
            "'nightly', 'beta'" \
            2019-12-15
```

The production settings for pre-release aggregates are as follows:

```bash
"main_v4"              "'nightly', 'beta'"
"mobile_metrics_v1"    ""
```
