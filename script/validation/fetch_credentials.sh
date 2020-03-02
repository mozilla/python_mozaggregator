#!/bin/bash

creds="$(aws s3 cp s3://telemetry-spark-emr-2/aggregator_database_envvars.json -)"

function dev_creds() {
    key=$1
    echo "$creds" | jq -r ".${key}"
}

export POSTGRES_DB="$(dev_creds POSTGRES_DB)"
export POSTGRES_USER="$(dev_creds POSTGRES_USER)"
export POSTGRES_PASS="$(dev_creds POSTGRES_PASS)"
export POSTGRES_HOST="$(dev_creds POSTGRES_RO_HOST)"


# useful command when sourcing this script
: << EOF
PGPASSWORD=$POSTGRES_PASS psql \
    --host=$POSTGRES_RO_HOST \
    --username=$POSTGRES_USER \
    --dbname=$POSTGRES_DB
EOF