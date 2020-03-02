#!/bin/bash

set -ex

# relative to the current directory
cd "$(dirname "$0")"

set +x
source fetch_credentials.sh
set -x

HOST_REF=${1?missing first host}
HOST_TEST=${2?missing second host}
DATE=${3:-20200301}

python3 fetch_stats.py \
    validate_data_ref.py \
    --host $HOST_REF \
    --date $DATE

python3 fetch_stats.py \
    validate_data_test.py \
    --host $HOST_TEST \
    --date $DATE

black .

python3 validate.py
