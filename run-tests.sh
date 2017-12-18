#!/bin/bash
set -e

if [ -z "$SPARK_HOME" ]; then
    echo 'You need to set $SPARK_HOME to run these tests.' >&2
    exit 1
fi

# see https://github.com/travis-ci/travis-ci/issues/7940
export BOTO_CONFIG=/dev/null

# make sure mozaggregator is properly importable
# this is mainly needed for TravisCI support
source ~/.bashrc # run the Bash config, in case we haven't opened a new shell since ansible provisioned our machine
export PYTHONPATH=$PYTHONPATH:.

# set connection string needed to access the database
# this is used by `./mozaggregator/service.py` and `nosetests ./tests/`
export DB_TEST_URL="dbname=postgres user=$USER host=127.0.0.1"

clean_exit() {
    local error_code="$?" # save the original error code
    for job in $(jobs -p); do
        pkill -9 -P $job >/dev/null 2>&1 || true
        kill -9 $job >/dev/null 2>&1 || true
    done
    rm -rf "PGSQL_DATA"
    return $error_code
}

check_for_cmd () {
    which "$1" >/dev/null 2>&1 || {
        echo "Could not find '$1' command" >&2
        exit 1
    }
}

wait_for_line () {
    echo "Waiting for '$1' to appear in file '$2'..."
    timeout 20 grep -q "$1" < "$2" || {
        echo "ERROR: waiting for '$1' to appear in file '$2' failed or timed out" >&2
        return 1
    }
    return 0
}

trap clean_exit EXIT # run clean_exit() when bash exits

PGSQL_DATA=$(mktemp -d /tmp/PGSQL-XXXXX) # temp dir for database storage, and the database output FIFO
PGSQL_PATH=$(pg_config --bindir) # PostgreSQL binaries path

# make sure we have everything we need to run
check_for_cmd ${PGSQL_PATH}/initdb
check_for_cmd ${PGSQL_PATH}/postgres
check_for_cmd ~/miniconda2/bin/python
check_for_cmd nosetests

# start a PostgreSQL database server in the background with a new database
${PGSQL_PATH}/initdb ${PGSQL_DATA} # initialize the database
echo "host all all 0.0.0.0/0 trust" >> $PGSQL_DATA/pg_hba.conf # allow anyone to access the database
mkfifo ${PGSQL_DATA}/out # create output FIFO for the database to let us read the output programmatically
${PGSQL_PATH}/postgres -h '*' -F -k ${PGSQL_DATA} -D ${PGSQL_DATA} > ${PGSQL_DATA}/out 2>&1 & # start the database server
wait_for_line "database system is ready to accept connections" ${PGSQL_DATA}/out || { # wait for PostgreSQL to start listening for connections
    cat ${PGSQL_DATA}/out # print out the log for convenience
    exit 1
}

# launch the HTTP API service in the background
mkfifo ${PGSQL_DATA}/out_service
~/miniconda2/bin/python ./mozaggregator/service.py -d > ${PGSQL_DATA}/out_service 2>&1 &
wait_for_line "* Running " ${PGSQL_DATA}/out_service || {
    cat ${PGSQL_DATA}/out_service # print out the log for convenience
    exit 1
}

~/miniconda2/bin/python "$(which nosetests)" ./tests/*.py
