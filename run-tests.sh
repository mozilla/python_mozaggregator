#!/bin/bash
set -e

if [ -z "$SPARK_HOME" ]; then
    echo 'You need to set $SPARK_HOME to run these tests.' >&2
    exit 1
fi

PYFORJ=`ls -1 $SPARK_HOME/python/lib/py4j-*-src.zip | head -1`

export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:../
export PYTHONPATH=$PYTHONPATH:$PYFORJ

clean_exit() {
    local error_code="$?"
    for job in $(jobs -p); do
	pkill -9 -P $job >/dev/null 2>&1 || true
	kill -9 $job >/dev/null 2>&1 || true
    done
    rm -rf "PGSQL_DATA"
    return $error_code
}

check_for_cmd () {
    if ! which "$1" >/dev/null 2>&1
    then
        echo "Could not find $1 command" 1>&2
        exit 1
    fi
}

wait_for_line () {
    while read line
    do
        echo "$line" | grep -q "$1" && break
    done < "$2"
    # Read the fifo for ever otherwise process would block
    cat "$2" >/dev/null &
}

check_for_cmd postgres

trap "clean_exit" EXIT

# Start PostgreSQL process for tests
PGSQL_DATA=`mktemp -d /tmp/PGSQL-XXXXX`
PGSQL_PATH=`pg_config --bindir`
${PGSQL_PATH}/initdb ${PGSQL_DATA}
echo "host all all 0.0.0.0/0 trust" >> $PGSQL_DATA/pg_hba.conf
mkfifo ${PGSQL_DATA}/out
${PGSQL_PATH}/postgres -h '*' -F -k ${PGSQL_DATA} -D ${PGSQL_DATA} &> ${PGSQL_DATA}/out &
# Wait for PostgreSQL to start listening to connection
wait_for_line "database system is ready to accept connections" ${PGSQL_DATA}/out
export DB_TEST_URL="postgresql:///?host=${PGSQL_DATA}&dbname=template1"

# Launch db service
mkfifo ${PGSQL_DATA}/out_service
python ./mozaggregator/service.py -d &> ${PGSQL_DATA}/out_service &
wait_for_line "* Running " ${PGSQL_DATA}/out_service

nosetests ./tests/
