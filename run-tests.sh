#!/bin/bash
set -eo pipefail

if [ -z "$SPARK_HOME" ]; then
    echo 'You need to set $SPARK_HOME to run these tests.' >&2
    exit 1
fi

if [ -z "$POSTGRES_HOST" ]; then
    echo 'You need to set $POSTGRES_HOST to run these tests.' >&2
    exit 1
fi

if [ -z "$POSTGRES_USER" ]; then
    echo 'You need to set $POSTGRES_USER to run these tests.' >&2
    exit 1
fi

# See https://github.com/travis-ci/travis-ci/issues/7940
export BOTO_CONFIG=/dev/null
export DB_TEST_URL="dbname=postgres user=${POSTGRES_USER} host=${POSTGRES_HOST}"

# Allow mozaggregator to be imported.
export PYTHONPATH=$PYTHONPATH:.

python ./mozaggregator/service.py -d &

python "$(which nosetests)" --with-coverage --cover-package=mozaggregator ./tests/*.py || exit 1

# Show a text coverage report after a test run.
coverage report -m
