#!/bin/bash
set -eo pipefail

if [ -z "$SPARK_HOME" ]; then
    echo 'You need to set $SPARK_HOME to run these tests.' >&2
    exit 1
fi

python "$(which nosetests)" --with-coverage --cover-package=mozaggregator ./tests/*.py || exit 1

# Show a text coverage report after a test run.
coverage report -m
