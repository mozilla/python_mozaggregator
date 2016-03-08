#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import psycopg2
import pandas as pd
import ujson as json
import boto.rds2
import os
import string
import sql
import config

from moztelemetry.spark import Histogram
from boto.s3.connection import S3Connection
from cStringIO import StringIO
from mozaggregator.aggregator import simple_measures_labels, count_histogram_labels

# Use latest revision, we don't really care about histograms that have
# been removed. This only works though if histogram definitions are
# immutable, which has been the case so far.
histogram_revision_map = {"nightly": "https://hg.mozilla.org/mozilla-central/rev/tip",
                          "aurora": "https://hg.mozilla.org/releases/mozilla-aurora/rev/tip",
                          "beta": "https://hg.mozilla.org/releases/mozilla-beta/rev/tip",
                          "release": "https://hg.mozilla.org/releases/mozilla-release/rev/tip"}

_metric_printable = set(string.ascii_uppercase + string.ascii_lowercase + string.digits + "_-[]")

def get_db_connection_string():
    if os.getenv("DB_TEST_URL"):
        return os.getenv("DB_TEST_URL")
    elif config.USE_PRODUCTION_DB:
        s3 = S3Connection(host="s3-us-west-2.amazonaws.com")
        secret = json.loads(s3.get_bucket(config.BUCKET).get_key(config.SECRET).get_contents_as_string())["password"]
        rds = boto.rds2.connect_to_region(config.REGION)
        db = rds.describe_db_instances(config.RDS)["DescribeDBInstancesResponse"]["DescribeDBInstancesResult"]["DBInstances"][0]
        return "dbname={} user={} password={} host={}".format(db["DBName"], db["MasterUsername"], secret, db["Endpoint"]["Address"])
    else:
        return "dbname={} user={} password={} host={}".format(config.DBNAME, config.DBUSER, config.DBPASS, config.DBHOST)


def _create_connection(autocommit=True, connection_string_override=None):
    if connection_string_override:
        conn = psycopg2.connect(connection_string_override)
    else:
        conn = psycopg2.connect(get_db_connection_string())

    if autocommit:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    return conn


def submit_aggregates(aggregates, dry_run=False):
    _preparedb()

    connection_string = get_db_connection_string()

    build_id_count = aggregates[0].\
                     map(lambda x: (x[0][:4], _aggregate_to_sql(x))).\
                     reduceByKey(lambda x, y: x + y).\
                     map(lambda x: _upsert_build_id_aggregates(x[0], x[1], connection_string, dry_run=dry_run)).\
                     count()

    submission_date_count = aggregates[1].\
                            map(lambda x: (x[0][:3], _aggregate_to_sql(x))).\
                            reduceByKey(lambda x, y: x + y).\
                            map(lambda x: _upsert_submission_date_aggregates(x[0], x[1], connection_string, dry_run=dry_run)).\
                            count()

    # TODO: Auto-vacuuming might be sufficient. Re-enable if needed.
    # _vacuumdb()
    return build_id_count, submission_date_count


def _preparedb():
    conn = _create_connection()
    cursor = conn.cursor()
    cursor.execute(sql.query)


def _get_complete_histogram(channel, metric, values):
    revision = histogram_revision_map.get(channel, "nightly")  # Use nightly revision if the channel is unknown

    if metric.startswith("SIMPLE_MEASURES"):
        histogram = pd.Series({int(k): v for k, v in values.iteritems()}, index=simple_measures_labels).fillna(0).values
    elif metric.startswith("[[COUNT]]_"):  # Count histogram
        histogram = pd.Series({int(k): v for k, v in values.iteritems()}, index=count_histogram_labels).fillna(0).values
    else:
        histogram = Histogram(metric, {"values": values}, revision=revision).get_value(autocast=False).values

    return list(histogram)


def _aggregate_to_sql(aggregate):
    result = StringIO()
    key, metrics = aggregate
    submission_date, channel, version, application, architecture, os, os_version, e10s = key[:3] + key[-5:]
    dimensions = {"application": application,
                  "architecture": architecture,
                  "os": os,
                  "osVersion": os_version,
                  "e10sEnabled": e10s}

    for metric, payload in metrics.iteritems():
        metric, label, child = metric

        if not set(metric).issubset(_metric_printable):
            continue  # Ignore metrics with non printable characters...

        try:
            # Make sure values fit within a pgsql bigint
            # TODO: we should probably log this event
            if payload["sum"] > (1 << 63) - 1:
                continue

            histogram = _get_complete_histogram(channel, metric, payload["histogram"]) + [payload["sum"], payload["count"]]
            histogram = [str(long(x)) for x in histogram]
        except KeyError:
            continue

        dimensions["metric"] = metric
        dimensions["label"] = label
        dimensions["child"] = child

        json_dimensions = json.dumps(dimensions)
        # json.dumps takes care of properly escaping the text but a SQL command
        # will first be interpreted as a string literal before being executed.
        # This doubles the number of backslashes we need.
        json_dimensions = json_dimensions.replace("\\", "\\\\")

        result.write("{}\t{}\n".format(json_dimensions, "{" + ",".join(histogram) + "}"))

    return result.getvalue()


def _upsert_build_id_aggregates(key, stage_table, connection_string, dry_run=False):
    conn = _create_connection(autocommit=False, connection_string_override=connection_string)
    cursor = conn.cursor()
    submission_date, channel, version, build_id = key

    # Aggregates with different submisssion_dates write to the same tables, we need a lock
    cursor.execute("select lock_transaction(%s, %s, %s, %s)", ("build_id", channel, version, build_id))

    cursor.execute("select was_processed(%s, %s, %s, %s, %s)", ("build_id", channel, version, build_id, submission_date))
    if cursor.fetchone()[0]:
        # This aggregate has already been processed
        conn.rollback()
        return

    cursor.execute("select create_temporary_table(%s, %s, %s, %s)", ("build_id", channel, version, build_id))
    stage_table_name = cursor.fetchone()[0]

    cursor.copy_from(StringIO(stage_table), stage_table_name, columns=("dimensions", "histogram"))
    cursor.execute("select merge_table(%s, %s, %s, %s, %s)", ('build_id', channel, version, build_id, stage_table_name))

    if dry_run:
        conn.rollback()
    else:
        conn.commit()

    cursor.close()
    conn.close()


def _upsert_submission_date_aggregates(key, stage_table, connection_string, dry_run=False):
    conn = _create_connection(autocommit=False, connection_string_override=connection_string)
    cursor = conn.cursor()
    submission_date, channel, version = key

    cursor.execute("select was_processed(%s, %s, %s, %s, %s)", ("submission_date", channel, version, submission_date, submission_date))
    if cursor.fetchone()[0]:
        # This aggregate has already been processed
        conn.rollback()
        return

    cursor.execute("select create_temporary_table(%s, %s, %s, %s)", ("submission_date", channel, version, submission_date))
    stage_table_name = cursor.fetchone()[0]

    cursor.copy_from(StringIO(stage_table), stage_table_name, columns=("dimensions", "histogram"))
    cursor.execute("select merge_table(%s, %s, %s, %s, %s)", ("submission_date", channel, version, submission_date, stage_table_name))

    if dry_run:
        conn.rollback()
    else:
        conn.commit()

    cursor.close()
    conn.close()


def _vacuumdb():
    conn = _create_connection()
    conn.set_isolation_level(0)
    cursor = conn.cursor()
    cursor.execute("vacuum")
    cursor.close()
    conn.close()
