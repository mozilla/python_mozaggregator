#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import os
import string
from collections import defaultdict
from io import StringIO

import pandas as pd
import psycopg2
from . import sql
import ujson as json
from moztelemetry.histogram import Histogram

from . import config
from .aggregator import SCALAR_MEASURE_MAP

# Use latest revision, we don't really care about histograms that have
# been removed. This only works though if histogram definitions are
# immutable, which has been the case so far.
_histogram_revision_map = {
    "nightly": "https://hg.mozilla.org/mozilla-central/rev/tip",
    # Aurora channel (aka Dev Edition) is a repacked beta.
    "aurora": "https://hg.mozilla.org/releases/mozilla-beta/rev/tip",
    "beta": "https://hg.mozilla.org/releases/mozilla-beta/rev/tip",
    "release": "https://hg.mozilla.org/releases/mozilla-release/rev/tip"
}
# NOTE: Using `histogram_revision_map.get(...)` will still return `None`.
# Use dict subscripts when mapping URLs with this dictionary.
histogram_revision_map = defaultdict(lambda: _histogram_revision_map['nightly'])
histogram_revision_map.update(_histogram_revision_map)

_metric_printable = set(string.ascii_uppercase + string.ascii_lowercase + string.digits + "_-[].")

db_pass = "POSTGRES_PASS"
db_user = "POSTGRES_USER"
db_host = "POSTGRES_HOST"
db_ro_host = "POSTGRES_RO_HOST"
db_name = "POSTGRES_DB"

# Configure logging via py4j; see https://www.py4j.org/faq.html
logger = logging.getLogger("py4j")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class NoticeLoggingCursor(psycopg2.extensions.cursor):
    """
    Cursor subclass that emits Postgres NOTICE messages (db-level logs) to application logs.

    Introduced for bug 1474590.

    See:
    http://initd.org/psycopg/docs/advanced.html#subclassing-cursor
    https://github.com/zzzeek/sqlalchemy/blob/2f03ec08b5a1c633133c0a38d82b05eb83708f69/lib/sqlalchemy/dialects/postgresql/psycopg2.py#L482-L488
    """

    def execute(self, sql, args=None):
        psycopg2.extensions.cursor.execute(self, sql, args)
        for notice in self.connection.notices:
            level = self.parse_level(notice)
            # NOTICE messages have a newline character at the end
            logger.log(level, notice.rstrip())
        self.connection.notices[:] = []

    @staticmethod
    def parse_level(notice):
        """
        Return a python log level based on the PostgreSQL log level in notice.

        https://www.postgresql.org/docs/9.4/static/runtime-config-logging.html#RUNTIME-CONFIG-SEVERITY-LEVELS
        https://docs.python.org/2/library/logging.html#logging-levels
        """
        prefix, _, _ = notice.partition(':')
        if prefix in ['PANIC', 'FATAL']:
            return logging.CRITICAL
        if prefix in ['ERROR']:
            return logging.ERROR
        if prefix in ['WARNING']:
            return logging.WARNING
        if prefix in ['INFO', 'NOTICE']:
            return logging.INFO
        return logging.DEBUG


def get_db_connection_string(read_only=False):
    if os.getenv("DB_TEST_URL"):
        return os.getenv("DB_TEST_URL")
    elif config.USE_PRODUCTION_DB:
        if (os.getenv(db_pass) and
                os.getenv(db_host) and
                os.getenv(db_ro_host) and
                os.getenv(db_user) and
                os.getenv(db_name)):

            rds_pass = os.getenv(db_pass)
            rds_host = os.getenv(db_host)
            rds_ro_host = os.getenv(db_ro_host)
            rds_user = os.getenv(db_user)
            rds_db = os.getenv(db_name)
        else:
            print("One or more POSTGRES env vars not set.")
            exit(1)

        rds_endpoint = rds_ro_host if read_only else rds_host
        return "dbname={} user={} password={} host={}".format(rds_db, rds_user, rds_pass, rds_endpoint)
    else:
        return "dbname={} user={} password={} host={}".format(config.DBNAME, config.DBUSER, config.DBPASS, config.DBHOST)


def _create_connection(autocommit=True, connection_string_override=None):
    if connection_string_override:
        conn = psycopg2.connect(connection_string_override)
    else:
        conn = psycopg2.connect(get_db_connection_string(False))

    if autocommit:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    return conn


def submit_aggregates(aggregates, dry_run=False):
    _preparedb()

    connection_string = get_db_connection_string(False)

    build_id_count = (
        aggregates[0].map(lambda x: (x[0][:4], _aggregate_to_sql(x)))
                     .filter(lambda x: x[1])
                     .reduceByKey(lambda x, y: x + y)
                     .map(lambda x: _upsert_build_id_aggregates(x[0], x[1], connection_string, dry_run=dry_run))
                     .count())

    submission_date_count = (
        aggregates[1].map(lambda x: (x[0][:3], _aggregate_to_sql(x)))
                     .filter(lambda x: x[1])
                     .reduceByKey(lambda x, y: x + y)
                     .map(lambda x: _upsert_submission_date_aggregates(x[0], x[1], connection_string, dry_run=dry_run))
                     .count())

    # TODO: Auto-vacuuming might be sufficient. Re-enable if needed.
    # _vacuumdb()
    return build_id_count, submission_date_count


def _preparedb():
    conn = _create_connection()
    cursor = conn.cursor(cursor_factory=NoticeLoggingCursor)
    cursor.execute(sql.query)


def _get_complete_histogram(channel, metric, values):
    revision = histogram_revision_map[channel]

    for prefix, labels in SCALAR_MEASURE_MAP.items():
        if metric.startswith(prefix):
            histogram = pd.Series({int(k): v for k, v in values.items()}, index=labels).fillna(0).values
            break
    else:
        histogram = Histogram(metric, {"values": values}, revision=revision).get_value(autocast=False).values

    return list(histogram)


def _aggregate_to_sql(aggregate):
    result = StringIO()
    key, metrics = aggregate
    submission_date, channel, version, application, architecture, os, os_version = key[:3] + key[-4:]
    dimensions = {
        "application": application,
        "architecture": architecture,
        "os": os,
        "osVersion": os_version,
    }

    for metric, payload in metrics.items():
        metric, label, process_type = metric

        if not set(metric).issubset(_metric_printable):
            continue  # Ignore metrics with non printable characters...

        if any(("\u0000" in x for x in [metric, label, application, architecture, os, os_version])):
            continue  # Ignore dimensions with null character

        try:
            # Make sure values fit within a pgsql bigint
            # TODO: we should probably log this event
            if payload["sum"] > (1 << 63) - 1:
                continue

            histogram = _get_complete_histogram(channel, metric, payload["histogram"]) + [payload["sum"], payload["count"]]
            histogram = [str(int(x)) for x in histogram]
        except KeyError:
            # Should eventually log errors
            continue

        dimensions["metric"] = metric
        dimensions["label"] = label
        # Have to special-case content and parent here to maintain backwards compatibility.
        dimensions["child"] = {"content": "true",
                               "parent": "false"}.get(process_type, process_type)

        json_dimensions = json.dumps(dimensions)
        # json.dumps takes care of properly escaping the text but a SQL command
        # will first be interpreted as a string literal before being executed.
        # This doubles the number of backslashes we need.
        json_dimensions = json_dimensions.replace("\\", "\\\\")

        result.write("{}\t{}\n".format(json_dimensions, "{" + ",".join(histogram) + "}"))
    return result.getvalue()


def _upsert_build_id_aggregates(key, stage_table, connection_string, dry_run=False):
    conn = _create_connection(autocommit=False, connection_string_override=connection_string)
    cursor = conn.cursor(cursor_factory=NoticeLoggingCursor)
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
    cursor.execute("select merge_table(%s, %s, %s, %s, %s)", ("build_id", channel, version, build_id, stage_table_name))

    if dry_run:
        conn.rollback()
    else:
        conn.commit()

    cursor.close()
    conn.close()


def _upsert_submission_date_aggregates(key, stage_table, connection_string, dry_run=False):
    conn = _create_connection(autocommit=False, connection_string_override=connection_string)
    cursor = conn.cursor(cursor_factory=NoticeLoggingCursor)
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
    cursor = conn.cursor(cursor_factory=NoticeLoggingCursor)
    cursor.execute("vacuum")
    cursor.close()
    conn.close()


def clear_db():
    # For tests to clear the database between runs.
    conn = _create_connection()
    cursor = conn.cursor(cursor_factory=NoticeLoggingCursor)
    cursor.execute("select tablename from pg_tables where schemaname='public'")
    tables = [r[0] for r in cursor.fetchall()]
    for table in tables:
        # Note: Intentionally not using parameters here so the table name isn't quoted.
        cursor.execute("DROP TABLE IF EXISTS %s CASCADE" % table)
        conn.commit()
    cursor.close()
    conn.close()
