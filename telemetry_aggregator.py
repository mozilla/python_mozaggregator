#!/home/hadoop/anaconda2/bin/ipython

import logging
import ujson
from cStringIO import StringIO
from os import environ
from pyspark import SparkContext, SparkConf
from moztelemetry.dataset import Dataset
from mozaggregator.aggregator import _aggregate_metrics
from mozaggregator.db import _preparedb, get_db_connection_string,\
        _aggregate_to_sql, _create_connection
from datetime import datetime

conf = SparkConf().setAppName('telemetry-aggregates')
sc = SparkContext(conf=conf)


def aggregate_metrics(sc, channels, submission_date, fennec_ping_fraction=1):
    if not isinstance(channels, (tuple, list)):
        channels = [channels]

    channels = set(channels)

    fennec_pings = Dataset.from_source('telemetry') \
                  .where(appUpdateChannel=lambda x : x in channels, 
                         submissionDate=submission_date,
                         docType='saved_session',
                         sourceVersion='4',
                         appName = 'Fennec') \
                  .records(sc, sample=fennec_ping_fraction)

    return _aggregate_metrics(fennec_pings)


before = datetime.now()

date = environ['date']
print "Running job for {}".format(date)

aggregates = aggregate_metrics(sc, ("nightly", "aurora", "beta", "release"), date, fennec_ping_fraction=1)

print "Number of build-id aggregates: {}".format(aggregates[0].count())
print "Number of submission date aggregates: {}".format(aggregates[1].count())

during = datetime.now()
print "Took {}s to count".format((during-before).total_seconds())

dry_run = False
_preparedb()
connection_string = get_db_connection_string()

### Create table syntax
# create table table_update_dates_fennec (tablename text primary key, submission_dates text[]);
# create index on table_update_dates_fennec (tablename);

was_processed_query = ("select 1 from table_update_dates_fennec as t "
                       "where t.tablename = '{tablename}' and '{submission_date}' = any(t.submission_dates)")

is_processed_query = ("with upsert as (update table_update_dates_fennec "
                                       "set submission_dates = submission_dates || array['{submission_date}'] "
                                       "where tablename = '{tablename}' "
                                       "returning *) "
                        "insert into table_update_dates_fennec "
                        "select * from (values ('{tablename}', array['{submission_date}'])) as t "
                        "where not exists(select 1 from upsert);")

build_ids_ignored = sc.accumulator(0)
build_ids_submitted = sc.accumulator(0)
submission_dates_ignored = sc.accumulator(0)
submission_dates_submitted = sc.accumulator(0)

def upsert_build_id_aggregates(key, stage_table, connection_string, dry_run=False):
    conn = _create_connection(autocommit=False, connection_string_override=connection_string)
    cursor = conn.cursor()
    submission_date, channel, version, build_id = key

    # Aggregates with different submisssion_dates write to the same tables, we need a lock
    cursor.execute("select lock_transaction(%s, %s, %s, %s)", ("build_id", channel, version, build_id))

    tablename = "_".join(("build_id", channel, version, build_id)) 
    query_kwargs = {'tablename': tablename, 'submission_date': submission_date}
    cursor.execute(was_processed_query.format(**query_kwargs))
    if cursor.fetchone():
        # This aggregate has already been processed
        conn.rollback()
        build_ids_ignored.add(1)
        return

    build_ids_submitted.add(1)
    cursor.execute("select create_temporary_table(%s, %s, %s, %s)", ("build_id", channel, version, build_id))
    stage_table_name = cursor.fetchone()[0]

    cursor.copy_from(StringIO(stage_table), stage_table_name, columns=("dimensions", "histogram"))
    cursor.execute("select merge_table(%s, %s, %s, %s, %s)", ('build_id', channel, version, build_id, stage_table_name))

    cursor.execute(is_processed_query.format(**query_kwargs))

    if dry_run:
        conn.rollback()
    else:
        conn.commit()

    # will jobs fail here (after committing), forcing retry?
    cursor.close()
    conn.close()

def upsert_submission_date_aggregates(key, stage_table, connection_string, dry_run=False):
    conn = _create_connection(autocommit=False, connection_string_override=connection_string)
    cursor = conn.cursor()
    submission_date, channel, version = key

    tablename = "_".join(("submission_date", channel, version, submission_date)) 
    query_kwargs = {'tablename': tablename, 'submission_date': submission_date}
    cursor.execute(was_processed_query.format(**query_kwargs))
    if cursor.fetchone():
        # This aggregate has already been processed
        conn.rollback()
        submission_dates_ignored.add(1)
        return

    submission_dates_submitted.add(1)
    cursor.execute("select create_temporary_table(%s, %s, %s, %s)", ("submission_date", channel, version, submission_date))
    stage_table_name = cursor.fetchone()[0]

    cursor.copy_from(StringIO(stage_table), stage_table_name, columns=("dimensions", "histogram"))
    cursor.execute("select merge_table(%s, %s, %s, %s, %s)", ("submission_date", channel, version, submission_date, stage_table_name))

    cursor.execute(is_processed_query.format(**query_kwargs))

    if dry_run:
        conn.rollback()
    else:
        conn.commit()

    cursor.close()
    conn.close()


build_id_count = aggregates[0].\
                 map(lambda x: (x[0][:4], _aggregate_to_sql(x))).\
                 filter(lambda x: x[1]).\
                 reduceByKey(lambda x, y: x + y).\
                 map(lambda x: upsert_build_id_aggregates(x[0], x[1], connection_string, dry_run=dry_run)).\
                 count()

submission_date_count = aggregates[1].\
                        map(lambda x: (x[0][:3], _aggregate_to_sql(x))).\
                        filter(lambda x: x[1]).\
                        reduceByKey(lambda x, y: x + y).\
                        map(lambda x: upsert_submission_date_aggregates(x[0], x[1], connection_string, dry_run=dry_run)).\
                        count()

after = datetime.now()
print "{} Build ID Aggregates, {} Submission date aggregates".format(build_id_count, submission_date_count)
print "{} Build ID aggregates submitted, {} Ignored".format(build_ids_submitted.value, build_ids_ignored.value)
print "{} Submission date aggregates submitted, {} ignored".format(submission_dates_submitted.value, submission_dates_ignored.value)
print "Took {}s to submit".format((after-during).total_seconds())

sc.stop()
