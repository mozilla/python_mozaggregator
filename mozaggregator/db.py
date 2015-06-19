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


def create_connection(autocommit=True, host_override=None):
    # import boto.rds2  # The serializer doesn't pick this one up for some reason when using emacs...

    connection_string = os.getenv("DB_TEST_URL")  # Used only for testing
    if connection_string:
        conn = psycopg2.connect(connection_string)
    else:
        s3 = S3Connection()
        config = s3.get_bucket("telemetry-spark-emr").get_key("aggregator_credentials").get_contents_as_string()
        config = json.loads(config)

        rds = boto.rds2.connect_to_region("us-west-2")
        db = rds.describe_db_instances("telemetry-aggregates")["DescribeDBInstancesResponse"]["DescribeDBInstancesResult"]["DBInstances"][0]
        host = host_override or db["Endpoint"]["Address"]
        dbname = db["DBName"]
        user = db["MasterUsername"]

        conn = psycopg2.connect(dbname=dbname, user=user, password=config["password"], host=host)

    if autocommit:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    return conn


def submit_aggregates(aggregates, dry_run=False):
    _preparedb()

    build_id_count = aggregates[0].groupBy(lambda x: x[0][:4]).\
                                   map(lambda x: _upsert_build_id_aggregates(x, dry_run=dry_run)).\
                                   count()

    submission_date_count = aggregates[1].groupBy(lambda x: x[0][:3]).\
                                          map(lambda x: _upsert_submission_date_aggregates(x, dry_run=dry_run)).\
                                          count()

    _vacuumdb()
    return build_id_count, submission_date_count


def _preparedb():
    conn = create_connection()
    cursor = conn.cursor()
    query = r"""
create or replace function aggregate_table_name(prefix text, channel text, version text, date text) returns text as $$
begin
    return format('%s_%s_%s_%s', prefix, channel, version, date);
end
$$ language plpgsql strict immutable;


create or replace function aggregate_arrays(acc bigint[], x bigint[]) returns bigint[] as $$
begin
    return (select array(
                select sum(elem)
                from (values (1, acc), (2, x)) as t(idx, arr)
                     , unnest(t.arr) with ordinality x(elem, rn)
                group by rn
                order by rn));
end
$$ language plpgsql strict immutable;


drop aggregate if exists aggregate_histograms(bigint[]);
create aggregate aggregate_histograms (bigint[]) (
    sfunc = aggregate_arrays, stype = bigint[], initcond = '{}'
);


create or replace function merge_table(prefix text, channel text, version text, date text, stage_table regclass) returns void as $$
declare
    tablename text;
    table_exists bool;
begin
    tablename := aggregate_table_name(prefix, channel, version, date);
    -- Check if table exists and if not create one
    table_exists := (select exists (select 1 from information_schema.tables where table_schema = 'public' and table_name = tablename));

    if not table_exists then
        execute format('create table %s as table %s', tablename, stage_table);
        execute format('create index on %s using GIN (dimensions jsonb_path_ops)', tablename);
        return;
    end if;

    -- Update existing tuples and delete matching rows from the staging table
    execute 'with merge as (update ' || tablename || ' as dest
	                        set histogram = aggregate_arrays(dest.histogram, src.histogram)
	                        from ' || stage_table || ' as src
                            where dest.dimensions = src.dimensions
                            returning dest.*)
                  delete from ' || stage_table || ' as stage
                  using merge
                  where stage.dimensions = merge.dimensions';

    -- Insert new tuples
    execute 'insert into ' || tablename || ' (dimensions, histogram)
             select dimensions, histogram from ' || stage_table;
end
$$ language plpgsql strict;


create or replace function lock_transaction(prefix text, channel text, version text, date text) returns bigint as $$
declare
    table_name text;
    lock bigint;
begin
    table_name := aggregate_table_name(prefix, channel, version, date);
    lock := (select h_bigint(table_name));
    execute 'select pg_advisory_xact_lock($1)' using lock;
    return lock;
end
$$ language plpgsql strict;


create or replace function h_bigint(text) returns bigint as $$
    select ('x'||substr(md5($1),1,16))::bit(64)::bigint;
$$ language sql;


create or replace function create_temporary_table(prefix text, channel text, version text, date text) returns text as $$
declare
    tablename text;
begin
    tablename := aggregate_table_name('staging_' || prefix, channel, version, date);
    execute 'create temporary table ' || tablename || ' (dimensions jsonb, histogram bigint[]) on commit drop';
    return tablename;
end
$$ language plpgsql strict;


create or replace function was_processed(prefix text, channel text, version text, date text, submission_date text) returns boolean as $$
declare
    table_name text;
    was_processed boolean;
begin
    table_name := aggregate_table_name(prefix, channel, version, date);
    select exists(select 1
                  from table_update_dates as t
                  where t.tablename = table_name and submission_date = any(t.submission_dates))
                  into was_processed;

    if (was_processed) then
        return was_processed;
    end if;

    with upsert as (update table_update_dates
                    set submission_dates = submission_dates || submission_date
                    where tablename = table_name
                    returning *)
         insert into table_update_dates
         select * from (values (table_name, array[submission_date])) as t
         where not exists(select 1 from upsert);

    return was_processed;
end
$$ language plpgsql strict;


create or replace function get_metric(prefix text, channel text, version text, date text, dimensions jsonb) returns table(label text, histogram bigint[]) as $$
declare
    tablename text;
begin
    if not dimensions ? 'metric' then
        raise exception 'Missing metric field!';
    end if;

    tablename := aggregate_table_name(prefix, channel, version, date);

    return query execute
    E'select dimensions->>\'label\', aggregate_histograms(histogram)
        from ' || tablename || E'
        where dimensions @> $1
        group by dimensions->>\'label\''
        using dimensions;
end
$$ language plpgsql strict stable;


drop type if exists metric_type;
create type metric_type AS (label text, histogram bigint[]);

create or replace function batched_get_metric(prefix text, channel text, version text, dates text[], dimensions jsonb) returns table(date text, label text, histogram bigint[]) as $$
begin
    return query select t.date, (get_metric(prefix, channel, version, t.date, dimensions)::text::metric_type).*
                 from (select unnest(dates)) as t(date);
end
$$ language plpgsql strict;


create or replace function list_buildids(prefix text, channel text) returns table(version text, buildid text) as $$
begin
    return query execute
    E'select t.matches[2], t.matches[3] from
        (select regexp_matches(table_name::text, $3)
         from information_schema.tables
         where table_schema=\'public\' and table_type=\'BASE TABLE\' and table_name like $1 || $2
         order by table_name desc) as t (matches)'
       using prefix, '_' || channel || '%', '^' || prefix || '_([^_]+)_([0-9]+)_([0-9]+)$';
end
$$ language plpgsql strict;


create or replace function list_channels(prefix text) returns table(channel text) as $$
begin
    return query execute
    E'select distinct t.matches[1] from
        (select regexp_matches(table_name::text, $1 || \'_([^_]+)_([0-9]+)_([0-9]+)\')
         from information_schema.tables
         where table_schema=\'public\' and table_type=\'BASE TABLE\'
         order by table_name desc) as t (matches)'
       using prefix;
end
$$ language plpgsql strict;


create or replace function list_filter_options(prefix text, channel text, filter text) returns table(option text) as $$
declare
    last_table_name text;
begin
    execute E'select table_name
              from information_schema.tables
              where table_schema=\'public\' and table_type=\'BASE TABLE\' and table_name like $1 || $2
              order by table_name desc
              limit 1'
              into last_table_name
              using prefix, '_' || channel || '%';

     return query execute
     E'select distinct dimensions->>\'' || filter || E'\'
       from ' || last_table_name;
end
$$ language plpgsql strict stable;


create or replace function create_tables() returns void as $$
declare
    table_exists boolean;
begin
   table_exists := (select exists (select 1 from information_schema.tables where table_schema = 'public' and table_name = 'table_update_dates'));
   if (not table_exists) then
       create table table_update_dates (tablename text primary key, submission_dates text[]);
       create index on table_update_dates (tablename);
   end if;
end
$$ language plpgsql strict;


select create_tables();
    """

    cursor.execute(query)


def _get_complete_histogram(channel, metric, values):
    revision = histogram_revision_map.get(channel, "nightly")  # Use nightly revision if the channel is unknown

    if metric.startswith("SIMPLE_MEASURES"):
        histogram = pd.Series({int(k): v for k, v in values.iteritems()}, index=simple_measures_labels).fillna(0).values
    elif metric.startswith("[[COUNT]]_"):  # Count histogram
        histogram = pd.Series({int(k): v for k, v in values.iteritems()}, index=count_histogram_labels).fillna(0).values
    else:
        histogram = Histogram(metric, {"values": values}, revision=revision).get_value(autocast=False).values

    return map(long, list(histogram))


def _upsert_aggregate(stage_table, aggregate):
    key, metrics = aggregate
    submission_date, channel, version, application, architecture, os, os_version, e10s = key[:3] + key[-5:]
    dimensions = {"application": application,
                  "architecture": architecture,
                  "os": os,
                  "osVersion": os_version,
                  "e10sEnabled": e10s}

    for metric, payload in metrics.iteritems():
        metric, label, child = metric

        try:
            histogram = _get_complete_histogram(channel, metric, payload["histogram"]) + [payload["count"]]
        except KeyError:  # TODO: ignore expired histograms
            continue

        dimensions["metric"] = metric
        dimensions["label"] = label
        dimensions["child"] = child

        json_dimensions = json.dumps(dimensions)
        # json.dumps takes care of properly escaping the text but a SQL command
        # will first be interpreted as a string literal before being executed.
        # This doubles the number of backslashes we need.
        json_dimensions = json_dimensions.replace("\\", "\\\\")

        stage_table.write("{}\t{}\n".format(json_dimensions, "{" + json.dumps(histogram)[1:-1] + "}"))


def _upsert_build_id_aggregates(aggregates, dry_run=False):
    conn = create_connection(autocommit=False)
    cursor = conn.cursor()
    submission_date, channel, version, build_id = aggregates[0]

    # Aggregates with different submisssion_dates write to the same tables, we need a lock
    cursor.execute("select lock_transaction(%s, %s, %s, %s)", ("build_id", channel, version, build_id))

    cursor.execute("select was_processed(%s, %s, %s, %s, %s)", ("build_id", channel, version, build_id, submission_date))
    if cursor.fetchone()[0]:
        # This aggregate has already been processed
        conn.rollback()
        return

    stage_table = StringIO()
    cursor.execute("select create_temporary_table(%s, %s, %s, %s)", ("build_id", channel, version, build_id))
    stage_table_name = cursor.fetchone()[0]

    for aggregate in aggregates[1]:
        _upsert_aggregate(stage_table, aggregate)

    stage_table.seek(0)
    cursor.copy_from(stage_table, stage_table_name, columns=("dimensions", "histogram"))
    cursor.execute("select merge_table(%s, %s, %s, %s, %s)", ('build_id', channel, version, build_id, stage_table_name))

    if dry_run:
        conn.rollback()
    else:
        conn.commit()


def _upsert_submission_date_aggregates(aggregates, dry_run=False):
    conn = create_connection(autocommit=False)
    cursor = conn.cursor()
    submission_date, channel, version = aggregates[0]

    cursor.execute("select was_processed(%s, %s, %s, %s, %s)", ("submission_date", channel, version, submission_date, submission_date))
    if cursor.fetchone()[0]:
        # This aggregate has already been processed
        conn.rollback()
        return

    stage_table = StringIO()
    cursor.execute("select create_temporary_table(%s, %s, %s, %s)", ("submission_date", channel, version, submission_date))
    stage_table_name = cursor.fetchone()[0]

    for aggregate in aggregates[1]:
        _upsert_aggregate(stage_table, aggregate)

    stage_table.seek(0)
    cursor.copy_from(stage_table, stage_table_name, columns=("dimensions", "histogram"))
    cursor.execute("select merge_table(%s, %s, %s, %s, %s)", ("submission_date", channel, version, submission_date, stage_table_name))

    if dry_run:
        conn.rollback()
    else:
        conn.commit()


def _vacuumdb():
    conn = create_connection()
    conn.set_isolation_level(0)
    cursor = conn.cursor()
    cursor.execute("vacuum")
    conn.close()
