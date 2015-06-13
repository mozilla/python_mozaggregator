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
import uuid

from moztelemetry.spark import Histogram
from boto.s3.connection import S3Connection
from cStringIO import StringIO

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

    count = aggregates.groupBy(lambda x: x[0][:4]).\
               map(lambda x: _upsert_aggregates_fast(x, dry_run=dry_run)).\
               count()

    _vacuumdb()
    return count


def _preparedb():
    conn = create_connection()
    cursor = conn.cursor()
    query = """
create or replace function aggregate_arrays(acc bigint[], x bigint[]) returns bigint[] as $$
declare
    i int;
begin
    for i in 1 .. array_length(x, 1)
    loop
        acc[i] := coalesce(acc[i], 0) + x[i];
    end loop;
    return acc;
end
$$ language plpgsql strict immutable;


drop aggregate if exists aggregate_histograms(bigint[]);
create aggregate aggregate_histograms (bigint[]) (
    sfunc = aggregate_arrays, stype = bigint[], initcond = '{}'
);


create or replace function add_buildid_metric(channel text, version text, buildid text, dimensions jsonb, histogram bigint[]) returns void as $$
declare
    tablename text;
    table_exists bool;
    temporary text;
begin
    tablename := channel || '_' || version || '_' || buildid;
    -- Check if table exists and if not create one
    table_exists := (select exists (select 1 from information_schema.tables where table_schema = 'public' and table_name = tablename));

    if not table_exists then
        execute 'create table ' || tablename || '(id serial primary key, dimensions jsonb, histogram bigint[])';
        execute 'create index on ' || tablename || ' using GIN (dimensions jsonb_path_ops)';
    end if;

    -- Check if the document already exists and update it, if not create one
    execute 'with upsert as (update ' || tablename || ' as t
                             set histogram = (select aggregate_histograms(v) from (values (1, t.histogram), (2, $1)) as t (k, v))
                             where t.dimensions @> $2
                             returning t.*)
             insert into ' || tablename || ' (dimensions, histogram)
             select * from (values ($2, $1)) as t
             where not exists (select 1 from upsert)'
             using histogram, dimensions;
end
$$ language plpgsql strict;


create or replace function merge_buildid_table(channel text, version text, buildid text, stage_table regclass) returns void as $$
declare
    tablename text;
    table_exists bool;
begin
    tablename := channel || '_' || version || '_' || buildid;
    -- Check if table exists and if not create one
    table_exists := (select exists (select 1 from information_schema.tables where table_schema = 'public' and table_name = tablename));

    if not table_exists then
        execute format('create table %s as table %s', tablename, stage_table);
        execute format('create index on %s using GIN (dimensions jsonb_path_ops)', stage_table);
        return;
    end if;

    -- Update existing tuples and delete matching rows from the staging table
    execute 'with merge as (update ' || tablename || ' as dest
	                    set histogram = (select aggregate_histograms(v) from (values (1, dest.histogram), (2, src.histogram)) as t (k, v))
	                    from ' || stage_table || ' as src
                            where dest.dimensions @> src.dimensions
                            returning dest.*)
                  delete from ' || stage_table || ' as stage
                  using merge
                  where stage.dimensions @> merge.dimensions';

    -- Insert new tuples
    execute 'insert into ' || tablename || ' (dimensions, histogram)
             select dimensions, histogram from ' || stage_table;
end
$$ language plpgsql strict;


create or replace function lock_buildid_transaction(channel text, version text, buildid text) returns bigint as $$
declare
    table_name text;
    lock bigint;
begin
    table_name := channel || '_' || version || '_' || buildid;
    lock := (select h_bigint(table_name));
    execute 'select pg_advisory_xact_lock($1)' using lock;
    return lock;
end
$$ language plpgsql strict;


create function h_bigint(text) returns bigint as $$
    select ('x'||substr(md5($1),1,16))::bit(64)::bigint;
$$ language sql;


create or replace function create_temporary_buildid_table(channel text, version text, buildid text) returns text as $$
declare
    tablename text;
begin
    tablename := format('%s_%s_%s_staging', channel, version,  buildid);
    execute 'create temporary table ' || tablename || ' (id serial primary key, dimensions jsonb, histogram bigint[]) on commit drop';
    return tablename;
end
$$ language plpgsql strict;

create or replace function was_buildid_processed(channel text, version text, buildid text, submission_date text) returns boolean as $$
declare
    table_name text;
    was_processed boolean;
begin
    table_name := channel || '_' || version || '_' || buildid;
    select exists(select 1
                  from buildid_update_dates as t
                  where t.tablename = table_name and submission_date = any(t.submission_dates))
                  into was_processed;

    if (was_processed) then
        return was_processed;
    end if;

    with upsert as (update buildid_update_dates
                    set submission_dates = submission_dates || submission_date
                    where tablename = table_name
                    returning *)
         insert into buildid_update_dates
         select * from (values (table_name, array[submission_date])) as t
         where not exists(select 1 from upsert);

    return was_processed;
end
$$ language plpgsql strict;


create or replace function get_buildid_metric(channel text, version text, buildid text, dimensions jsonb) returns table(label text, histogram bigint[]) as $$
declare
    tablename text;
begin
    if not dimensions ? 'metric' then
        raise exception 'Missing metric field!';
    end if;

    tablename := channel || '_' || version || '_' || buildid;

    return query execute
    E'select dimensions->>\\'label\\', aggregate_histograms(histogram)
        from ' || tablename || E'
        where dimensions @> $1
        group by dimensions->>\\'label\\''
        using dimensions;
end
$$ language plpgsql strict immutable;


create or replace function list_buildids(channel text) returns table(version text, buildid text) as $$
begin
    return query execute
    E'select t.matches[2], t.matches[3] from
        (select regexp_matches(table_name::text, \\'([^_]*)_([0-9]*)_([0-9]*)\\')
         from information_schema.tables
         where table_schema=\\'public\\' and table_type=\\'BASE TABLE\\' and table_name like \\'' || channel || E'%\\'
         order by table_name desc) as t (matches)';
end
$$ language plpgsql strict;

create or replace function list_channels() returns table(channel text) as $$
begin
    return query execute
    E'select distinct t.matches[1] from
        (select regexp_matches(table_name::text, \\'([^_]*)_([0-9]*)_([0-9]*)\\')
         from information_schema.tables
         where table_schema=\\'public\\' and table_type=\\'BASE TABLE\\'
         order by table_name desc) as t (matches)';
end
$$ language plpgsql strict;


create or replace function create_tables() returns void as $$
declare
    table_exists boolean;
begin
   table_exists := (select exists (select 1 from information_schema.tables where table_schema = 'public' and table_name = 'buildid_update_dates'));
   if (not table_exists) then
       create table buildid_update_dates (tablename text primary key, submission_dates text[]);
       create index on buildid_update_dates (tablename);
   end if;
end
$$ language plpgsql strict;

select create_tables();

-- Example usage:
-- select get_buildid_metric('nightly', '41', '20150527', '{"metric": "JS_TELEMETRY_ADDON_EXCEPTIONS"}'::jsonb);
    """

    cursor.execute(query)


def _get_complete_histogram(channel, metric, values):
    revision = histogram_revision_map.get(channel, "nightly")  # Use nightly revision if the channel is unknown

    if metric.endswith("_SCALAR"):
        histogram = pd.Series(values).values  # histogram is already complete
        metric = metric[:-7]
    else:
        histogram = Histogram(metric, {"values": values}, revision=revision).get_value(autocast=False).values

    return metric, map(int, list(histogram))


def _upsert_aggregate_fast(stage_table, aggregate):
    key, metrics = aggregate
    submission_date, channel, version, build_id, application, architecture, os, os_version, e10s = key
    dimensions = {"application": application,
                  "architecture": architecture,
                  "os": os,
                  "os_version": os_version,
                  "e10sEnabled": e10s}

    for metric, payload in metrics.iteritems():
        metric, label, child = metric
        label = label.replace("'", "")  # Postgres doesn't like quotes
        label = label.replace("\t", "")  # Tab is used as separator in the table

        try:
            metric, histogram = _get_complete_histogram(channel, metric, payload["histogram"])
            histogram += [payload["count"]]
        except KeyError:  # TODO: ignore expired histograms
            continue

        dimensions["metric"] = metric
        dimensions["label"] = label
        dimensions["child"] = child

        stage_table.write("{}\t{}\n".format(json.dumps(dimensions), "{" + repr(histogram)[1:-1] + "}"))


def _upsert_aggregate(cursor, aggregate):
    key, metrics = aggregate
    submission_date, channel, version, build_id, application, architecture, os, os_version, e10s = key

    dimensions = {"application": application,
                  "architecture": architecture,
                  "os": os,
                  "os_version": os_version,
                  "e10sEnabled": e10s}

    for metric, payload in metrics.iteritems():
        metric, label, child = metric
        label = label.replace("'", "")  # Postgres doesn't like quotes

        try:
            metric, histogram = _get_complete_histogram(channel, metric, payload["histogram"])
            histogram += [payload["count"]]
        except KeyError:  # TODO: ignore expired histograms
            continue

        dimensions["metric"] = metric
        dimensions["label"] = label
        dimensions["child"] = child

        cursor.execute("select add_buildid_metric(%s, %s, %s, %s, %s)", (channel, version, build_id, json.dumps(dimensions), histogram))

def _upsert_aggregates_fast(aggregates, dry_run=False):
    conn = create_connection(autocommit=False)
    cursor = conn.cursor()
    submission_date, channel, version, build_id = aggregates[0]

    # Aggregates with different submisssion_dates write to the same tables
    cursor.execute("select lock_buildid_transaction(%s, %s, %s)", (channel, version, build_id))

    cursor.execute("select was_buildid_processed(%s, %s, %s, %s)", (channel, version, build_id, submission_date))
    if cursor.fetchone()[0]:
        # This aggregate has already been processed
        conn.rollback()
        return

    stage_table = StringIO()
    cursor.execute("select create_temporary_buildid_table(%s, %s, %s)", (channel, version, build_id))
    stage_table_name = cursor.fetchone()[0]

    for aggregate in aggregates[1]:
        _upsert_aggregate_fast(stage_table, aggregate)

    stage_table.seek(0)
    cursor.copy_from(stage_table, stage_table_name, columns=("dimensions", "histogram"))
    cursor.execute("select merge_buildid_table(%s, %s, %s, %s)", (channel, version, build_id, stage_table_name))

    if dry_run:
        conn.rollback()
    else:
        conn.commit()


def _upsert_aggregates(aggregates, dry_run=False):
    conn = create_connection(autocommit=False)
    cursor = conn.cursor()
    submission_date, channel, version, build_id = aggregates[0]

    while(True):
        try:
            cursor.execute(u"select was_buildid_processed(%s, %s, %s, %s)", (channel, version, build_id, submission_date))
            if cursor.fetchone()[0]:
                # This aggregate has already been processed
                return
            else:
                break
        except psycopg2.IntegrityError:  # Multiple aggregates from different submission dates might try to insert at the same time
            conn.rollback()  # Transaction is aborted after an error
            continue

    for aggregate in aggregates[1]:
        _upsert_aggregate(cursor, aggregate)

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
