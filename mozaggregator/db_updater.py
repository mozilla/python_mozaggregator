#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import psycopg2
import pandas as pd
import ujson as json
import argparse

from datetime import datetime
from moztelemetry.spark import Histogram


def preparedb(conn, cursor):
    query = """
create or replace function aggregate_arrays(acc int[], x jsonb) returns int[] as $$
declare
   i int;
   tmp int;
begin
for i in 0 .. json_array_length(x::json) - 1
loop
   if acc[i + 1] is NULL then
       acc[i + 1] = 0;
   end if;
   tmp := x->i;
   acc[i + 1] := acc[i + 1] + tmp;
end loop;
return acc;
end
$$ language plpgsql strict immutable;

drop aggregate if exists aggregate_histograms(jsonb);
create aggregate aggregate_histograms ( jsonb ) (
    sfunc = aggregate_arrays, stype = int[], initcond = '{}'
);

create or replace function add_buildid_metric(channel text, version text, buildid text, dimensions jsonb, histogram jsonb) returns void as $$
declare
    tablename text;
    table_exists bool;
    temporary text;
begin
    tablename := channel || '_' || version || '_' || buildid;
    -- Check if table exists and if not create one
    table_exists := (select exists (select 1 from information_schema.tables where table_schema = 'public' and table_name = tablename));
    if not table_exists then
        execute 'create table ' || tablename || '(id serial primary key) inherits (telemetry_aggregates_buildid)';
        execute 'create index on ' || tablename || ' using GIN (dimensions jsonb_path_ops)';
    end if;
    -- Check if the document already exists and update it, if not create one
    execute 'with upsert as (update ' || tablename || ' as t
                             set histogram = array_to_json((select aggregate_histograms(v) from (values (1, t.histogram), (2, $1)) as t (k, v)))::jsonb
                             where t.dimensions @> $2
                             returning t.*)
             insert into ' || tablename || ' (dimensions, histogram)
                    select * from (values ($2, $1)) as t
                    where not exists (select 1 from upsert)'
             using histogram, dimensions;
end
$$ language plpgsql strict;

create table if not exists telemetry_aggregates_buildid (dimensions jsonb, histogram jsonb);
    """

    cursor.execute(query)
    conn.commit()


def get_complete_histogram(metric, values):
    if metric.startswith("SIMPLE_"):
        histogram = values  # histogram is already complete
    else:
        histogram = Histogram(metric, {"values": values}).get_value(autocast=False).values

    return map(int, list(histogram))


def transform_entry(row):
    entry = row.to_dict()
    entry["histogram"] = get_complete_histogram(row["metric"], row["histogram"])
    entry["child"] = bool(row["child"])
    entry["label"] = row["label"].replace("'", "");  # Postgres doesn't like quotes
    return entry


def updatedb(conn, frame, max_entries=None):
    cursor = conn.cursor()
    preparedb(conn, cursor)

    beginning = datetime.now()
    batch_start = beginning
    processed = 0

    for i in range(len(frame) if not max_entries else min(max_entries, len(frame))):
        try:
            entry = transform_entry(frame.iloc[i])
        except KeyError as e:
            continue

        processed += 1
        channel = entry.pop("channel")
        version = entry.pop("version")
        build_id = entry.pop("build_id")
        histogram = json.dumps(entry.pop("histogram"))
        dimensions = json.dumps(entry)

        cursor.execute("select add_buildid_metric('{}', '{}', '{}', '{}', '{}')".
                    format(channel, version, build_id, dimensions, histogram))

        if processed % 10000 == 0 and processed != 0:
            now = datetime.now()
            print "10K entries updated in {}s - current total {}".format((now - batch_start).seconds, processed)
            batch_start = now

    conn.commit()
    print "All done in {}, total entries added {}".format((datetime.now() - beginning), processed)


def process_build_id(cursor, aggregate):    
    key, metrics = aggregate
    channel, version, build_id, application, architecture, revision, os, os_version = key
    
    dimensions = {"application": application,
                  "architecture": architecture,
                  "revision": revision,
                  "os": os,
                  "os_version": os_version}
    
    for metric, payload in metrics.iteritems():
        metric, label, child = metric
        label = label.replace("'", ""); # Postgres doesn't like quotes
        
        dimensions["metric"] = metric
        dimensions["label"] = label
        dimensions["child"] = child
        
        try:
            histogram = get_complete_histogram(metric, payload["histogram"]) + [payload["count"]]  # Append count at the end
        except KeyError as e:  # TODO: use revision service once it's ready 
            continue
        
    
        cursor.execute("select add_buildid_metric('{}', '{}', '{}', '{}', '{}')".format(channel,
                                                                                        version,
                                                                                        build_id,
                                                                                        json.dumps(dimensions),
                                                                                        json.dumps(histogram)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database updater utitily.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-i", "--input", help="Pandas filename containing the aggregates", required=True)
    parser.add_argument("-u", "--user", help="Postgres username", default="root")
    parser.add_argument("-p", "--password", help="Postgres password", required=True)
    parser.add_argument("-d", "--database", help="Postgres database", default="telemetry")
    parser.add_argument("-o", "--host", help="Postgres host", required=True)
    parser.add_argument("-l", "--limit", help="Updates limit", default=None)
    args = parser.parse_args()

    conn = psycopg2.connect(dbname=args.database, user=args.user, password=args.password, host=args.host)
    frame = pd.read_json(args.input)
    updatedb(conn, frame, int(args.limit) if args.limit else None)
