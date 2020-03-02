#!/usr/bin/env python3

# pip3 install psycopg2-binary

from argparse import ArgumentParser
import os
import psycopg2
from datetime import datetime, timedelta

names = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASS"]

parser = ArgumentParser()
parser.add_argument("output")
parser.add_argument("--host", default=os.getenv("POSTGRES_HOST"))
parser.add_argument("--date", default="20201119")
args = parser.parse_args()


def yesterday(ds):
    fmt = "%Y%m%d"
    return (datetime.strptime(ds, fmt) - timedelta(1)).strftime(fmt)


conn_str = f"dbname={{}} user={{}} password={{}} host={args.host}".format(
    *[os.getenv(name) for name in names]
)

conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

cursor.execute(
    f"""
select *
from pg_catalog.pg_tables
where schemaname='public'
and tablename like 'submission_date%{args.date[:4]}%';
"""
)
rows = cursor.fetchall()
reference_period = [yesterday(args.date), args.date]
table_names = [x[1] for x in rows if x[1].split("_")[-1] in reference_period]

subquery = " UNION ALL ".join(
    f"select '{table.split('_')[-1]}' as submission_date, dimensions->>'metric' as metric, histogram from {table}"
    for table in table_names
)

# get the number of rows for each submission_date (aggregates over each dimension)
cursor.execute(
    f"""
    select
        submission_date,
        count(*)
    from ({subquery}) as unioned
    group by submission_date
"""
)
counts = list(cursor.fetchall())

# calculate the sum and counts for all histograms in a date
cursor.execute(
    f"""
    select
        submission_date,
        -- sum and count are inserted into the last two elements of a histogram
        sum(histogram[array_upper(histogram, 1)-1]) as sum_sum,
        sum(histogram[array_upper(histogram, 1)]) as sum_count
    from ({subquery}) as unioned
    group by submission_date
"""
)
sums = list(cursor.fetchall())

# Calculate the aggregates for GC_MS for each date
cursor.execute(
    f"""
    select
        submission_date,
        -- sum and count are inserted into the last two elements of a histogram
        aggregate_histograms(histogram)
    from ({subquery}) as unioned
    where metric = 'GC_MS'
    group by submission_date
"""
)
gc_ms = list(cursor.fetchall())

with open(args.output, "w") as fp:
    fp.write("from decimal import Decimal\n")
    fp.write(f"counts = {counts}\n")
    fp.write(f"sums = {sums}\n")
    fp.write(f"gc_ms = {gc_ms}\n")


# Other queries
"""
-- number of submissions in 2020
select tablename
from pg_catalog.pg_tables
where schemaname='public'
and tablename like 'submission_date%2020%';
order by tablename desc
limit 100;

-- number of submissions
select count(*)
from pg_catalog.pg_tables
where schemaname='public'
and tablename like 'submission_date%';

-- number of builds
select count(*)
from pg_catalog.pg_tables
where schemaname='public'
and tablename like 'build_id%';
"""
