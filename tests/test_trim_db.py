from click.testing import CliRunner

import pytest
from dataset import (
    BUILD_ID_1,
    BUILD_ID_2,
    DATE_FMT,
    SUBMISSION_DATE_1,
    SUBMISSION_DATE_2,
    generate_pings,
)
from mozaggregator import trim_db
from mozaggregator.aggregator import _aggregate_metrics
from mozaggregator.db import clear_db, submit_aggregates


@pytest.fixture()
def aggregates(sc):
    raw_pings = list(generate_pings())
    aggregates = _aggregate_metrics(sc.parallelize(raw_pings), num_reducers=10)
    submit_aggregates(aggregates)
    return aggregates


@pytest.fixture(autouse=True)
def clear_state():
    clear_db()


@pytest.fixture()
def conn(aggregates):
    conn = trim_db.create_connection("postgres", "postgres", "pass", "db")
    yield conn
    conn.close()


def test_extract_ds_nodash():
    cases = [
        "build_id_beta_39_",
        "build_id_beta_1_1",
        "build_id_aurora_0_1000001",
        "build_id_nightly_72_20191119",
    ]
    expect = ["", "1", "1000001", "20191119"]
    actual = [trim_db.extract_ds_nodash(case) for case in cases]
    assert actual == expect


def test_retention_date_range():
    base = "20191110"
    period = 3
    buffer = 0
    expect = {"20191107", "20191108", "20191109"}
    assert trim_db.retention_date_range(base, period, buffer) == expect

    expect.add("20191110")
    assert trim_db.retention_date_range(base, period, buffer + 1) == expect


def test_query_submission_date(conn):
    cursor = conn.cursor()
    dates = [SUBMISSION_DATE_1.strftime(DATE_FMT), SUBMISSION_DATE_2.strftime(DATE_FMT)]
    retain, trim = trim_db.query_submission_date(cursor, set(dates))
    assert len(trim) == 0
    assert len(retain) > 2  # each tablename includes the dimensions
    assert {trim_db.extract_ds_nodash(table) for table in retain} == set(dates)
    assert all(["submission_date" in table for table in retain])

    retain, trim = trim_db.query_submission_date(cursor, set(dates[:1]))
    assert {trim_db.extract_ds_nodash(table) for table in retain} == set(dates[:1])
    assert {trim_db.extract_ds_nodash(table) for table in trim} == set(dates[1:])


def test_query_build_id(conn):
    cursor = conn.cursor()
    builds = [BUILD_ID_1.strftime(DATE_FMT), BUILD_ID_2.strftime(DATE_FMT)]
    retain, trim = trim_db.query_build_id(cursor, set(builds))
    assert len(trim) == 0
    assert len(retain) > 2  # each tablename includes the dimensions
    assert {trim_db.extract_ds_nodash(table) for table in retain} == set(builds)
    assert all(["build_id" in table for table in retain])

    retain, trim = trim_db.query_build_id(cursor, set(builds[:1]))
    assert {trim_db.extract_ds_nodash(table) for table in retain} == set(builds[:1])
    assert {trim_db.extract_ds_nodash(table) for table in trim} == set(builds[1:])


def test_trim_tables(conn):
    cursor = conn.cursor()
    list_tables = "select tablename from pg_catalog.pg_tables where schemaname='public'"
    cursor.execute(list_tables)
    full = {row[0] for row in cursor.fetchall()}

    dates = [SUBMISSION_DATE_1.strftime(DATE_FMT), SUBMISSION_DATE_2.strftime(DATE_FMT)]
    retain, trim = trim_db.query_submission_date(cursor, set(dates[:1]))

    expect = full - trim
    trim_db.trim_tables(conn, trim)
    conn.commit()
    cursor.execute(list_tables)
    actual = {row[0] for row in cursor.fetchall()}

    assert expect == actual


def test_trim_db_cli(conn):
    cursor = conn.cursor()
    list_tables = "select tablename from pg_catalog.pg_tables where schemaname='public'"
    cursor.execute(list_tables)
    before = {row[0] for row in cursor.fetchall()}

    result = CliRunner().invoke(
        trim_db.main,
        [
            "--base-date",
            SUBMISSION_DATE_1.strftime(DATE_FMT),
            "--retention-period",
            1,
            "--dry-run",
            "--postgres-db",
            "postgres",
            "--postgres-user",
            "postgres",
            "--postgres-pass",
            "pass",
            "--postgres-host",
            "db",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    cursor.execute(list_tables)
    after = {row[0] for row in cursor.fetchall()}
    assert before == after

    result = CliRunner().invoke(
        trim_db.main,
        [
            "--base-date",
            SUBMISSION_DATE_1.strftime(DATE_FMT),
            "--retention-period",
            1,
            "--no-dry-run",
            "--postgres-db",
            "postgres",
            "--postgres-user",
            "postgres",
            "--postgres-pass",
            "pass",
            "--postgres-host",
            "db",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    cursor.execute(list_tables)
    after = {row[0] for row in cursor.fetchall()}

    assert before != after
    assert all([SUBMISSION_DATE_2.strftime(DATE_FMT) in x for x in before - after]), (
        before - after
    )
