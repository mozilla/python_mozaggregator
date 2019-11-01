import json
import logging
import os

from click.testing import CliRunner

import pytest
from dataset import (DATE_FMT, SUBMISSION_DATE_1, generate_pings,
                     ping_dimensions)
from mozaggregator.aggregator import _aggregate_metrics
from mozaggregator.cli import run_aggregator
from mozaggregator.db import (NoticeLoggingCursor, _create_connection,
                              submit_aggregates, clear_db)
from testfixtures import LogCapture
from utils import runif_bigquery_testing_enabled

SERVICE_URI = "http://localhost:5000"


logger = logging.getLogger("py4j")
logger.setLevel(logging.INFO)


@pytest.fixture()
def aggregates(sc):
    raw_pings = list(generate_pings())
    aggregates = _aggregate_metrics(sc.parallelize(raw_pings), num_reducers=10)
    submit_aggregates(aggregates)
    return aggregates


@pytest.fixture(autouse=True)
def clear_state():
    clear_db()


def test_connection():
    db = _create_connection()
    assert(db)


def test_submit(aggregates):
    # Multiple submissions should not alter the aggregates in the db
    build_id_count, submission_date_count = submit_aggregates(aggregates)

    n_submission_dates = len(ping_dimensions["submission_date"])
    n_channels = len(ping_dimensions["channel"])
    n_versions = len(ping_dimensions["version"])
    n_build_ids = len(ping_dimensions["build_id"])
    assert(build_id_count == n_submission_dates * n_channels * n_versions * n_build_ids)
    assert(submission_date_count == n_submission_dates * n_channels * n_versions)


def test_null_label_character_submit(sc):
    metric_info = ("SIMPLE_MEASURES_NULL_METRIC_LABEL", "\u0001\u0000\u0000\u0000\u7000\ub82c", False)
    payload = {"sum": 4, "count": 2, "histogram": {2: 2}}
    key = ('20161111', 'nightly', '52', '20161111', '', 'Firefox', 'arch', 'Windows', '2.4.21')
    aggregate = (key, {metric_info: payload})

    aggregates = [sc.parallelize([aggregate]), sc.parallelize([aggregate])]
    build_id_count, submission_date_count = submit_aggregates(aggregates)

    assert build_id_count == 0, "Build id count should be 0, was {}".format(build_id_count)
    assert submission_date_count == 0, "submission date count should be 0, was {}".format(build_id_count)


def test_null_arch_character_submit(sc):
    metric_info = ("SIMPLE_MEASURES_NULL_ARCHITECTURE", "", False)
    payload = {"sum": 4, "count": 2, "histogram": {2: 2}}
    key = ('20161111', 'nightly', '52', '20161111', '', "Firefox", "\x00", 'Windows', '2.4.21')
    aggregate = (key, {metric_info: payload})

    aggregates = [sc.parallelize([aggregate]), sc.parallelize([aggregate])]
    build_id_count, submission_date_count = submit_aggregates(aggregates)

    assert build_id_count == 0, "Build id count should be 0, was {}".format(build_id_count)
    assert submission_date_count == 0, "submission date count should be 0, was {}".format(build_id_count)


def test_new_db_functions_backwards_compatible(aggregates):
    conn = _create_connection()
    cursor = conn.cursor()

    old_query = 'SELECT * FROM batched_get_metric(%s, %s, %s, %s, %s)'
    cursor.execute(old_query, (
        'submission_date', 'nightly', '41', [SUBMISSION_DATE_1.strftime(DATE_FMT)],
        json.dumps({'metric': 'GC_MAX_PAUSE_MS_2', 'child': 'true'})))

    # Just 1 result since this is 1 date and not a keyed histogram
    result = cursor.fetchall()
    assert len(result) == 1, result

    new_query = 'SELECT * FROM batched_get_metric(%s, %s, %s, %s, %s, %s)'
    cursor.execute(new_query, (
        'submission_date', 'nightly', '41', [SUBMISSION_DATE_1.strftime(DATE_FMT)],
        json.dumps({'metric': 'GC_MAX_PAUSE_MS_2', 'child': 'true'}),
        json.dumps({'metric': 'DEVTOOLS_PERFTOOLS_RECORDING_FEATURES_USED'})))

    # 1 for the non-keyed histogram, 1 for the 1 key of the keyed histogram
    # Note we don't actually use batched_get_metric for multiple metrics,
    # but this behavior is expected
    assert len(cursor.fetchall()) == 2


def test_aggregate_histograms():
    conn = _create_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT aggregate_histograms(t.histos) AS aggregates
        FROM (VALUES (ARRAY[1,1,1,1]), (ARRAY[1,1,1,1,1])) AS t(histos)
    """)
    res = cursor.fetchall()
    assert res == [([2, 2, 1, 2, 2],)]


def test_cast_array_to_bigint():
    conn = _create_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT cast_array_to_bigint_safe(ARRAY[-9223372036854775809, 9223372036854775808, 12]);")
    res = cursor.fetchall()
    assert res == [([-9223372036854775808, 9223372036854775807, 12],)]


def test_notice_logging_cursor():
    conn = _create_connection()
    cursor = conn.cursor(cursor_factory=NoticeLoggingCursor)
    expected = ('py4j',
                'WARNING',
                'WARNING:  Truncating positive value(s) too large for bigint in array: {9223372036854775808}')
    with LogCapture("py4j") as lc:
        cursor.execute("SELECT cast_array_to_bigint_safe(ARRAY[9223372036854775808]);")
    lc.check(expected)


def test_aggregation_cli(tmp_path, monkeypatch, spark):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")

    test_creds = str(tmp_path / "creds")
    # generally points to the production credentials
    creds = {"DB_TEST_URL": "dbname=postgres user=postgres host=db"}
    with open(test_creds, "w") as f:
        json.dump(creds, f)

    class Dataset:
        @staticmethod
        def from_source(*args, **kwargs):
            return Dataset()

        def where(self, *args, **kwargs):
            return self

        def records(self, *args, **kwargs):
            return spark.sparkContext.parallelize(generate_pings())

    monkeypatch.setattr("mozaggregator.aggregator.Dataset", Dataset)

    result = CliRunner().invoke(
        run_aggregator,
        [
            "--date",
            SUBMISSION_DATE_1.strftime('%Y%m%d'),
            "--channels",
            "nightly,beta",
            "--credentials-protocol",
            "file",
            "--credentials-bucket",
            "/",
            "--credentials-prefix",
            test_creds,
            "--num-partitions",
            10,
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output


@runif_bigquery_testing_enabled
def test_aggregation_cli_bigquery(tmp_path, monkeypatch, spark, bq_testing_table):
    test_creds = str(tmp_path / "creds")
    # generally points to the production credentials
    creds = {"DB_TEST_URL": "dbname=postgres user=postgres host=db"}
    with open(test_creds, "w") as f:
        json.dump(creds, f)

    result = CliRunner().invoke(
        run_aggregator,
        [
            "--date",
            SUBMISSION_DATE_1.strftime('%Y%m%d'),
            "--channels",
            "nightly,beta",
            "--credentials-protocol",
            "file",
            "--credentials-bucket",
            "/",
            "--credentials-prefix",
            test_creds,
            "--num-partitions",
            10,
            "--source",
            "bigquery",
            "--project-id",
            os.environ["PROJECT_ID"],
            "--dataset-id",
            "pytest_mozaggregator_test"
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
