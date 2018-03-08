import json
import logging

import pyspark
from dataset import generate_pings, ping_dimensions
from mozaggregator.aggregator import _aggregate_metrics
from mozaggregator.db import _create_connection, submit_aggregates

SERVICE_URI = "http://localhost:5000"


logger = logging.getLogger("py4j")
logger.setLevel(logging.ERROR)


def setup_module():
    global aggregates
    global sc

    sc = pyspark.SparkContext(master="local[*]")
    raw_pings = list(generate_pings())
    aggregates = _aggregate_metrics(sc.parallelize(raw_pings), num_reducers=10)
    submit_aggregates(aggregates)


def teardown_module():
    sc.stop()


def test_connection():
    db = _create_connection()
    assert(db)


def test_submit():
    # Multiple submissions should not alter the aggregates in the db
    build_id_count, submission_date_count = submit_aggregates(aggregates)

    n_submission_dates = len(ping_dimensions["submission_date"])
    n_channels = len(ping_dimensions["channel"])
    n_versions = len(ping_dimensions["version"])
    n_build_ids = len(ping_dimensions["build_id"])
    assert(build_id_count == n_submission_dates * n_channels * n_versions * n_build_ids)
    assert(submission_date_count == n_submission_dates * n_channels * n_versions)


def test_null_label_character_submit():
    metric_info = ("SIMPLE_MEASURES_NULL_METRIC_LABEL", u"\u0001\u0000\u0000\u0000\u7000\ub82c", False)
    payload = {"sum": 4, "count": 2, "histogram": {2: 2}}
    key = ('20161111', 'nightly', '52', '20161111', 'Firefox', 'arch', 'linux', '42', 'false')
    aggregate = (key, {metric_info: payload})

    aggregates = [sc.parallelize([aggregate]), sc.parallelize([aggregate])]
    build_id_count, submission_date_count = submit_aggregates(aggregates)

    assert build_id_count == 0, "Build id count should be 0, was {}".format(build_id_count)
    assert submission_date_count == 0, "submission date count should be 0, was {}".format(build_id_count)


def test_new_db_functions_backwards_compatible():
    conn = _create_connection()
    cursor = conn.cursor()

    old_query = 'SELECT * FROM batched_get_metric(%s, %s, %s, %s, %s)'
    cursor.execute(old_query, ('submission_date', 'nightly', '41', ['20150603'], json.dumps({'metric': 'GC_MAX_PAUSE_MS_2', 'child': 'true'})))

    # Just 1 result since this is 1 date and not a keyed histogram
    result = cursor.fetchall()
    assert len(result) == 1, result

    new_query = 'SELECT * FROM batched_get_metric(%s, %s, %s, %s, %s, %s)'
    cursor.execute(new_query, ('submission_date', 'nightly', '41', ['20150603'], json.dumps({'metric': 'GC_MAX_PAUSE_MS_2', 'child': 'true'}), json.dumps({'metric': 'DEVTOOLS_PERFTOOLS_RECORDING_FEATURES_USED'})))

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
