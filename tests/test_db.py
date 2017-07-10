import pyspark
import logging
import requests
import pandas as pd
import json
import re

from mozaggregator.constants import *
from mozaggregator.aggregator import _aggregate_metrics
from mozaggregator.db import _create_connection, submit_aggregates
from mozaggregator.service import SUBMISSION_DATE_ETAG, CLIENT_CACHE_SLACK_SECONDS
from mozaggregator import config
from dataset import *
from moztelemetry.histogram import Histogram
from nose.tools import nottest

SERVICE_URI = "http://localhost:5000"


logger = logging.getLogger("py4j")
logger.setLevel(logging.ERROR)


def setup_module():
    global aggregates
    global sc

    sc = pyspark.SparkContext(master="local[*]")
    raw_pings = list(generate_pings())
    aggregates = _aggregate_metrics(sc.parallelize(raw_pings))
    build_id_count, submission_date_count = submit_aggregates(aggregates)


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
    assert(build_id_count == n_submission_dates*n_channels*n_versions*n_build_ids)
    assert(submission_date_count == n_submission_dates*n_channels*n_versions)


def test_channels():
    channels = requests.get("{}/aggregates_by/build_id/channels/".format(SERVICE_URI)).json()
    assert(set(channels) == set(ping_dimensions["channel"]))

    channels = requests.get("{}/aggregates_by/submission_date/channels/".format(SERVICE_URI)).json()
    assert(set(channels) == set(ping_dimensions["channel"]))


def test_build_ids():
    template_channel = ping_dimensions["channel"]
    template_version = ping_dimensions["version"]
    template_build_id = ping_dimensions["build_id"]

    for channel in template_channel:
        buildids = requests.get("{}/aggregates_by/build_id/channels/{}/dates/".format(SERVICE_URI, channel)).json()
        assert(len(buildids) == len(template_version)*len(template_build_id))

        for buildid in buildids:
            assert(set(buildid.keys()) == set(["date", "version"]))
            assert(buildid["date"] in [x[:-6] for x in template_build_id])
            assert(buildid["version"] in [x.split('.')[0] for x in template_version])


def test_submission_dates():
    template_channel = ping_dimensions["channel"]
    template_version = ping_dimensions["version"]
    template_submission_date = ping_dimensions["submission_date"]

    for channel in template_channel:
        submission_dates = requests.get("{}/aggregates_by/submission_date/channels/{}/dates/".format(SERVICE_URI, channel)).json()
        assert(len(submission_dates) == len(template_version)*len(template_submission_date))

        for submission_date in submission_dates:
            assert(set(submission_date.keys()) == set(["date", "version"]))
            assert(submission_date["date"] in template_submission_date)
            assert(submission_date["version"] in [x.split('.')[0] for x in template_version])


def test_filters():
    for channel in ping_dimensions["channel"]:
        for version in [v.split('.')[0] for v in ping_dimensions["version"]]:
            options = requests.get("{}/filters/?channel={}&version={}".format(SERVICE_URI, channel, version)).json()
            #  We should really test all filters...
            assert set(options["application"]) == set(ping_dimensions["application"])
            assert set(options["architecture"]) == set(ping_dimensions["arch"])
            assert set(options["e10sEnabled"]) == set(["true", "false"])
            assert set(options["child"]) == set(["gpu", "content", "parent"])


def test_build_id_metrics():
    template_channel = ping_dimensions["channel"]
    template_version = [x.split('.')[0] for x in ping_dimensions["version"]]
    template_build_id = [x[:-6] for x in ping_dimensions["build_id"]]

    expected_count = 1
    for dimension, values in ping_dimensions.iteritems():
        if dimension not in ["channel", "version", "build_id"]:
            expected_count *= len(values)

    for channel in template_channel:
        for version in template_version:

            histogram_expected_count = NUM_PINGS_PER_DIMENSIONS * expected_count
            for metric, value in histograms_template.iteritems():
                test_histogram("build_id", channel, version, template_build_id, metric, value, histogram_expected_count)

            # Count = product(dimensions) * pings_per_dimensions
            # 1 Count for parent, then 1 Count for each NUM_CHILDREN_PER_PING
            simple_measure_expected_count = expected_count * NUM_PINGS_PER_DIMENSIONS * (NUM_CHILDREN_PER_PING + 1)

            for simple_measure, value in simple_measurements_template.iteritems():
                if not isinstance(value, int):
                    continue

                metric = "{}_{}".format(SIMPLE_MEASURES_PREFIX, simple_measure.upper())
                test_simple_measure("build_id", channel, version, template_build_id, metric, value, simple_measure_expected_count)

            # for gpu and content process, NUM_AGGREGATED_CHILD_PINGS * expected_count gets the expected number of counts
            # (we only add gpu and content scalars for aggregated child pings)
            # for parent processes, NUM_PINGS_PER_DIMENSIONS * expected_count 
            numeric_scalar_expected_count = ((2 * NUM_AGGREGATED_CHILD_PINGS) + NUM_PINGS_PER_DIMENSIONS) * expected_count

            for scalar, value in numeric_scalars_template.iteritems():
                if not isinstance(value, int):
                    continue

                metric = '{}_{}'.format(SCALARS_PREFIX, scalar.upper())
                test_numeric_scalar("build_id", channel, version, template_build_id, metric, value, numeric_scalar_expected_count)

            for metric, _dict in keyed_scalars_template.iteritems():
                metric_name = '{}_{}'.format(SCALARS_PREFIX, metric.upper())
                test_keyed_numeric_scalar("build_id", channel, version, template_build_id, metric_name, _dict, numeric_scalar_expected_count)

            for metric, histograms in keyed_histograms_template.iteritems():
                test_keyed_histogram("build_id", channel, version, template_build_id, metric, histograms, histogram_expected_count)


def test_submission_dates_metrics():
    template_channel = ping_dimensions["channel"]
    template_version = [x.split('.')[0] for x in ping_dimensions["version"]]
    template_submission_date = ping_dimensions["submission_date"]

    expected_count = 1
    for dimension, values in ping_dimensions.iteritems():
        if dimension not in ["channel", "version", "submission_date"]:
            expected_count *= len(values)

    for channel in template_channel:
        for version in template_version:

            histogram_expected_count = NUM_PINGS_PER_DIMENSIONS * expected_count
            for metric, value in histograms_template.iteritems():
                test_histogram("submission_date", channel, version, template_submission_date, metric, value, histogram_expected_count)

            # Count = product(dimensions) * pings_per_dimensions
            # 1 Count for parent, then 1 Count for each NUM_CHILDREN_PER_PING
            simple_measure_expected_count = expected_count * NUM_PINGS_PER_DIMENSIONS * (NUM_CHILDREN_PER_PING + 1)

            for simple_measure, value in simple_measurements_template.iteritems():
                if not isinstance(value, int):
                    continue

                metric = "{}_{}".format(SIMPLE_MEASURES_PREFIX, simple_measure.upper())
                test_simple_measure("submission_date", channel, version, template_submission_date, metric, value, simple_measure_expected_count)

            # for gpu and content process, NUM_AGGREGATED_CHILD_PINGS * expected_count gets the expected number of counts
            # (we only add gpu and content scalars for aggregated child pings)
            # for parent processes, NUM_PINGS_PER_DIMENSIONS * expected_count 
            numeric_scalar_expected_count = ((2 * NUM_AGGREGATED_CHILD_PINGS) + NUM_PINGS_PER_DIMENSIONS) * expected_count

            for scalar, value in numeric_scalars_template.iteritems():
                if not isinstance(value, int):
                    continue
                metric = '{}_{}'.format(SCALARS_PREFIX, scalar.upper())
                test_numeric_scalar("submission_date", channel, version, template_submission_date, metric, value, numeric_scalar_expected_count)

            for metric, _dict in keyed_scalars_template.iteritems():
                metric_name = '{}_{}'.format(SCALARS_PREFIX, metric.upper())
                test_keyed_numeric_scalar("submission_date", channel, version, template_submission_date, metric_name, _dict, numeric_scalar_expected_count)

            for metric, histograms in keyed_histograms_template.iteritems():
                test_keyed_histogram("submission_date", channel, version, template_submission_date, metric, histograms, histogram_expected_count)


def test_null_label_character_submit():
    metric_info = ("SIMPLE_MEASURES_NULL_METRIC_LABEL", u"\u0001\u0000\u0000\u0000\u7000\ub82c", False)
    payload = {"sum": 4, "count": 2, "histogram": {2: 2}}
    key = ('20161111', 'nightly', '52', '20161111', 'Firefox', 'arch', 'linux', '42', 'false')
    aggregate = (key, {metric_info: payload})

    aggregates = [sc.parallelize([aggregate]), sc.parallelize([aggregate])]
    build_id_count, submission_date_count = submit_aggregates(aggregates)

    assert build_id_count == 0, "Build id count should be 0, was {}".format(build_id_count)
    assert submission_date_count == 0, "submission date count should be 0, was {}".format(build_id_count)

def test_changed_child_value():
    # See bug 1339139
    reply = requests.get("{}/aggregates_by/submission_date/channels/nightly?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS&child=true".format(SERVICE_URI))
    assert reply.ok
    assert reply.json() is not None

def test_using_content_types():
    reply = requests.get("{}/aggregates_by/submission_date/channels/nightly?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS&child=content".format(SERVICE_URI))
    assert reply.ok
    assert reply.json() is not None

def test_using_parent_types():
    reply = requests.get("{}/aggregates_by/submission_date/channels/nightly?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS&child=parent".format(SERVICE_URI))
    assert reply.ok
    assert reply.json() is not None

def test_using_gpu_types():
    reply = requests.get("{}/aggregates_by/submission_date/channels/nightly?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS&child=gpu".format(SERVICE_URI))
    assert reply.ok
    assert reply.json() is not None

def test_new_db_functions_backwards_compatible():
    conn = _create_connection()
    cursor = conn.cursor()

    old_query = 'SELECT * FROM batched_get_metric(%s, %s, %s, %s, %s)'
    cursor.execute(old_query, ('submission_date', 'nightly', '41', ['20150603'], json.dumps({'metric': 'GC_MAX_PAUSE_MS', 'child': 'true'})))

    # Just 1 result since this is 1 date and not a keyed histogram
    assert len(cursor.fetchall()) == 1

    new_query = 'SELECT * FROM batched_get_metric(%s, %s, %s, %s, %s, %s)'
    cursor.execute(new_query, ('submission_date', 'nightly', '41', ['20150603'], json.dumps({'metric': 'GC_MAX_PAUSE_MS', 'child': 'true'}), json.dumps({'metric': 'BLOCKED_ON_PLUGIN_INSTANCE_DESTROY_MS'})))

    # 1 for the non-keyed histogram, 1 for the 1 key of the keyed histogram
    # Note we don't actually use batched_get_metric for multiple metrics,
    # but this behavior is expected
    assert len(cursor.fetchall()) == 2

def test_submission_dates_cache_control():
    reply = requests.get("{}/aggregates_by/submission_date/channels/nightly?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS".format(SERVICE_URI))
    assert reply.ok
    assert reply.headers.get('Cache-Control') == 'max-age={}'.format(config.TIMEOUT)

def test_submission_dates_etag():
    reply = requests.get("{}/aggregates_by/submission_date/channels/nightly?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS".format(SERVICE_URI))
    assert reply.ok
    assert reply.headers.get('etag').strip('"') == SUBMISSION_DATE_ETAG, "ETag expected {}, got {}".format(SUBMISSION_DATE_ETAG, reply.headers.get('etag'))

def test_submission_dates_etag_header():
    url = "{}/aggregates_by/submission_date/channels/nightly?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS".format(SERVICE_URI)
    headers = {'If-None-Match': SUBMISSION_DATE_ETAG}
    reply = requests.get(url, headers=headers)
    assert reply.ok
    assert reply.status_code == 304, "Status code expected 304, was {}".format(reply.status_code)

def test_submission_dates_etag_header_wrong():
    url = "{}/aggregates_by/submission_date/channels/nightly?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS".format(SERVICE_URI)
    headers = {'If-None-Match': SUBMISSION_DATE_ETAG + "_"}
    reply = requests.get(url, headers=headers)
    assert reply.ok
    assert reply.status_code == 200, "Status code expected 200, was {}".format(reply.status_code)

def test_build_id_cache_control():
    reply = requests.get("{}/aggregates_by/build_id/channels/nightly?version=41&dates=20150601&metric=GC_MAX_PAUSE_MS".format(SERVICE_URI))
    assert reply.ok

    matches = re.match(r'max-age=(\d+)', reply.headers.get('Cache-Control'))
    assert matches is not None, "Cache Control response not set, but should be"
    assert int(matches.group(1)) > 0, "max-age expected greater than 0, was {}".format(matches.group(1))
    assert int(matches.group(1)) < config.TIMEOUT + CLIENT_CACHE_SLACK_SECONDS, "max-age expected less than TIMEOUT"

def test_build_id_dates_no_etag():
    reply = requests.get("{}/aggregates_by/build_id/channels/nightly?version=41&dates=20150601&metric=GC_MAX_PAUSE_MS".format(SERVICE_URI))
    assert reply.ok
    assert reply.headers.get('etag') is None

def test_build_id_etag_header_ignored():
    url = "{}/aggregates_by/build_id/channels/nightly?version=41&dates=20150601&metric=GC_MAX_PAUSE_MS".format(SERVICE_URI)
    headers = {'If-None-Match': SUBMISSION_DATE_ETAG}
    reply = requests.get(url, headers=headers)
    assert reply.ok
    assert reply.status_code == 200, "Etag should be ignored for build id"


@nottest
def test_histogram(prefix, channel, version, dates, metric, value, expected_count):
    if metric.endswith("CONTENT_DOCUMENTS_DESTROYED"):  # Ignore USE_COUNTER2_ support histograms
        return

    reply = requests.get("{}/aggregates_by/{}/channels/{}?version={}&dates={}&metric={}".format(SERVICE_URI, prefix, channel, version, ",".join(dates), metric)).json()
    assert(len(reply["data"]) == len(dates))

    bucket_index = COUNT_HISTOGRAM_LABELS.index(COUNT_SCALAR_BUCKET)

    for res in reply["data"]:
        # From pings before bug 1218576 (old), `count` is the number of processes.
        # From pings after bug 1218576 (new), `count` is the number of process types.
        old_pings_expected_count = expected_count * (NUM_PINGS_PER_DIMENSIONS - NUM_AGGREGATED_CHILD_PINGS) / NUM_PINGS_PER_DIMENSIONS
        new_pings_expected_count = expected_count * NUM_AGGREGATED_CHILD_PINGS / NUM_PINGS_PER_DIMENSIONS
        assert(res["count"] == new_pings_expected_count*NUM_PROCESS_TYPES + old_pings_expected_count*(NUM_CHILDREN_PER_PING + 1))

        if value["histogram_type"] == 4:  # Count histogram
            current = pd.Series(res["histogram"], index=map(int, reply["buckets"]))
            expected = pd.Series(index=COUNT_HISTOGRAM_LABELS, data=0)
            expected[COUNT_SCALAR_BUCKET] = res["count"]

            assert(res["histogram"][bucket_index] == res["count"])
            assert(res["sum"] == value["sum"]*res["count"])
            assert((current == expected).all())
        elif metric.startswith("USE_COUNTER2_"):
            if metric.endswith("_PAGE"):
                destroyed = histograms_template["TOP_LEVEL_CONTENT_DOCUMENTS_DESTROYED"]["sum"]
            else:
                destroyed = histograms_template["CONTENT_DOCUMENTS_DESTROYED"]["sum"]
            value["values"]["0"] = destroyed - value["values"]["1"]

            current = pd.Series(res["histogram"], index=map(int, reply["buckets"]))
            expected = Histogram(metric, value).get_value()*res["count"]

            assert((current == expected).all())
            assert(res["sum"] == value["sum"]*res["count"])
        else:
            ind_type = int if value["histogram_type"] != 5 else str #categorical histograms
            current = pd.Series(res["histogram"], index=map(ind_type, reply["buckets"]))
            expected = Histogram(metric, value).get_value()*res["count"]

            assert((current == expected).all())
            assert(res["sum"] == value["sum"]*res["count"])


@nottest
def test_simple_measure(prefix, channel, version, dates, metric, value, expected_count):
    _test_numeric_scalar(prefix, channel, version, dates, metric, value, expected_count, SIMPLE_SCALAR_BUCKET, SIMPLE_MEASURES_LABELS, False)

@nottest
def test_numeric_scalar(prefix, channel, version, dates, metric, value, expected_count):
    _test_numeric_scalar(prefix, channel, version, dates, metric, value, expected_count, NUMERIC_SCALAR_BUCKET, NUMERIC_SCALARS_LABELS, True)

@nottest
def _test_numeric_scalar(prefix, channel, version, dates, metric, value, expected_count, bucket, labels, has_def):
    endpoint = "{}/aggregates_by/{}/channels/{}?version={}&dates={}&metric={}".format(SERVICE_URI, prefix, channel, version, ",".join(dates), metric)
    reply = requests.get(endpoint).json()
    assert(len(reply["data"]) == len(dates))

    assert(not has_def or reply["description"] != "")

    bucket_index = labels.index(bucket)

    for res in reply["data"]:
        assert(res["count"] == expected_count)

        current = pd.Series(res["histogram"], index=map(int, reply["buckets"]))
        expected = pd.Series(index=labels, data=0)
        expected[bucket] = res["count"]

        assert(res["histogram"][bucket_index] == res["count"])
        assert(res["sum"] == value*res["count"])
        assert((current == expected).all())


@nottest
def test_keyed_numeric_scalar(prefix, channel, version, dates, metric, histograms, expected_count):
    reply = requests.get("{}/aggregates_by/{}/channels/{}?version={}&dates={}&metric={}".format(SERVICE_URI, prefix, channel, version, ",".join(dates), metric)).json()
    assert(len(reply["data"]) == len(histograms) * len(dates))

    for label, value in histograms.iteritems():
        reply = requests.get("{}/aggregates_by/{}/channels/{}?version={}&dates={}&metric={}&label={}".format(SERVICE_URI, prefix, channel, version, ",".join(dates), metric, label.upper())).json()

        assert(reply["description"] != "")
        assert(len(reply["data"]) == len(dates))
        for res in reply["data"]:
            assert(res["count"] == expected_count)

            current = pd.Series(res["histogram"], index=map(int, reply["buckets"]))
            expected = pd.Series(index=NUMERIC_SCALARS_LABELS, data=0)
            expected[NUMERIC_SCALAR_BUCKET] = expected_count

            assert((current == expected).all())
            assert(res["sum"] == SCALAR_VALUE * res["count"])

@nottest
def test_keyed_histogram(prefix, channel, version, dates, metric, histograms, expected_count):
    reply = requests.get("{}/aggregates_by/{}/channels/{}?version={}&dates={}&metric={}".format(SERVICE_URI, prefix, channel, version, ",".join(dates), metric)).json()
    assert(len(reply["data"]) == len(histograms)*len(dates))

    for label, value in histograms.iteritems():
        reply = requests.get("{}/aggregates_by/{}/channels/{}?version={}&dates={}&metric={}&label={}".format(SERVICE_URI, prefix, channel, version, ",".join(dates), metric, label)).json()
        assert(len(reply["data"]) == len(dates))

        for res in reply["data"]:
            old_pings_expected_count = expected_count * (NUM_PINGS_PER_DIMENSIONS - NUM_AGGREGATED_CHILD_PINGS) / NUM_PINGS_PER_DIMENSIONS
            new_pings_expected_count = expected_count * NUM_AGGREGATED_CHILD_PINGS / NUM_PINGS_PER_DIMENSIONS
            assert(res["count"] == new_pings_expected_count*NUM_PROCESS_TYPES + old_pings_expected_count*(NUM_CHILDREN_PER_PING + 1))

            current = pd.Series(res["histogram"], index=map(int, reply["buckets"]))
            expected = Histogram(metric, value).get_value()*res["count"]

            assert((current == expected).all())
            assert(res["sum"] == value["sum"]*res["count"])
