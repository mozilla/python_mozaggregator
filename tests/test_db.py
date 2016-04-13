import pyspark
import logging
import requests
import pandas as pd

from mozaggregator.aggregator import _aggregate_metrics, count_histogram_labels, simple_measures_labels
from mozaggregator.db import _create_connection, submit_aggregates
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
            assert set(options["child"]) == set(["true", "false"])


def test_build_id_metrics():
    template_channel = ping_dimensions["channel"]
    template_version = [x.split('.')[0] for x in ping_dimensions["version"]]
    template_build_id = [x[:-6] for x in ping_dimensions["build_id"]]

    expected_count = NUM_PINGS_PER_DIMENSIONS
    for dimension, values in ping_dimensions.iteritems():
        if dimension not in ["channel", "version", "build_id"]:
            expected_count *= len(values)

    for channel in template_channel:
        for version in template_version:
            for metric, value in histograms_template.iteritems():
                test_histogram("build_id", channel, version, template_build_id, metric, value, expected_count)

                for simple_measure, value in simple_measurements_template.iteritems():
                    if not isinstance(value, int):
                        continue

                    metric = "SIMPLE_MEASURES_{}".format(simple_measure.upper())
                    test_simple_measure("build_id", channel, version, template_build_id, metric, value, expected_count)

                for metric, histograms in keyed_histograms_template.iteritems():
                    test_keyed_histogram("build_id", channel, version, template_build_id, metric, histograms, expected_count)


def test_submission_dates_metrics():
    template_channel = ping_dimensions["channel"]
    template_version = [x.split('.')[0] for x in ping_dimensions["version"]]
    template_submission_date = ping_dimensions["submission_date"]

    expected_count = NUM_PINGS_PER_DIMENSIONS
    for dimension, values in ping_dimensions.iteritems():
        if dimension not in ["channel", "version", "submission_date"]:
            expected_count *= len(values)

    for channel in template_channel:
        for version in template_version:
            for metric, value in histograms_template.iteritems():
                test_histogram("submission_date", channel, version, template_submission_date, metric, value, expected_count)

            for simple_measure, value in simple_measurements_template.iteritems():
                if not isinstance(value, int):
                    continue

                metric = "SIMPLE_MEASURES_{}".format(simple_measure.upper())
                test_simple_measure("submission_date", channel, version, template_submission_date, metric, value, expected_count)

            for metric, histograms in keyed_histograms_template.iteritems():
                test_keyed_histogram("submission_date", channel, version, template_submission_date, metric, histograms, expected_count)


@nottest
def test_histogram(prefix, channel, version, dates, metric, value, expected_count):
    if metric.endswith("CONTENT_DOCUMENTS_DESTROYED"):  # Ignore USE_COUNTER2_ support histograms
        return

    reply = requests.get("{}/aggregates_by/{}/channels/{}?version={}&dates={}&metric={}".format(SERVICE_URI, prefix, channel, version, ",".join(dates), metric)).json()
    assert(len(reply["data"]) == len(dates))

    bucket_index = count_histogram_labels.index(COUNT_SCALAR_BUCKET)

    for res in reply["data"]:
        assert(res["count"] == expected_count*(NUM_CHILDREN_PER_PING + 1))

        if value["histogram_type"] == 4:  # Count histogram
            current = pd.Series(res["histogram"], index=map(int, reply["buckets"]))
            expected = pd.Series(index=count_histogram_labels, data=0)
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
            current = pd.Series(res["histogram"], index=map(int, reply["buckets"]))
            expected = Histogram(metric, value).get_value()*res["count"]

            assert((current == expected).all())
            assert(res["sum"] == value["sum"]*res["count"])


@nottest
def test_simple_measure(prefix, channel, version, dates, metric, value, expected_count):
    reply = requests.get("{}/aggregates_by/{}/channels/{}?version={}&dates={}&metric={}".format(SERVICE_URI, prefix, channel, version, ",".join(dates), metric)).json()
    assert(len(reply["data"]) == len(dates))

    bucket_index = simple_measures_labels.index(SIMPLE_SCALAR_BUCKET)

    for res in reply["data"]:
        assert(res["count"] == expected_count*(NUM_CHILDREN_PER_PING + 1))

        current = pd.Series(res["histogram"], index=map(int, reply["buckets"]))
        expected = pd.Series(index=simple_measures_labels, data=0)
        expected[SIMPLE_SCALAR_BUCKET] = res["count"]

        assert(res["histogram"][bucket_index] == res["count"])
        assert(res["sum"] == value*res["count"])
        assert((current == expected).all())


@nottest
def test_keyed_histogram(prefix, channel, version, dates, metric, histograms, expected_count):
    reply = requests.get("{}/aggregates_by/{}/channels/{}?version={}&dates={}&metric={}".format(SERVICE_URI, prefix, channel, version, ",".join(dates), metric)).json()
    assert(len(reply["data"]) == len(histograms)*len(dates))

    for label, value in histograms.iteritems():
        reply = requests.get("{}/aggregates_by/{}/channels/{}?version={}&dates={}&metric={}&label={}".format(SERVICE_URI, prefix, channel, version, ",".join(dates), metric, label)).json()
        assert(len(reply["data"]) == len(dates))

        for res in reply["data"]:
            assert(res["count"] == expected_count*(NUM_CHILDREN_PER_PING + 1))

            current = pd.Series(res["histogram"], index=map(int, reply["buckets"]))
            expected = Histogram(metric, value).get_value()*res["count"]

            assert((current == expected).all())
            assert(res["sum"] == value["sum"]*res["count"])
