import pyspark
import logging
import pandas as pd

from mozaggregator.aggregator import _aggregate_metrics
from collections import defaultdict
from dataset import *


def setup_module():
    global build_id_aggregates
    global submission_date_aggregates

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)

    sc = pyspark.SparkContext(master="local[*]")
    raw_pings = list(generate_pings())
    build_id_aggregates, submission_date_aggregates = _aggregate_metrics(sc.parallelize(raw_pings))
    build_id_aggregates = build_id_aggregates.collect()
    submission_date_aggregates = submission_date_aggregates.collect()

    # Note: most tests are based on the build-id aggregates as the aggregation
    # code is the same for both scenarios.

    sc.stop()


def test_count():
    num_build_ids = len(ping_dimensions["build_id"])
    assert(len(list(generate_pings()))/NUM_PINGS_PER_DIMENSIONS == len(build_id_aggregates))
    assert(len(list(generate_pings()))/NUM_PINGS_PER_DIMENSIONS/num_build_ids == len(submission_date_aggregates))


def test_keys():
    for aggregate in build_id_aggregates:
        submission_date, channel, version, build_id, app, arch, os, os_version, e10s = aggregate[0]

        assert(submission_date in ping_dimensions["submission_date"])
        assert(channel in ping_dimensions["channel"])
        assert(version in [x.split('.')[0] for x in ping_dimensions["version"]])
        assert(build_id in [x[:8] for x in ping_dimensions["build_id"]])
        assert(app in ping_dimensions["application"])
        assert(arch in ping_dimensions["arch"])
        assert(os in ping_dimensions["os"])
        if os == "Linux":
            assert(os_version in [x[:3] for x in ping_dimensions["os_version"]])
        else:
            assert(os_version in ping_dimensions["os_version"])

    for aggregate in submission_date_aggregates:
        submission_date, channel, version, app, arch, os, os_version, e10s = aggregate[0]

        assert(submission_date in ping_dimensions["submission_date"])
        assert(channel in ping_dimensions["channel"])
        assert(version in [x.split('.')[0] for x in ping_dimensions["version"]])
        assert(app in ping_dimensions["application"])
        assert(arch in ping_dimensions["arch"])
        assert(os in ping_dimensions["os"])
        if os == "Linux":
            assert(os_version in [x[:3] for x in ping_dimensions["os_version"]])
        else:
            assert(os_version in ping_dimensions["os_version"])


def test_simple_measurements():
    metric_count = defaultdict(int)

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key

            if metric.startswith("SIMPLE_MEASURES_"):
                metric_count[metric] += 1
                assert(label == "")
                # simple measurements are still in childPayloads
                assert(value["count"] == (NUM_CHILDREN_PER_PING if child else 1) * NUM_PINGS_PER_DIMENSIONS)
                assert(value["sum"] == value["count"]*SCALAR_VALUE)
                assert(value["histogram"][str(SIMPLE_SCALAR_BUCKET)] == value["count"])

    assert len(metric_count) == len(simple_measurements_template)
    for v in metric_count.values():
        assert(v == 2*len(build_id_aggregates)) # Count both child and parent metrics


def test_classic_histograms():
    metric_count = defaultdict(int)
    histograms = {k: v for k, v in histograms_template.iteritems() if v["histogram_type"] != 4 and not k.startswith("USE_COUNTER2_")}

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key
            histogram = histograms.get(metric, None)

            if histogram:
                metric_count[metric] += 1
                assert(label == "")
                if value["count"] != expected_count(child):
                  print "metric %s child %s count %d expected_count %d" % (metric, child, count, expected_count(child))
                assert(value["count"] == expected_count(child))
                assert(value["sum"] == value["count"]*histogram["sum"])
                assert(set(histogram["values"].keys()) == set(value["histogram"].keys()))
                assert((pd.Series(histogram["values"])*value["count"] == pd.Series(value["histogram"])).all())

    assert(len(metric_count) == len(histograms))
    for v in metric_count.values():
        assert(v == 2*len(build_id_aggregates))  # Count both child and parent metrics


def test_count_histograms():
    metric_count = defaultdict(int)
    histograms = {"[[COUNT]]_{}".format(k): v for k, v in histograms_template.iteritems() if v["histogram_type"] == 4 and not k.endswith("CONTENT_DOCUMENTS_DESTROYED")}

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key
            histogram = histograms.get(metric, None)

            if histogram:
                metric_count[metric] += 1
                assert(label == "")
                if value["count"] != expected_count(child):
                  print "metric %s child %s count %d expected_count %d" % (metric, child, count, expected_count(child))
                assert(value["count"] == expected_count(child))
                assert(value["sum"] == value["count"]*histogram["sum"])
                assert(value["histogram"][str(COUNT_SCALAR_BUCKET)] == value["count"])

    assert len(metric_count) == len(histograms)
    for v in metric_count.values():
        assert(v == 2*len(build_id_aggregates))  # Count both child and parent metrics


def test_use_counter2_histogram():
    metric_count = defaultdict(int)
    histograms = {k: v for k, v in histograms_template.iteritems() if k.startswith("USE_COUNTER2_")}

    pages_destroyed = histograms_template["TOP_LEVEL_CONTENT_DOCUMENTS_DESTROYED"]["sum"]
    docs_destroyed = histograms_template["CONTENT_DOCUMENTS_DESTROYED"]["sum"]

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key
            histogram = histograms.get(metric, None)

            if histogram:
                metric_count[metric] += 1
                assert(label == "")
                if value["count"] != expected_count(child):
                  print "metric %s child %s count %d expected_count %d" % (metric, child, count, expected_count(child))
                assert(value["count"] == expected_count(child))
                assert(value["sum"] == value["count"]*histogram["sum"])

                if metric.endswith("_DOCUMENT"):
                    assert(value["histogram"]["0"] == value["count"]*(docs_destroyed - histogram["values"]["1"]))
                else:
                    assert(value["histogram"]["0"] == value["count"]*(pages_destroyed - histogram["values"]["1"]))


    assert len(metric_count) == len(histograms)
    for v in metric_count.values():
        assert(v == 2*len(build_id_aggregates))  # Count both child and parent metrics


def test_keyed_histograms():
    metric_count = defaultdict(int)

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key

            if metric in keyed_histograms_template.keys():
                metric_count["{}_{}".format(metric, label)] += 1
                assert(label != "")
                if value["count"] != expected_count(child):
                  print "metric %s child %s count %d expected_count %d" % (metric, child, count, expected_count(child))
                assert(value["count"] == expected_count(child))
                assert(value["sum"] == value["count"]*keyed_histograms_template[metric][label]["sum"])

                histogram_template = keyed_histograms_template[metric][label]["values"]
                assert(set(histogram_template.keys()) == set(value["histogram"].keys()))
                assert((pd.Series(histogram_template)*value["count"] == pd.Series(value["histogram"])).all())

            assert(metric not in ignored_keyed_histograms_template.keys())

    assert(len(metric_count) == len(keyed_histograms_template))  # Assume one label per keyed histogram
    for v in metric_count.values():
        assert(v == 2*len(build_id_aggregates))  # Count both child and parent metrics
