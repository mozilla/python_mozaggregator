import pyspark
import logging
import pandas as pd
import re

from collections import defaultdict
from mozaggregator.aggregator import _aggregate_metrics
from dataset import *


def setup_module():
    global aggregates

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)

    sc = pyspark.SparkContext(master="local[*]")
    raw_pings = list(generate_pings())
    aggregates = _aggregate_metrics(sc.parallelize(raw_pings)).collect()
    sc.stop()


def test_count():
    assert(len(list(generate_pings()))/NUM_PINGS_PER_DIMENSIONS == len(aggregates))


def test_keys():
    for aggregate in aggregates:
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


def test_simple_measurements():
    metric_count = defaultdict(int)

    for aggregate in aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key

            if re.match("^\[\[SCALAR\]\]_SIMPLE_MEASURES.*$", metric):
                metric_count[metric] += 1
                assert(label == "")
                assert(child is False)
                assert(value["count"] == NUM_PINGS_PER_DIMENSIONS)
                assert(value["histogram"][str(SCALAR_BUCKET)] == value["count"])

    assert len(metric_count) == len(simple_measurements_template)
    for v in metric_count.values():
        assert(v == len(aggregates))


def test_classic_histograms():
    metric_count = defaultdict(int)
    histograms = {k: v for k, v in histograms_template.iteritems() if v["histogram_type"] != 4}

    for aggregate in aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key
            histogram = histograms.get(metric, None)

            if histogram:
                metric_count[metric] += 1
                assert(label == "")
                assert(value["count"] == NUM_PINGS_PER_DIMENSIONS*(NUM_CHILDREN_PER_PING if child else 1))
                assert(set(histogram["values"].keys()) == set(value["histogram"].keys()))
                assert((pd.Series(histogram["values"])*value["count"] == pd.Series(value["histogram"])).all())

    assert(len(metric_count) == len(histograms))
    for v in metric_count.values():
        assert(v == 2*len(aggregates))  # Count both child and parent metrics


def test_count_histograms():
    metric_count = defaultdict(int)
    histograms = {"[[SCALAR]]_{}".format(k): v for k, v in histograms_template.iteritems() if v["histogram_type"] == 4}

    for aggregate in aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key
            histogram = histograms.get(metric, None)

            if histogram:
                metric_count[metric] += 1
                assert(label == "")
                assert(value["count"] == NUM_PINGS_PER_DIMENSIONS*(NUM_CHILDREN_PER_PING if child else 1))
                assert(value["histogram"][str(SCALAR_BUCKET)] == value["count"])

    assert len(metric_count) == len(histograms)
    for v in metric_count.values():
        assert(v == 2*len(aggregates))  # Count both child and parent metrics


def test_keyed_histograms():
    metric_count = defaultdict(int)

    for aggregate in aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key

            if metric in keyed_histograms_template.keys():
                metric_count["{}_{}".format(metric, label)] += 1
                assert(label != "")
                assert(value["count"] == NUM_PINGS_PER_DIMENSIONS*(NUM_CHILDREN_PER_PING if child else 1))

                histogram_template = keyed_histograms_template[metric][label]["values"]
                assert(set(histogram_template.keys()) == set(value["histogram"].keys()))
                assert((pd.Series(histogram_template)*value["count"] == pd.Series(value["histogram"])).all())

    assert(len(metric_count) == len(keyed_histograms_template))  # Assume one label per keyed histogram
    for v in metric_count.values():
        assert(v == 2*len(aggregates))  # Count both child and parent metrics
