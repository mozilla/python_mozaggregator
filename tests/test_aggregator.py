import logging
from collections import defaultdict

import pandas as pd
import dataset as d
import pyspark
from mozaggregator.aggregator import (
    PROCESS_TYPES, COUNT_HISTOGRAM_PREFIX, NUMERIC_SCALARS_PREFIX,
    SIMPLE_MEASURES_PREFIX, _aggregate_metrics)


def setup_module():
    global build_id_aggregates
    global submission_date_aggregates

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)

    sc = pyspark.SparkContext(master="local[*]")
    raw_pings = list(d.generate_pings())
    build_id_aggregates, submission_date_aggregates = _aggregate_metrics(sc.parallelize(raw_pings), num_reducers=10)
    build_id_aggregates = build_id_aggregates.collect()
    submission_date_aggregates = submission_date_aggregates.collect()

    # Note: most tests are based on the build-id aggregates as the aggregation
    # code is the same for both scenarios.
    sc.stop()


def test_count():
    pings = list(d.generate_pings())
    num_build_ids = len(d.ping_dimensions["build_id"])
    assert(len(pings) / d.NUM_PINGS_PER_DIMENSIONS == len(build_id_aggregates))
    assert(len(pings) / d.NUM_PINGS_PER_DIMENSIONS / num_build_ids == len(submission_date_aggregates))


def test_keys():
    for aggregate in build_id_aggregates:
        submission_date, channel, version, build_id, app, arch, os, os_version = aggregate[0]

        assert(submission_date in d.ping_dimensions["submission_date"])
        assert(channel in d.ping_dimensions["channel"])
        assert(version in [x.split(".")[0] for x in d.ping_dimensions["version"]])
        assert(build_id in [x[:8] for x in d.ping_dimensions["build_id"]])
        assert(app in d.ping_dimensions["application"])
        assert(arch in d.ping_dimensions["arch"])
        assert(os in d.ping_dimensions["os"])
        if os == "Linux":
            assert(os_version in [x[:3] for x in d.ping_dimensions["os_version"]])
        else:
            assert(os_version in d.ping_dimensions["os_version"])

    for aggregate in submission_date_aggregates:
        submission_date, channel, version, app, arch, os, os_version = aggregate[0]

        assert(submission_date in d.ping_dimensions["submission_date"])
        assert(channel in d.ping_dimensions["channel"])
        assert(version in [x.split(".")[0] for x in d.ping_dimensions["version"]])
        assert(app in d.ping_dimensions["application"])
        assert(arch in d.ping_dimensions["arch"])
        assert(os in d.ping_dimensions["os"])
        if os == "Linux":
            assert(os_version in [x[:3] for x in d.ping_dimensions["os_version"]])
        else:
            assert(os_version in d.ping_dimensions["os_version"])


def test_simple_measurements():
    metric_count = defaultdict(lambda: defaultdict(int))

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, process_type = key

            if metric.startswith(SIMPLE_MEASURES_PREFIX):
                metric_count[metric][process_type] += 1
                assert(label == "")
                # Simple measurements are still in childPayloads.
                # d.expected_count() is correct only for child dimensions in processes.content.
                assert(value["count"] == d.NUM_PINGS_PER_DIMENSIONS * (d.NUM_CHILDREN_PER_PING if process_type != "parent" else 1))
                assert(value["sum"] == value["count"] * d.SCALAR_VALUE)
                assert(value["histogram"][str(d.SIMPLE_SCALAR_BUCKET)] == value["count"])

    assert len(metric_count) == len(d.simple_measurements_template)
    for process_counts in metric_count.values():
        assert(len(process_counts) == 2)  # 1 for parent, 1 for childPayloads
        for v in process_counts.values():
            assert(v == len(build_id_aggregates))


def test_numerical_scalars():
    metric_count = defaultdict(lambda: defaultdict(int))
    scalar_metrics = set([k.upper() for k in d.scalars_template.keys()])
    keyed_scalar_metrics = set([k.upper() for k in d.keyed_scalars_template.keys()])
    keyed_scalar_metrics |= set([k.upper() for k in d.private_keyed_scalars_template.keys()])

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, process_type = key

            if metric.startswith(NUMERIC_SCALARS_PREFIX):
                orig_name = metric.replace(NUMERIC_SCALARS_PREFIX + "_", "")
                assert(orig_name in scalar_metrics | keyed_scalar_metrics)

                if orig_name in scalar_metrics:
                    assert(label == "")
                else:
                    assert(label != "")
                    metric = "{}_{}".format(metric, label)

                metric_count[metric][process_type] += 1
                assert value["count"] == d.expected_count(process_type, True), (
                    "Expected {}, Got {}, Process {}".format(d.expected_count(process_type, True), value["count"], process_type))
                assert(value["sum"] == value["count"] * d.SCALAR_VALUE)
                assert(value["histogram"][str(d.NUMERIC_SCALAR_BUCKET)] == value["count"])

    keyed_scalars_template_len = len([key for m, dic in d.keyed_scalars_template.iteritems() for key in dic])
    keyed_scalars_template_len += len([key for m, dic in d.private_keyed_scalars_template.iteritems() for key in dic])
    assert len(metric_count) == len(d.scalars_template) + keyed_scalars_template_len
    for metric, process_counts in metric_count.iteritems():
        assert(process_counts.viewkeys() == PROCESS_TYPES)
        for v in process_counts.values():
            assert(v == len(build_id_aggregates))


def test_classic_histograms():
    metric_count = defaultdict(lambda: defaultdict(int))
    histograms = {k: v for k, v in d.histograms_template.iteritems()
                  if v is not None and v.get("histogram_type", -1) != 4 and not k.startswith("USE_COUNTER2_")}

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, process_type = key
            histogram = histograms.get(metric, None)

            if histogram:
                metric_count[metric][process_type] += 1
                assert(label == "")
                assert(value["count"] == d.expected_count(process_type))
                assert(value["sum"] == value["count"] * histogram["sum"])
                assert(set(histogram["values"].keys()) == set(value["histogram"].keys()))
                assert((pd.Series(histogram["values"]) * value["count"] == pd.Series(value["histogram"])).all())

    assert(len(metric_count) == len(histograms))
    for process_counts in metric_count.values():
        assert(process_counts.viewkeys() == PROCESS_TYPES)
        for v in process_counts.values():
            assert(v == len(build_id_aggregates))


def test_count_histograms():
    metric_count = defaultdict(lambda: defaultdict(int))
    histograms = {"{}_{}".format(COUNT_HISTOGRAM_PREFIX, k): v for k, v in d.histograms_template.iteritems()
                  if v is not None and v.get("histogram_type", -1) == 4 and not k.endswith("CONTENT_DOCUMENTS_DESTROYED")}

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, process_type = key
            histogram = histograms.get(metric, None)

            if histogram:
                metric_count[metric][process_type] += 1
                assert(label == "")
                assert(value["count"] == d.expected_count(process_type))
                assert(value["sum"] == value["count"] * histogram["sum"])
                assert(value["histogram"][str(d.COUNT_SCALAR_BUCKET)] == value["count"])

    assert len(metric_count) == len(histograms)
    for process_counts in metric_count.values():
        assert(process_counts.viewkeys() == PROCESS_TYPES)
        for v in process_counts.values():
            assert(v == len(build_id_aggregates))


def test_keyed_histograms():
    metric_count = defaultdict(lambda: defaultdict(int))

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, process_type = key

            if metric in d.keyed_histograms_template.keys():
                metric_label = "{}_{}".format(metric, label)
                if metric_label not in metric_count:
                    metric_count[metric_label] = defaultdict(int)
                metric_count[metric_label][process_type] += 1
                assert(label != "")
                assert(value["count"] == d.expected_count(process_type))
                assert(value["sum"] == value["count"] * d.keyed_histograms_template[metric][label]["sum"])

                histogram_template = d.keyed_histograms_template[metric][label]["values"]
                assert(set(histogram_template.keys()) == set(value["histogram"].keys()))
                assert((pd.Series(histogram_template) * value["count"] == pd.Series(value["histogram"])).all())

            assert(metric not in d.ignored_keyed_histograms_template.keys())

    assert(len(metric_count) == len(d.keyed_histograms_template))  # Assume one label per keyed histogram
    for process_counts in metric_count.values():
        assert(process_counts.viewkeys() == PROCESS_TYPES)
        for v in process_counts.values():
            assert(v == len(build_id_aggregates))
