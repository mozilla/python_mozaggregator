import pyspark
import logging
import pandas as pd

from mozaggregator.aggregator import _aggregate_metrics, SIMPLE_MEASURES_PREFIX, SCALARS_PREFIX, COUNT_HISTOGRAM_PREFIX, PROCESS_TYPES
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
    metric_count = defaultdict(lambda: defaultdict(int))

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, process_type = key

            if metric.startswith(SIMPLE_MEASURES_PREFIX):
                metric_count[metric][process_type] += 1
                assert(label == "")
                # Simple measurements are still in childPayloads.
                # expected_count() is correct only for child dimensions in processes.content.
                assert(value["count"] == NUM_PINGS_PER_DIMENSIONS*(NUM_CHILDREN_PER_PING if process_type != "parent" else 1))
                assert(value["sum"] == value["count"]*SCALAR_VALUE)
                assert(value["histogram"][str(SIMPLE_SCALAR_BUCKET)] == value["count"])

    assert len(metric_count) == len(simple_measurements_template)
    for process_counts in metric_count.values():
        assert(len(process_counts) == 2) # 1 for parent, 1 for childPayloads
        for v in process_counts.values():
          assert(v == len(build_id_aggregates))

def test_numerical_scalars():
    metric_count = defaultdict(lambda: defaultdict(int))
    numeric_scalar_metrics = set([k.upper() for k in numeric_scalars_template.keys()])
    keyed_scalar_metrics =  set([k.upper() for k in keyed_scalars_template.keys()])
    string_scalar_metrics = set([k.upper() for k in string_scalars_template.keys()])

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, process_type = key

            if metric.startswith(SCALARS_PREFIX):
                orig_name = metric.replace(SCALARS_PREFIX + '_', "")
                assert(orig_name in numeric_scalar_metrics | keyed_scalar_metrics | string_scalar_metrics)

                if orig_name in numeric_scalar_metrics:
                    assert(label == "")
                elif orig_name in keyed_scalar_metrics:
                    keys = keyed_scalars_template[orig_name.lower()].keys()
                    assert label.lower() in keys, "Metric {}: {} should be in {}, but was not".format(orig_name, label, ", ".join(keys))
                    metric = "{}_{}".format(metric, label)
                elif orig_name in string_scalar_metrics:
                    assert(label == STRING_SCALAR_VALUE)

                metric_count[metric][process_type] += 1
                assert value["count"] == expected_count(process_type, True), "Expected {}, Got {}, Process {}".format(expected_count(process_type, True), value["count"], process_type)
                if orig_name in numeric_scalar_metrics | keyed_scalar_metrics:
                    assert(value["sum"] == value["count"] * SCALAR_VALUE)
                    assert(value["histogram"][str(NUMERIC_SCALAR_BUCKET)] == value["count"])
                else:
                    assert(value["sum"] == value["count"])
                    assert value["histogram"][STRING_SCALAR_BUCKET] == value["count"], "Metric {}: histogram={}, bucket={}, expected_count={}".format(orig_name, str(value["histogram"]), STRING_SCALAR_BUCKET, value["count"])

    keyed_scalars_template_len = len([key for metric, dic in keyed_scalars_template.iteritems() for key in dic])
    assert len(metric_count) == len(numeric_scalars_template) + len(string_scalars_template) + keyed_scalars_template_len
    for metric, process_counts in metric_count.iteritems():
        assert(process_counts.viewkeys() == PROCESS_TYPES)
        for v in process_counts.values():
          assert(v == len(build_id_aggregates))


def test_classic_histograms():
    metric_count = defaultdict(lambda: defaultdict(int))
    histograms = {k: v for k, v in histograms_template.iteritems() if v["histogram_type"] != 4 and not k.startswith("USE_COUNTER2_")}

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, process_type = key
            histogram = histograms.get(metric, None)

            if histogram:
                metric_count[metric][process_type] += 1
                assert(label == "")
                assert(value["count"] == expected_count(process_type))
                assert(value["sum"] == value["count"]*histogram["sum"])
                assert(set(histogram["values"].keys()) == set(value["histogram"].keys()))
                assert((pd.Series(histogram["values"])*value["count"] == pd.Series(value["histogram"])).all())

    assert(len(metric_count) == len(histograms))
    for process_counts in metric_count.values():
        assert(process_counts.viewkeys() == PROCESS_TYPES)
        for v in process_counts.values():
          assert(v == len(build_id_aggregates))


def test_count_histograms():
    metric_count = defaultdict(lambda: defaultdict(int))
    histograms = {"{}_{}".format(COUNT_HISTOGRAM_PREFIX, k): v for k, v in histograms_template.iteritems() if v["histogram_type"] == 4 and not k.endswith("CONTENT_DOCUMENTS_DESTROYED")}

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, process_type = key
            histogram = histograms.get(metric, None)

            if histogram:
                metric_count[metric][process_type] += 1
                assert(label == "")
                assert(value["count"] == expected_count(process_type))
                assert(value["sum"] == value["count"]*histogram["sum"])
                assert(value["histogram"][str(COUNT_SCALAR_BUCKET)] == value["count"])

    assert len(metric_count) == len(histograms)
    for process_counts in metric_count.values():
        assert(process_counts.viewkeys() == PROCESS_TYPES)
        for v in process_counts.values():
          assert(v == len(build_id_aggregates))


def test_use_counter2_histogram():
    metric_count = defaultdict(lambda: defaultdict(int))
    histograms = {k: v for k, v in histograms_template.iteritems() if k.startswith("USE_COUNTER2_")}

    pages_destroyed = histograms_template["TOP_LEVEL_CONTENT_DOCUMENTS_DESTROYED"]["sum"]
    docs_destroyed = histograms_template["CONTENT_DOCUMENTS_DESTROYED"]["sum"]

    for aggregate in build_id_aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, process_type = key
            histogram = histograms.get(metric, None)

            if histogram:
                metric_count[metric][process_type] += 1
                assert(label == "")
                assert(value["count"] == expected_count(process_type))
                assert(value["sum"] == value["count"]*histogram["sum"])

                if metric.endswith("_DOCUMENT"):
                    assert(value["histogram"]["0"] == value["count"]*(docs_destroyed - histogram["values"]["1"]))
                else:
                    assert(value["histogram"]["0"] == value["count"]*(pages_destroyed - histogram["values"]["1"]))


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

            if metric in keyed_histograms_template.keys():
                metric_label = "{}_{}".format(metric, label)
                if metric_label not in metric_count:
                    metric_count[metric_label] = defaultdict(int)
                metric_count[metric_label][process_type] += 1
                assert(label != "")
                assert(value["count"] == expected_count(process_type))
                assert(value["sum"] == value["count"]*keyed_histograms_template[metric][label]["sum"])

                histogram_template = keyed_histograms_template[metric][label]["values"]
                assert(set(histogram_template.keys()) == set(value["histogram"].keys()))
                assert((pd.Series(histogram_template)*value["count"] == pd.Series(value["histogram"])).all())

            assert(metric not in ignored_keyed_histograms_template.keys())

    assert(len(metric_count) == len(keyed_histograms_template))  # Assume one label per keyed histogram
    for process_counts in metric_count.values():
        assert(process_counts.viewkeys() == PROCESS_TYPES)
        for v in process_counts.values():
          assert(v == len(build_id_aggregates))
