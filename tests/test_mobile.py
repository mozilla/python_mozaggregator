import logging

import mobile_dataset as d
import pyspark
from mozaggregator.mobile import _aggregate_metrics


def setup_module():
    global sc
    global aggregates

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)

    sc = pyspark.SparkContext(master="local[*]")
    raw_pings = list(d.generate_mobile_pings())
    aggregates = _aggregate_metrics(sc.parallelize(raw_pings), num_partitions=10)
    aggregates = aggregates.collect()

    # Note: most tests are based on the build-id aggregates as the aggregation
    # code is the same for both scenarios.
    sc.stop()


def test_count():
    pings = list(d.generate_mobile_pings())
    assert(len(pings) / d.NUM_PINGS_PER_DIMENSIONS == len(aggregates))


def test_keys():
    for aggregate in aggregates:
        (submission_date, channel, version, build_id, application,
         architecture, os, os_version) = aggregate[0]
        assert(submission_date == d.meta_template["submissionDate"])
        assert(channel in d.ping_dimensions["normalizedChannel"])
        assert(version == d.meta_template["appVersion"])
        assert(build_id == d.meta_template["appBuildId"])
        assert(application == d.meta_template["appName"])
        assert(architecture in d.ping_dimensions["arch"])
        assert(os == d.meta_template["normalizedOs"])
        assert(os_version in d.ping_dimensions["osversion"])


def test_histograms():
    n = d.NUM_PINGS_PER_DIMENSIONS
    for aggregate in aggregates:
        for metric_data in aggregate[1].items():
            metric_name, metric_key, process = metric_data[0]
            # A regular histogram.
            if metric_name in d.histograms_template.keys():
                tpl = d.histograms_template[metric_name]
                assert(metric_data[1]['count'] == n)
                assert(metric_data[1]['sum'] == tpl['sum'] * n)
                for k, v in tpl['values'].items():
                    assert(metric_data[1]['histogram'][k] == v * n)
            # A keyed histogram.
            elif metric_name in d.keyed_histograms_template.keys():
                tpl = d.keyed_histograms_template[metric_name]
                assert(metric_data[1]['count'] == n)
                assert(metric_data[1]['sum'] == tpl[metric_key]['sum'] * n)
                for k, v in tpl[metric_key]['values'].items():
                    assert(metric_data[1]['histogram'][k] == v * n)


def test_scalars():
    n = d.NUM_PINGS_PER_DIMENSIONS
    for aggregate in aggregates:
        for metric_data in aggregate[1].items():
            metric_name, metric_key, process = metric_data[0]
            metric_name = metric_name.split('_')[1].lower()
            # A regular scalar.
            if metric_name in d.scalars_template.keys():
                value = d.scalars_template[metric_name]
            # A keyed scalar.
            elif metric_name in d.keyed_scalars_template.keys():
                value = d.keyed_scalars_template[metric_name][metric_key]
            else:
                continue
            assert(metric_data[1]['count'] == n)
            assert(metric_data[1]['sum'] == value * n)
            assert(metric_data[1]['histogram'] == {str(value): n})
