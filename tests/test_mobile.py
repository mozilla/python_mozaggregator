import logging
import os

import pyspark
import pytest
from click.testing import CliRunner

import mobile_dataset as d
from mozaggregator.cli import run_mobile
from mozaggregator.mobile import _aggregate_metrics, get_aggregates_dataframe
from utils import runif_bigquery_testing_enabled

@pytest.fixture()
def aggregates_rdd(sc):
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)

    raw_pings = list(d.generate_mobile_pings())
    return _aggregate_metrics(sc.parallelize(raw_pings), num_partitions=10)


@pytest.fixture()
def aggregates(aggregates_rdd):
    return aggregates_rdd.collect()


def test_count(aggregates):
    pings = list(d.generate_mobile_pings())
    assert(len(pings) / d.NUM_PINGS_PER_DIMENSIONS == len(aggregates))


def test_keys(aggregates):
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


def test_histograms(aggregates):
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


def test_scalars(aggregates):
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


def test_mobile_aggregation_cli(tmp_path, monkeypatch, spark, aggregates_rdd):
    output = str(tmp_path / "output")

    class Dataset:
        @staticmethod
        def from_source(*args, **kwargs):
            return Dataset()

        def where(self, *args, **kwargs):
            return self

        def records(self, *args, **kwargs):
            return spark.sparkContext.parallelize(d.generate_mobile_pings())

    monkeypatch.setattr("mozaggregator.mobile.Dataset", Dataset)

    result = CliRunner().invoke(
        run_mobile,
        [
            "--date",
            # this date is ignored because we are monkeypatching the dataset
            "20190901",
            "--output",
            output,
            "--num-partitions",
            10,
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    expect = get_aggregates_dataframe(spark, aggregates_rdd)
    actual = spark.read.parquet(output)

    assert expect.count() > 0 and actual.count() > 0
    assert expect.count() == actual.count()


@runif_bigquery_testing_enabled
def test_mobile_aggregation_cli_bigquery(tmp_path, spark, aggregates_rdd, bq_testing_table):
    output = str(tmp_path / "output")

    result = CliRunner().invoke(
        run_mobile,
        [
            "--date",
            d.SUBMISSION_DATE_1.strftime('%Y%m%d'),
            "--output",
            output,
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

    assert result.exit_code == 0

    expect = get_aggregates_dataframe(spark, aggregates_rdd)
    actual = spark.read.parquet(output)

    assert expect.count() > 0 and actual.count() > 0
    assert expect.count() == actual.count()
