import logging
import unittest

import pyspark
import pytest
from click.testing import CliRunner

import dataset as d
from mozaggregator.parquet import _aggregate_metrics
from mozaggregator.cli import run_parquet


class testParquetAggregation(unittest.TestCase):

    def setUp(self):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.ERROR)

        self.sc = pyspark.SparkContext(master="local[*]")
        raw_pings = list(d.generate_pings())
        build_id_aggs, submission_date_aggs = (
            _aggregate_metrics(self.sc.parallelize(raw_pings), num_reducers=10))
        self.build_id_aggs = build_id_aggs.collect()
        self.submission_date_aggs = submission_date_aggs.collect()

    def tearDown(self):
        self.sc.stop()

    def test_count(self):
        pings = list(d.generate_pings())
        num_build_ids = len(d.ping_dimensions["build_id"])
        self.assertEqual(len(pings) / d.NUM_PINGS_PER_DIMENSIONS,
                         len(self.build_id_aggs))
        self.assertEqual(
            len(pings) / d.NUM_PINGS_PER_DIMENSIONS / num_build_ids,
            len(self.submission_date_aggs))


@pytest.fixture()
def raw_pings():
    return list(d.generate_pings())


def test_parquet_aggregation_cli(tmp_path, monkeypatch, spark, raw_pings):
    output = str(tmp_path / "output")

    class Dataset:
        @staticmethod
        def from_source(*args, **kwargs):
            return Dataset()

        def where(self, *args, **kwargs):
            self.is_fennec = kwargs.get("docType") == "saved_session"
            return self

        def records(self, *args, **kwargs):
            if self.is_fennec:
                return spark.sparkContext.emptyRDD()
            else:
                return spark.sparkContext.parallelize(raw_pings)

    monkeypatch.setattr("mozaggregator.parquet.Dataset", Dataset)

    result = CliRunner().invoke(
        run_parquet,
        [
            "--date",
            "20190901",
            "--channels",
            "nightly,beta",
            "--output",
            output,
            "--num-partitions",
            10,
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    df = spark.read.parquet(output)
    # 31104 is the empirical count from the generated pings
    assert df.count() > len(raw_pings)
