import logging
import unittest

import pyspark
import pytest

import dataset as d
from mozaggregator.parquet import _aggregate_metrics


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
