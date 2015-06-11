import pyspark
import logging
import psycopg2
import os

from mozaggregator.aggregator import _aggregate_metrics
from dataset import *


def setup_module():
    global aggregates
    global db

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)

    sc = pyspark.SparkContext(master="local[*]")
    raw_pings = list(generate_pings())
    aggregates = _aggregate_metrics(sc.parallelize(raw_pings)).collect()
    sc.stop()

    url = os.getenv("DB_TEST_URL")
    db = psycopg2.connect(url)


def test_connection():
    assert(db)
