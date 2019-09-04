import pytest
from pyspark.sql import SparkSession


@pytest.fixture()
def spark():
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    yield spark
    spark.stop()


@pytest.fixture()
def sc(spark):
    return spark.sparkContext
