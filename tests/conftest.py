import os

from google.cloud import bigquery
from pyspark.sql import SparkSession

import pytest
from dataset import generate_pings
from mobile_dataset import generate_mobile_pings
from utils import (
    runif_bigquery_testing_enabled,
    format_payload_bytes_decoded,
    format_payload_bytes_decoded_mobile,
)


@pytest.fixture()
def spark():
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    yield spark
    spark.stop()


@pytest.fixture()
def sc(spark):
    return spark.sparkContext


@runif_bigquery_testing_enabled
@pytest.fixture
def bq_testing_table():
    bq_client = bigquery.Client()

    project_id = os.environ["PROJECT_ID"]
    dataset_id = "{project_id}.pytest_mozaggregator_test".format(project_id=project_id)
    bq_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
    bq_client.create_dataset(dataset_id)

    schema = bq_client.schema_from_json(
        os.path.join(os.path.dirname(__file__), "decoded.1.bq")
    )
    # use load_table instead of insert_rows to avoid eventual consistency guarantees
    df = [format_payload_bytes_decoded(ping) for ping in generate_pings()]
    mobile_df = [
        format_payload_bytes_decoded_mobile(ping) for ping in generate_mobile_pings()
    ]

    # create the relevant tables
    for table_name, df in [
        ("main_v4", df),
        ("saved_session_v4", df),
        ("mobile_metrics_v1", mobile_df),
    ]:
        table_id = "{dataset_id}.telemetry_telemetry__{table_name}".format(
            dataset_id=dataset_id, table_name=table_name
        )
        table = bq_client.create_table(bigquery.table.Table(table_id, schema))
        bq_client.load_table_from_json(
            df, table, job_config=bigquery.job.LoadJobConfig(schema=schema)
        ).result()

    yield

    bq_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
