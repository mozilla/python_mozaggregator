import os

from google.cloud import bigquery, storage
from pyspark.sql import SparkSession

import pytest
from dataset import generate_pings
from mobile_dataset import generate_mobile_pings
from utils import (
    runif_avro_testing_enabled,
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
    dataset_id = f"{project_id}.pytest_mozaggregator_test"
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

    # result set to be yielded are (table_name, fully-qualified path) pairs
    results = []
    # create the relevant tables
    for table_name, df in [
        ("main_v5", df),
        ("mobile_metrics_v1", mobile_df),
    ]:
        table_id = f"{dataset_id}.telemetry_telemetry__{table_name}"
        table = bigquery.table.Table(table_id, schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="submission_timestamp"
        )
        bq_client.create_table(table)
        bq_client.load_table_from_json(
            df, table, job_config=bigquery.job.LoadJobConfig(schema=schema)
        ).result()

        results.append((table_name, table_id))

    yield results

    bq_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@runif_avro_testing_enabled
@pytest.fixture
def avro_testing_files(bq_testing_table):
    bq_client = bigquery.Client()
    parent_path = os.path.join(os.environ["TMP_AVRO_PATH"], "mozaggregator_test_avro")

    for table_name, table_id in bq_testing_table:
        job = bq_client.query(
            f"SELECT distinct cast(extract(date from submission_timestamp) as string) as ds FROM `{table_id}`"
        )
        for row in job.result():
            ds_nodash = row.ds.replace("-", "")
            path = f"{parent_path}/{ds_nodash}/{table_name}/*.avro"
            bq_client.extract_table(
                f"{table_id}${ds_nodash}",
                path,
                job_config=bigquery.job.ExtractJobConfig(destination_format="AVRO"),
            ).result()

    yield parent_path

    storage_client = storage.Client()
    parts = parent_path.strip("gs://").split("/")
    bucket = parts[0]
    prefix = "/".join(parts[1:parts.index("mozaggregator_test_avro")+1])
    bucket = storage_client.get_bucket(bucket)
    for blob in bucket.list_blobs(prefix=prefix):
        print(f"deleting {blob.name}")
        blob.delete()
