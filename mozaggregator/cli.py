import json
from datetime import datetime, timedelta
from os import environ

import click
from pyspark.sql import SparkSession

from mozaggregator import aggregator, db, parquet, mobile

DS_NODASH_YESTERDAY = datetime.strftime(datetime.utcnow() - timedelta(1), "%Y%m%d")


@click.group()
def entry_point():
    pass


@click.command()
@click.option("--date", type=str, default=DS_NODASH_YESTERDAY)
@click.option("--channels", type=str, default="nightly")
@click.option(
    "--credentials-protocol", type=click.Choice(["file", "s3", "gcs"]), default="s3"
)
@click.option("--credentials-bucket", type=str, required=False)
@click.option("--credentials-prefix", type=str, required=False)
@click.option("--num-partitions", type=int, default=10000)
@click.option(
    "--source",
    type=click.Choice(["bigquery", "moztelemetry", "avro"]),
    default="moztelemetry",
)
@click.option(
    "--project-id", envvar="PROJECT_ID", type=str, default="moz-fx-data-shared-prod"
)
@click.option("--dataset-id", type=str, default="payload_bytes_decoded")
@click.option("--avro-prefix", type=str)
def run_aggregator(
    date,
    channels,
    credentials_protocol,
    credentials_bucket,
    credentials_prefix,
    num_partitions,
    source,
    project_id,
    dataset_id,
    avro_prefix,
):
    spark = SparkSession.builder.getOrCreate()

    # Mozaggregator expects a series of POSTGRES_* variables in order to connect
    # to a db instance; we may pull them into the environment now by reading an
    # object from a file system.
    def create_path(protocol, bucket, prefix):
        mapping = {"file": "file", "s3": "s3a", "gcs": "gs"}
        return f"{mapping[protocol]}://{bucket}/{prefix}"

    if credentials_bucket and credentials_prefix:
        path = create_path(credentials_protocol, credentials_bucket, credentials_prefix)
        print(f"reading credentials from {path}")
        creds = spark.read.json(path, multiLine=True).first().asDict()
        for k, v in creds.items():
            environ[k] = v
    else:
        print(f"assuming credentials from the environment")

    # Attempt a database connection now so we can fail fast if credentials are broken.
    db._preparedb()

    channels = [channel.strip() for channel in channels.split(",")]
    print(f"Running job for {date}")
    aggregates = aggregator.aggregate_metrics(
        spark.sparkContext,
        channels,
        date,
        num_reducers=num_partitions,
        source=source,
        project_id=project_id,
        dataset_id=dataset_id,
        avro_prefix=avro_prefix,
    )
    print(f"Number of build-id aggregates: {aggregates[0].count()}")
    print(f"Number of submission date aggregates: {aggregates[1].count()}")

    # Store the results in Postgres.
    db.submit_aggregates(aggregates)


@click.command()
@click.option("--date", type=str, default=DS_NODASH_YESTERDAY)
@click.option("--channels", type=str, default="nightly")
@click.option("--output", type=str, default="s3://telemetry-parquet/aggregates_poc/v1")
@click.option("--num-partitions", type=int, default=10000)
@click.option(
    "--source",
    type=click.Choice(["bigquery", "moztelemetry", "avro"]),
    default="moztelemetry",
)
@click.option(
    "--project-id", envvar="PROJECT_ID", type=str, default="moz-fx-data-shared-prod"
)
@click.option("--dataset-id", type=str, default="payload_bytes_decoded")
@click.option("--avro-prefix", type=str)
def run_parquet(
    date, channels, output, num_partitions, source, project_id, dataset_id, avro_prefix
):
    spark = SparkSession.builder.getOrCreate()
    channels = [channel.strip() for channel in channels.split(",")]

    print(f"Running job for {date}")
    aggregates = parquet.aggregate_metrics(
        spark.sparkContext,
        channels,
        date,
        num_reducers=num_partitions,
        source=source,
        project_id=project_id,
        dataset_id=dataset_id,
        avro_prefix=avro_prefix,
    )
    print(f"Number of build-id aggregates: {aggregates[0].count()}")
    print(f"Number of submission date aggregates: {aggregates[1].count()}")

    parquet.write_aggregates(spark, aggregates, output, "append")


@click.command()
@click.option("--date", type=str, default=DS_NODASH_YESTERDAY)
@click.option(
    "--output",
    type=str,
    default="s3://{}/{}/{}".format(
        mobile.PATH_BUCKET, mobile.PATH_PREFIX, mobile.PATH_VERSION
    ),
)
@click.option("--num-partitions", type=int, default=10000)
@click.option(
    "--source",
    type=click.Choice(["bigquery", "moztelemetry", "avro"]),
    default="moztelemetry",
)
@click.option(
    "--project-id", envvar="PROJECT_ID", type=str, default="moz-fx-data-shared-prod"
)
@click.option("--dataset-id", type=str, default="payload_bytes_decoded")
@click.option("--avro-prefix", type=str)
def run_mobile(
    date, output, num_partitions, source, project_id, dataset_id, avro_prefix
):
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    print(f"Running job for {date}")
    agg_metrics = mobile.aggregate_metrics(
        spark.sparkContext,
        date,
        num_partitions=num_partitions,
        source=source,
        project_id=project_id,
        dataset_id=dataset_id,
        avro_prefix=avro_prefix,
    )
    aggs = mobile.get_aggregates_dataframe(spark, agg_metrics)
    mobile.write_parquet(aggs, output)


entry_point.add_command(run_aggregator, "aggregator")
entry_point.add_command(run_mobile, "mobile")
entry_point.add_command(run_parquet, "parquet")

if __name__ == "__main__":
    entry_point()
