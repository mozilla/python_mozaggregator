import json
from datetime import datetime, timedelta
from os import environ

import click
from pyspark.sql import SparkSession

from . import aggregator, db, parquet, mobile

DS_NODASH_YESTERDAY = datetime.strftime(datetime.utcnow() - timedelta(1), "%Y%m%d")


@click.group()
def entry_point():
    pass


@click.command()
@click.option("--date", type=str, default=DS_NODASH_YESTERDAY)
@click.option("--channels", type=str, default="nightly")
@click.option("--credentials-bucket", type=str, required=True)
@click.option("--credentials-prefix", type=str, required=True)
@click.option("--num-partitions", type=int, default=10000)
def run_aggregator(
    date, channels, credentials_bucket, credentials_prefix, num_partitions
):

    # Mozaggregator expects a series of POSTGRES_* variables in order to connect
    # to a db instance; we pull them into the environment now by reading an
    # object from S3.
    s3 = boto3.resource("s3")
    obj = s3.Object(credentials_bucket, credentials_prefix)
    creds = json.loads(obj.get()["Body"].read().decode("utf-8"))
    for k, v in creds.items():
        environ[k] = v

    # Attempt a database connection now so we can fail fast if credentials are broken.
    db._preparedb()

    spark = SparkSession.builder.getOrCreate()
    channels = [channel.strip() for channel in channels.split(",")]
    print("Running job for {}".format(date))
    aggregates = aggregator.aggregate_metrics(
        spark.sparkContext, channels, date, num_reducers=num_partitions
    )
    print("Number of build-id aggregates: {}".format(aggregates[0].count()))
    print("Number of submission date aggregates: {}".format(aggregates[1].count()))

    # Store the results in Postgres.
    db.submit_aggregates(aggregates)


@click.command()
@click.option("--date", type=str, default=DS_NODASH_YESTERDAY)
@click.option("--channels", type=str, default="nightly")
@click.option("--output", type=str, default="s3://telemetry-parquet/aggregates_poc/v1")
@click.option("--num-partitions", type=int, default=10000)
def run_parquet(date, channels, output, num_partitions):
    spark = SparkSession.builder.getOrCreate()
    channels = [channel.strip() for channel in channels.split(",")]

    print("Running job for {}".format(date))
    aggregates = parquet.aggregate_metrics(
        spark.sparkContext, channels, date, num_reducers=num_partitions
    )
    print("Number of build-id aggregates: {}".format(aggregates[0].count()))
    print("Number of submission date aggregates: {}".format(aggregates[1].count()))

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
def run_mobile(date, output, num_partitions):
    spark = SparkSession.builder.getOrCreate()

    print("Running job for {}".format(date))
    agg_metrics = mobile.aggregate_metrics(
        spark.sparkContext, date, num_partitions=num_partitions
    )
    aggs = mobile.get_aggregates_dataframe(spark, agg_metrics)
    mobile.write_parquet(aggs, output)


entry_point.add_command(run_aggregator, "aggregator")
entry_point.add_command(run_mobile, "mobile")
entry_point.add_command(run_parquet, "parquet")

if __name__ == "__main__":
    entry_point()
