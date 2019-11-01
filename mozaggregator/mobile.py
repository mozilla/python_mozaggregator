from collections import defaultdict
from os import environ

import pandas as pd
from pyspark.sql.types import (
    LongType, MapType, StringType, StructField, StructType)

from mozaggregator.aggregator import (
    SCALAR_MEASURE_MAP, _aggregate_aggregates, _extract_numeric_scalars,
    _extract_keyed_numeric_scalars, _extract_main_histograms,
    _extract_keyed_histograms)
from mozaggregator.db import histogram_revision_map
from moztelemetry.dataset import Dataset
from moztelemetry.histogram import Histogram
from .bigquery import BigQueryDataset

import warnings
warnings.filterwarnings("always")

PATH_BUCKET = environ.get('bucket', 'telemetry-parquet')
PATH_PREFIX = 'mobile_metrics_aggregates'
PATH_VERSION = 'v2'

SCHEMA = StructType([
    StructField('submission_date', StringType(), False),
    StructField('channel', StringType(), False),
    StructField('version', StringType(), False),
    StructField('build_id', StringType(), True),
    StructField('application', StringType(), False),
    StructField('architecture', StringType(), False),
    StructField('os', StringType(), False),
    StructField('os_version', StringType(), False),
    StructField('metric', StringType(), False),
    StructField('key', StringType(), True),
    StructField('process', StringType(), False),
    StructField('count', LongType(), False),
    StructField('sum', LongType(), False),
    StructField('histogram', MapType(StringType(), LongType(), False), False),
])


def get_aggregates_dataframe(spark, aggregates):
    build_id_agg = aggregates.flatMap(lambda row: _explode(row))
    return spark.createDataFrame(build_id_agg, SCHEMA)


def write_parquet(df, path):
    (df.repartition('metric')
       .sortWithinPartitions(['channel', 'version', 'submission_date'])
       .write
       .partitionBy('metric')
       .parquet(path, mode='append'))


def _explode(row):
    dimensions, metrics = row

    for k, v in metrics.items():
        try:
            histogram = _get_complete_histogram(dimensions[1], k[0], v['histogram'])
        except KeyError:
            continue
        yield dimensions + k + (v['count'], v['sum'], histogram)


def _get_complete_histogram(channel, metric, values):
    revision = histogram_revision_map[channel]

    for prefix, labels in SCALAR_MEASURE_MAP.items():
        if metric.startswith(prefix):
            histogram = pd.Series({int(k): v for k, v in values.items()},
                                  index=labels).fillna(0)
            break
    else:
        histogram = Histogram(metric, {"values": values},
                              revision=revision).get_value(autocast=False)

    return {str(k): int(v) for k, v in histogram.to_dict().items()}


def _extract_process_scalars(state, metrics, process):
    scalars = metrics.get("scalars", {})
    keyed_scalars = metrics.get("keyedScalars", {})

    if not isinstance(scalars, dict) or not isinstance(keyed_scalars, dict):
        raise("Scalar is not a scalar!")

    _extract_numeric_scalars(state, scalars, process)
    _extract_keyed_numeric_scalars(state, keyed_scalars, process)


def _extract_process_histograms(state, metrics, process):
    histograms = metrics.get("histograms", {})
    keyedHistograms = metrics.get("keyedHistograms", {})

    if not isinstance(histograms, dict) or not isinstance(keyedHistograms, dict):
        raise Exception("Histogram is not a histogram!")

    _extract_main_histograms(state, histograms, process)
    for name, histogram in keyedHistograms.items():
        _extract_keyed_histograms(state, name, histogram, process)


def _aggregate_ping(state, metrics):
    if not isinstance(metrics, dict):
        raise Exception(
            "When is a ping not a ping? (%s)"
            % type(metrics)
        )

    for process in list(metrics.keys()):
        process_metrics = metrics.get(process, {})
        _extract_process_histograms(state, process_metrics, process)
        _extract_process_scalars(state, process_metrics, process)
    return state


def _aggregate_metrics(pings, num_partitions):
    trimmed = (
        pings.map(_map_ping_to_dimensions)
             .filter(lambda x: x))

    return trimmed.aggregateByKey(
        defaultdict(dict), _aggregate_ping, _aggregate_aggregates,
        num_partitions)


def _map_ping_to_dimensions(ping):
    try:
        submission_date = ping["meta"]["submissionDate"]
        channel = ping["meta"]["normalizedChannel"]
        version = ping["meta"]["appVersion"]
        build_id = ping["meta"]["appBuildId"]
        application = ping["meta"]["appName"]
        architecture = ping["arch"]
        os = ping["os"]
        os_version = ping["osversion"]

        # TODO: Validate build_id string against the whitelist from build hub.

        # Note that some dimensions don't vary within a single submission
        # (e.g. channel) while some do (e.g. process type).
        # Dimensions that don't vary should appear in the submission key, while
        # the ones that do vary should appear within the key of a single metric.
        return (
            (submission_date, channel, version, build_id, application,
             architecture, os, os_version),
            ping.get("metrics", {})
        )
    except KeyError:
        raise


def aggregate_metrics(
    sc,
    begin,
    end=None,
    num_partitions=10000,
    source="moztelemetry",
    project_id=None,
    dataset_id=None,
    ):
    """
    Returns the build-id and submission date aggregates for a given submission date.

    :param sc: A SparkContext instance
    :param begin: A string for the beginning date, in form "YYYYMMDD"
    :param end: An optional string for the end date, in form "YYYYMMDD". If
        not provided, metrics will only be aggregrated for the date provided
        with `begin`.
    :param num_partitions: An optional value to be passed to `aggregateByKey`.

    """
    if end is None:
        end = begin

    if source == "bigquery" and project_id and dataset_id:
        if end != begin:
            raise NotImplementedError(
                "processing multiple days of data is not supported for BigQuery source"
            )
        dataset = BigQueryDataset()
        pings = dataset.load(project_id, dataset_id, "mobile_metrics", begin, doc_version="v1")
    else:
        pings = (Dataset.from_source('telemetry')
                        .where(docType='mobile_metrics',
                            submissionDate=lambda x: begin <= x <= end)
                        .records(sc))
    assert pings.count() > 0
    return _aggregate_metrics(pings, num_partitions)


def run(sparkSession, begin, end=None):
    agg_metrics = aggregate_metrics(sparkSession.sparkContext, begin, end)
    aggs = get_aggregates_dataframe(sparkSession, agg_metrics)
    path = 's3://{bucket}/{prefix}/{version}'.format(
        bucket=PATH_BUCKET, prefix=PATH_PREFIX, version=PATH_VERSION
    )
    write_parquet(aggs, path)
