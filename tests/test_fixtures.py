from google.cloud import bigquery, storage
from utils import runif_avro_testing_enabled


@runif_avro_testing_enabled
def test_avro_matches_bigquery_resource(spark, bq_testing_table, avro_testing_files):
    """Test that testing resources for exporting into avro and loading into spark 
    matches results from bigquery."""

    bq_client = bigquery.Client()

    for table_name, table_id in bq_testing_table:
        df = spark.read.format("avro").load(
            avro_testing_files + "/" + table_name + "/*.avro"
        )
        avro_counts = df.count()

        job = bq_client.query("SELECT count(*) as row_count FROM `{}`".format(table_id))
        bq_counts = list(job.result())[0].row_count

        assert avro_counts > 0
        assert avro_counts == bq_counts
