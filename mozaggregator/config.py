REGION="us-west-2"
BUCKET="telemetry-spark-emr-2"
SECRET="aggregator_credentials"
WRITE_RDS="telemetry-aggregates"
READ_RDS="telemetry-aggregates-read-replica"
TIMEOUT=24*60*60
MINCONN=4
MAXCONN=64
CACHETYPE="simple"
USE_PRODUCTION_DB=True

