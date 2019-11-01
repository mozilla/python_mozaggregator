import base64
import json
import os
import zlib
from datetime import datetime


def format_payload_bytes_decoded(ping):
    # fields are created in tests/dataset.py
    return {
        "normalized_app_name": ping["application"]["name"],
        "normalized_channel": ping["application"]["channel"],
        "normalized_os": ping["environment"]["system"]["os"]["name"],
        "metadata": {
            "uri": {
                "app_version": ping["application"]["version"],
                "app_build_id": ping["application"]["buildId"],
                "app_name": ping["application"]["name"],
            }
        },
        "sample_id": ping["meta"]["sampleId"],
        "submission_timestamp": datetime.strptime(
            ping["meta"]["submissionDate"], "%Y%m%d"
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "payload": base64.b64encode(zlib.compress(json.dumps(ping).encode())).decode(),
    }


def format_payload_bytes_decoded_mobile(ping):
    """Format the mobile payload, this requires less meta information than the
    normal dataset because there is little to no filtering being done in the job.

    Fields are created in tests/mobile_dataset.py.
    """
    return {
        "submission_timestamp": datetime.strptime(
            ping["meta"]["submissionDate"], "%Y%m%d"
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "normalized_channel": ping["meta"]["normalizedChannel"],
        "metadata": {
            "uri": {
                "app_version": str(ping["meta"]["appVersion"]),
                "app_build_id": ping["meta"]["appBuildId"],
                "app_name": ping["meta"]["appName"],
            }
        },
        "payload": base64.b64encode(zlib.compress(json.dumps(ping).encode())).decode(),
    }


def runif_bigquery_testing_enabled(func):
    """A decorator that will skip the test if the current environment is not set up for running tests.

        @runif_bigquery_testing_enabled
        def test_my_function_that_uses_bigquery_spark_connector(table_fixture):
            ...
    """
    # importing this at module scope will break test discoverability
    import pytest

    bigquery_testing_enabled = os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS"
    ) and os.environ.get("PROJECT_ID")
    return pytest.mark.skipif(
        not bigquery_testing_enabled,
        reason="requires valid gcp credentials and project id",
    )(func)


def runif_avro_testing_enabled(func):
    """A decorator that will skip the test if the current environment is not set up for running tests.

        @runif_avro_testing_enabled
        def test_my_function_that_uses_gcs_connector(table_fixture):
            ...
    """
    # importing this at module scope will break test discoverability
    import pytest

    avro_testing_enabled = (
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        and os.environ.get("PROJECT_ID")
        and os.environ.get("TMP_AVRO_PATH")
    )
    if os.environ.get("TMP_AVRO_PATH"):
        assert os.environ["TMP_AVRO_PATH"].startswith(
            "gs://"
        ), "temporary avro path must be start with gs://"
    return pytest.mark.skipif(
        not avro_testing_enabled,
        reason="requires valid gcp credentials, project id, and temporary avro path",
    )(func)
