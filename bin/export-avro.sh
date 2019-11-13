#!/bin/bash

# A testing script for verifying the avro exports work with the existing
# mozaggregator code. This requires `gcloud` to be configured to point at a
# sandbox project for reading data from `payload_bytes_decoded`. There is a
# 10 TB export limit per day, so be conservative with usage.

set -eou pipefail

# system agnostic way of obtaining yesterday's date, macOS' date utility doesnt provide -d
function default_date() {
    python3 - <<END
from datetime import date, timedelta
ds = f"{date.today() - timedelta(1)}"
print(ds)
END
}

function query_to_destination() {
    local table_name=$1
    local channels=$2

    local table="${SOURCE_PROJECT}.payload_bytes_decoded.telemetry_telemetry__${table_name}"

    local channel_clause=""
    if [[ -n ${channels} ]]; then
        channel_clause="AND normalized_channel in (${channels})"
    fi
    bq query \
        --max_rows=0 \
        --destination_table "${DESTINATION_DATASET}.${table_name}" \
        --replace \
        --use_legacy_sql=false \
        "SELECT * FROM \`${table}\` WHERE DATE(submission_timestamp) = DATE \"${DATE}\" ${channel_clause}"

}

function extract_table() {
    local table_name=$1

    local suffix='*.avro'
    local ds_nodash
    ds_nodash=$(echo "${DATE}" | tr -d "-")

    local table="${DESTINATION_DATASET}.${table_name}"
    local output="${OUTPUT_PREFIX}/${SOURCE_PROJECT}/${ds_nodash}/${table_name}/${suffix}"

    if gsutil -q stat "${output}"; then
        echo "${output} already exists! skipping..."
        return
    fi

    echo "writing ${table} to ${output}"
    bq extract --destination_format=AVRO "${table}" "${output}"
}

function main() {
    # moz-fx-data-shared-prod OR moz-fx-data-shar-nonprod-efed
    SOURCE_PROJECT=${1?expected source project of BigQuery tables in first argument}
    DESTINATION_DATASET=${2?expect destination dataset of form project_id:dataset}
    OUTPUT_PREFIX=${3?expected gs:// output path in second argument}
    DATE=${4:-$(default_date)}

    # NOTE: channels are hardcoded...
    query_to_destination "main_v4"              "'nightly', 'beta'"
    query_to_destination "saved_session_v4"     "'nightly', 'beta'"
    query_to_destination "mobile_metrics_v1"    ""

    extract_table "main_v4"
    extract_table "saved_session_v4"
    extract_table "mobile_metrics_v1"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
