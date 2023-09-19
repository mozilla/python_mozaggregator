#!/bin/bash

# A testing script for verifying the avro exports work with the existing
# mozaggregator code. This requires `gcloud` to be configured to point at a
# sandbox project for reading data from `telemetry_stable`. There is a
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

    local table="${SOURCE_PROJECT}.telemetry_stable.${table_name}"

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
    SOURCE_PROJECT=${1?expected source project of BigQuery tables in 1st argument}
    DESTINATION_DATASET=${2?expected destination dataset of form project_id:dataset}
    OUTPUT_PREFIX=${3?expected gs:// output path in 3rd argument}
    TABLE_NAME=${4?expected table name in 4th argument}
    CHANNELS=${5?expected channels in the 5th argument}
    DATE=${6:-$(default_date)}

    query_to_destination "${TABLE_NAME}" "${CHANNELS}"
    extract_table "${TABLE_NAME}"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
