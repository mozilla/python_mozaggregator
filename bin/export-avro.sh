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
print(ds.replace("-",""))
END
}

function extract_table() {
    local table_name=$1
    local suffix='*.avro'

    # date partition must be escaped
    local table="${SOURCE_PROJECT}:payload_bytes_decoded.telemetry_telemetry__${table_name}\$${DATE}" 
    local output="${OUTPUT_PREFIX}/${SOURCE_PROJECT}/${DATE}/${table_name}/${suffix}"
    
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
    OUTPUT_PREFIX=${2?expected gs:// output path in second argument}
    DATE=${3:-$(default_date)}

    extract_table "main_v4"
    extract_table "saved_session_v4"
    extract_table "mobile_metrics_v1"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
