#!/usr/bin/env bash
set -e

bq -q rm -rf --dataset ${FEAST_WAREHOUSE_DATASET}
gsutil -q rm ${BATCH_IMPORT_DATA_GCS_PATH}
helm delete --purge $FEAST_RELEASE_NAME
