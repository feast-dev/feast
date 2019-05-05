#!/usr/bin/env bash

bq --project_id=kf-feast rm -rf --dataset ${FEAST_WAREHOUSE_DATASET}
gsutil rm -r ${BATCH_IMPORT_DATA_GCS_PATH}
