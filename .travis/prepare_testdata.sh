#!/usr/bin/env bash

cd ${TRAVIS_BUILD_DIR}/integration-tests/testdata/import_specs
envsubst < batch_from_gcs.yaml.template > batch_from_gcs.yaml
envsubst < stream_from_kafka.yaml.template > stream_from_kafka.yaml

bq --project_id=kf-feast mk --dataset $FEAST_WAREHOUSE_DATASET
gsutil cp ${BATCH_IMPORT_DATA_LOCAL_PATH} ${BATCH_IMPORT_DATA_GCS_PATH}