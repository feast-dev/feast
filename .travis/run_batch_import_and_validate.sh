#!/usr/bin/env bash

cd ${TRAVIS_BUILD_DIR}/integration-tests

feast apply entity testdata/entity_specs/entity_1.yaml
feast apply feature testdata/feature_specs/entity_1.feature_*.yaml
feast jobs run testdata/import_specs/batch_from_gcs.yaml --wait

python -m testutils.validate_feature_values \
--entity_spec_file=testdata/entity_specs/entity_1.yaml \
--feature_spec_files=testdata/feature_specs/entity_1*.yaml \
--expected-warehouse-values-file=testdata/feature_values/ingestion_1.csv \
--expected-serving-values-file=testdata/feature_values/serving_1.csv \
--bigquery-dataset-for-warehouse=${FEAST_WAREHOUSE_DATASET} \
--feast-serving-url=${FEAST_SERVING_URI} \
--project=kf-feast