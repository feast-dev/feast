#!/usr/bin/env bash

set -e

echo "============================================================"
echo "Installing Feast Release"
echo "============================================================"

helm install --name ${FEAST_RELEASE_NAME} --wait --timeout 210 ${FEAST_HOME}/charts/feast -f integration-tests/feast-helm-values.yaml

echo "============================================================"
echo "Testing Batch Import"
echo "============================================================"

cd ${FEAST_HOME}/integration-tests/testdata

feast apply entity entity_specs/entity_1.yaml
feast apply feature feature_specs/entity_1*.yaml
feast jobs run import_specs/batch_from_gcs.yaml --wait

cd $FEAST_HOME/integration-tests

python -m testutils.validate_feature_values \
  --entity_spec_file=testdata/entity_specs/entity_1.yaml \
  --feature_spec_files=testdata/feature_specs/entity_1*.yaml \
  --expected-warehouse-values-file=testdata/feature_values/ingestion_1.csv \
  --expected-serving-values-file=testdata/feature_values/serving_1.csv \
  --bigquery-dataset-for-warehouse=${FEAST_WAREHOUSE_DATASET} \
  --feast-serving-url=${FEAST_SERVING_URL}

echo "============================================================"
echo "Testing Streaming Import"
echo "============================================================"

cd $FEAST_HOME/integration-tests/testdata

feast apply entity entity_specs/entity_2.yaml
feast apply feature feature_specs/entity_2*.yaml
feast jobs run import_specs/stream_from_kafka.yaml &

IMPORT_JOB_PID=$!
sleep 20

cd $FEAST_HOME/integration-tests

python -m testutils.kafka_producer \
  --bootstrap_servers=$KAFKA_BROKERS \
  --topic=$KAFKA_TOPICS \
  --entity_spec_file=testdata/entity_specs/entity_2.yaml \
  --feature_spec_files=testdata/feature_specs/entity_2*.yaml \
  --feature_values_file=testdata/feature_values/ingestion_2.csv
sleep 20

python -m testutils.validate_feature_values \
  --entity_spec_file=testdata/entity_specs/entity_2.yaml \
  --feature_spec_files=testdata/feature_specs/entity_2*.yaml \
  --expected-serving-values-file=testdata/feature_values/serving_2.csv \
  --feast-serving-url=$FEAST_SERVING_URL

kill -9 ${IMPORT_JOB_PID}
