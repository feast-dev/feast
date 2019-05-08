#!/usr/bin/env bash

cd ${TRAVIS_BUILD_DIR}/integration-tests
feast apply entity testdata/entity_specs/entity_2.yaml
feast apply feature testdata/feature_specs/entity_2.feature_*.yaml
feast jobs run testdata/import_specs/stream_from_kafka.yaml &

sleep 20

python -m testutils.kafka_producer \
--bootstrap_servers=${KAFKA_BROKERS} \
--topic=${KAFKA_TOPICS} \
--entity_spec_file=testdata/entity_specs/entity_2.yaml \
--feature_spec_files=testdata/feature_specs/entity_2*.yaml \
--feature_values_file=testdata/feature_values/ingestion_2.csv

sleep 20

python -m testutils.validate_feature_values \
--entity_spec_file=testdata/entity_specs/entity_2.yaml \
--feature_spec_files=testdata/feature_specs/entity_2*.yaml \
--expected-serving-values-file=testdata/feature_values/serving_2.csv \
--feast-serving-url=${FEAST_SERVING_URI}