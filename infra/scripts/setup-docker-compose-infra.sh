#!/usr/bin/env bash

set -e

if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ] ; then echo "GOOGLE_APPLICATION_CREDENTIALS must be set"; exit 1;  fi
if [ -z "$FEAST_VERSION" ] ; then echo "FEAST_VERSION must be set"; exit 1; fi
if [ -z "$TEMP_BUCKET" ] ; then echo "TEMP_BUCKET must be set"; exit 1; fi
if [ -z "$TEST_RUN_ID" ] ; then echo "TEST_RUN_ID must be set"; exit 1; fi
if [ -z "$ALLOW_DELETE_BQ_DATASET" ] ; then echo "ALLOW_DELETE_BQ_DATASET must be set"; exit 1; fi

test -z ${BIGQUERY_DATASET_NAME} && BIGQUERY_DATASET_NAME=$TEST_RUN_ID
test -z ${GCLOUD_PROJECT} && GCLOUD_PROJECT="kf-feast"

export PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
export SCRIPTS_DIR=${PROJECT_ROOT_DIR}/infra/scripts
source ${SCRIPTS_DIR}/setup-common-functions.sh

# Wait for docker images
wait_for_docker_image gcr.io/kf-feast/feast-core:"${FEAST_VERSION}"
wait_for_docker_image gcr.io/kf-feast/feast-serving:"${FEAST_VERSION}"
wait_for_docker_image gcr.io/kf-feast/feast-jupyter:"${FEAST_VERSION}"

# Print gcloud information
gcloud auth list
gcloud config list

# Delete BigQuery dataset if it exists
if [ "${ALLOW_DELETE_BQ_DATASET}" == "true" ]; then
  bq rm -r --project_id ${GCLOUD_PROJECT} --force "${BIGQUERY_DATASET_NAME}"  || true
fi

# Create BigQuery dataset from scratch
bq --location=US --project_id=${GCLOUD_PROJECT} mk \
  --dataset \
  --default_table_expiration 86400 \
  ${GCLOUD_PROJECT}:${BIGQUERY_DATASET_NAME}

# Setup docker compose
cd "${PROJECT_ROOT_DIR}"/infra/docker-compose/
cp .env.sample .env

# Update configuration for historical serving
HISTORICAL_SERVING_CONFIG=$(mktemp)
cat <<EOF > ${HISTORICAL_SERVING_CONFIG}
feast:
  core-host: core
  active_store: historical
  stores:
    - name: historical
      type: BIGQUERY
      config:
        project_id: ${GCLOUD_PROJECT}
        dataset_id: ${BIGQUERY_DATASET_NAME}
        staging_location: ${TEMP_BUCKET}/staging
        initial_retry_delay_seconds: 2
        total_timeout_seconds: 21600
        write_triggering_frequency_seconds: 1
      subscriptions:
        - name: "*"
          project: "*"

  job_store:
    redis_host: redis
    redis_port: 6379

grpc:
  server:
    port: 6567
EOF


# Update configuration for feast core
CORE_CONFIG_FILE=$(mktemp)
cat <<EOF > "${CORE_CONFIG_FILE}"
feast:
  jobs:
    polling_interval_milliseconds: 10000
    active_runner: direct
    consolidate-jobs-per-source: true
    runners:
      - name: direct
        type: DirectRunner
        options:
          tempLocation: ${TEMP_BUCKET}/staging

  stream:
    type: kafka
    options:
      topic: feast-features
      bootstrapServers: "kafka:9092,localhost:9094"
      replicationFactor: 1
      partitions: 1
    specsOptions:
      specsTopic: feast-specs
      specsAckTopic: feast-specs-ack
      notifyIntervalMilliseconds: 1000
EOF

printenv
echo
cat .env
echo
cat ${HISTORICAL_SERVING_CONFIG}
cat ${CORE_CONFIG_FILE}

# Start Docker Compose containers
FEAST_HISTORICAL_SERVING_CONFIG=${HISTORICAL_SERVING_CONFIG} \
FEAST_CORE_CONFIG=${CORE_CONFIG_FILE} \
FEAST_HISTORICAL_SERVING_ENABLED=true \
GCP_SERVICE_ACCOUNT=${GOOGLE_APPLICATION_CREDENTIALS} \
FEAST_VERSION=${FEAST_VERSION} \
docker-compose up -d