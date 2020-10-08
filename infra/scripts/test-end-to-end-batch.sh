#!/usr/bin/env bash

set -e
set -o pipefail

PYTEST_MARK='direct_runner' #default

print_usage() {
  printf "Usage: ./test-end-to-end-batch -m pytest_mark"
}

while getopts 'm:' flag; do
  case "${flag}" in
    m) PYTEST_MARK="${OPTARG}" ;;
    *) print_usage
       exit 1 ;;
  esac
done

test -z ${GOOGLE_APPLICATION_CREDENTIALS} && GOOGLE_APPLICATION_CREDENTIALS="/etc/service-account/service-account.json"
test -z ${SKIP_BUILD_JARS} && SKIP_BUILD_JARS="false"
test -z ${GOOGLE_CLOUD_PROJECT} && GOOGLE_CLOUD_PROJECT="kf-feast"
test -z ${TEMP_BUCKET} && TEMP_BUCKET="feast-templocation-kf-feast"
test -z ${JOBS_STAGING_LOCATION} && JOBS_STAGING_LOCATION="gs://${TEMP_BUCKET}/staging-location/$(date +%s)"

# Get the current build version using maven (and pom.xml)
export FEAST_BUILD_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo Building version: $FEAST_BUILD_VERSION

# Get Feast project repository root and scripts directory
export PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
export SCRIPTS_DIR=${PROJECT_ROOT_DIR}/infra/scripts

echo "
This script will run end-to-end tests for Feast Core and Batch Serving.

1. Install gcloud SDK
2. Install Redis as the job store for Feast Batch Serving.
4. Install Postgres for persisting Feast metadata.
5. Install Kafka and Zookeeper as the Source in Feast.
6. Install Python 3.7.4, Feast Python SDK and run end-to-end tests from
   tests/e2e via pytest.
"

source ${SCRIPTS_DIR}/setup-common-functions.sh

install_test_tools
install_gcloud_sdk
install_and_start_local_redis
install_and_start_local_postgres
install_and_start_local_zookeeper_and_kafka

if [[ ${SKIP_BUILD_JARS} != "true" ]]; then
  build_feast_core_and_serving
else
  echo "[DEBUG] Skipping building jars"
fi

DATASET_NAME=feast_$(date +%s)
bq --location=US --project_id=${GOOGLE_CLOUD_PROJECT} mk \
  --dataset \
  --default_table_expiration 86400 \
  ${GOOGLE_CLOUD_PROJECT}:${DATASET_NAME}

# Start Feast Core in background
cat <<EOF > /tmp/jc.warehouse.application.yml
feast:
  core-host: localhost
  core-port: 6565
  jobs:
    polling_interval_milliseconds: 10000
    active_runner: direct
    consolidate-jobs-per-source: true
    runners:
      - name: direct
        type: DirectRunner
        options:
          tempLocation: gs://${TEMP_BUCKET}/tempLocation

EOF

cat <<EOF > /tmp/serving.warehouse.application.yml
feast:
  # GRPC service address for Feast Core
  # Feast Serving requires connection to Feast Core to retrieve and reload Feast metadata (e.g. FeatureSpecs, Store information)
  core-host: localhost
  core-grpc-port: 6565

  # Indicates the active store. Only a single store in the last can be active at one time. In the future this key
  # will be deprecated in order to allow multiple stores to be served from a single serving instance
  active_store: historical

  # List of store configurations
  stores:
    - name: historical
      type: BIGQUERY
      config:
        project_id: ${GOOGLE_CLOUD_PROJECT}
        dataset_id: ${DATASET_NAME}
        staging_location: ${JOBS_STAGING_LOCATION}
        initial_retry_delay_seconds: 1
        total_timeout_seconds: 21600
        write_triggering_frequency_seconds: 1
      subscriptions:
        - name: "*"
          project: "*"
          version: "*"

  job_store:
    redis_host: localhost
    redis_port: 6379

  tracing:
    enabled: false

server:
  port: 8081

EOF

cat /tmp/jc.warehouse.application.yml /tmp/serving.warehouse.application.yml

start_feast_core
start_feast_jobcontroller /tmp/jc.warehouse.application.yml
start_feast_serving /tmp/serving.warehouse.application.yml

install_python_with_miniconda_and_feast_sdk

print_banner "Running end-to-end tests with pytest at 'tests/e2e'"
# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts

ORIGINAL_DIR=$(pwd)
cd tests/e2e

set +e
pytest bq/* -v -m ${PYTEST_MARK} --gcs_path ${JOBS_STAGING_LOCATION} --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
TEST_EXIT_CODE=$?

if [[ ${TEST_EXIT_CODE} != 0 ]]; then
  echo "[DEBUG] Printing logs"
  ls -ltrh /var/log/feast*
  cat /var/log/feast-serving-online.log /var/log/feast-core.log /var/log/feast-jobcontroller.log

  echo "[DEBUG] Printing Python packages list"
  pip list
else
  print_banner "Cleaning up"

  bq rm -r -f ${GOOGLE_CLOUD_PROJECT}:${DATASET_NAME}
fi

exit ${TEST_EXIT_CODE}
