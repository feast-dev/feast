#!/usr/bin/env bash

set -e
set -o pipefail

test -z ${GOOGLE_APPLICATION_CREDENTIALS} && GOOGLE_APPLICATION_CREDENTIALS="/etc/gcloud/service-account.json"
test -z ${SKIP_BUILD_JARS} && SKIP_BUILD_JARS="false"
test -z ${GOOGLE_CLOUD_PROJECT} && GOOGLE_CLOUD_PROJECT="kf-feast"
test -z ${TEMP_BUCKET} && TEMP_BUCKET="feast-templocation-kf-feast"
test -z ${JOBS_STAGING_LOCATION} && JOBS_STAGING_LOCATION="gs://${TEMP_BUCKET}/staging-location"

# Get the current build version using maven (and pom.xml)
export FEAST_BUILD_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo Building version: $FEAST_BUILD_VERSION

# Get Feast project repository root and scripts directory
export PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
export SCRIPTS_DIR=${PROJECT_ROOT_DIR}/infra/scripts

echo "
This script will run end-to-end tests for Feast Core and Online Serving.

1. Install Redis as the store for Feast Online Serving.
2. Install Postgres for persisting Feast metadata.
3. Install Kafka and Zookeeper as the Source in Feast.
4. Install Python 3.7.4, Feast Python SDK and run end-to-end tests from
   tests/e2e via pytest.
"

source ${SCRIPTS_DIR}/setup-common-functions.sh

install_test_tools
install_gcloud_sdk
install_and_start_local_redis_cluster
install_and_start_local_postgres
install_and_start_local_zookeeper_and_kafka

if [[ ${SKIP_BUILD_JARS} != "true" ]]; then
  build_feast_core_and_serving
else
  echo "[DEBUG] Skipping building jars"
fi

# Start Feast Core with auth if enabled
cat <<EOF > /tmp/jc.warehouse.application.yml
feast:
  core-host: localhost
  core-port: 6565
  jobs:
    polling_interval_milliseconds: 5000
    active_runner: direct
    runners:
      - name: direct
        type: DirectRunner
        options: {}
EOF

start_feast_core
start_feast_jobcontroller /tmp/jc.warehouse.application.yml

cat <<EOF > /tmp/serving.online.application.yml
feast:
  core-host: localhost
  core-grpc-port: 6565

  active_store: online

  # List of store configurations
  stores:
    - name: online # Name of the store (referenced by active_store)
      type: REDIS_CLUSTER # Type of the store. REDIS, BIGQUERY are available options
      config: 
        # Connection string specifies the IP and ports of Redis instances in Redis cluster
        connection_string: "localhost:7000,localhost:7001,localhost:7002,localhost:7003,localhost:7004,localhost:7005"
        flush_frequency_seconds: 1
      # Subscriptions indicate which feature tables needs to be retrieved and used to populate this store
      subscriptions:
        # Wildcards match all options. No filtering is done.
        - name: "*"
          project: "*"
          version: "*"

  tracing:
    enabled: false

spring:
  main:
    web-environment: false

EOF

start_feast_serving /tmp/serving.online.application.yml

install_python_with_miniconda_and_feast_sdk

print_banner "Running end-to-end tests with pytest at 'tests/e2e'"

# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts

ORIGINAL_DIR=$(pwd)
cd tests/e2e

set +e
CORE_NO=$(nproc --all)
pytest *.py -n ${CORE_NO} --redis-url localhost:7000 \
  --dist=loadscope --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
TEST_EXIT_CODE=$?

if [[ ${TEST_EXIT_CODE} != 0 ]]; then
  echo "[DEBUG] Printing logs"
  ls -ltrh /var/log/feast*
  cat /var/log/feast-serving-online.log /var/log/feast-core.log

  echo "[DEBUG] Printing Python packages list"
  pip list
fi

cd ${ORIGINAL_DIR}
exit ${TEST_EXIT_CODE}
