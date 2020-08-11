#!/usr/bin/env bash

set -e
set -o pipefail
[[ $1 == "True" ]] && ENABLE_AUTH="true" || ENABLE_AUTH="false"
echo "Authenication enabled : ${ENABLE_AUTH}"

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
install_and_start_local_redis
install_and_start_local_postgres
install_and_start_local_zookeeper_and_kafka

if [[ ${SKIP_BUILD_JARS} != "true" ]]; then
  build_feast_core_and_serving
else
  echo "[DEBUG] Skipping building jars"
fi

# Start Feast Core with auth if enabled
cat <<EOF > /tmp/auth.core.warehouse.application.yml
feast:
  jobs:
    polling_interval_milliseconds: 5000
    active_runner: direct
    runners:
      - name: direct
        type: DirectRunner
        options: {}
  security:
    authentication:
      enabled: true
      provider: jwt
      options:
        jwkEndpointURI: "https://www.googleapis.com/oauth2/v3/certs"
    authorization:
      enabled: false
      provider: none
EOF

cat <<EOF > /tmp/core.warehouse.application.yml
feast:
  jobs:
    polling_interval_milliseconds: 5000
    active_runner: direct
    runners:
      - name: direct
        type: DirectRunner
        options: {}
EOF

cat <<EOF > /tmp/serving.warehouse.application.yml
feast:
  stores:
    - name: online
      type: REDIS
      config:
        host: localhost
        port: 6379
        flush_frequency_seconds: 1
      subscriptions:
        - name: "*"
          project: "*"
  core-authentication:
    enabled: $ENABLE_AUTH 
    provider: google 
  security:
    authentication:
      enabled: $ENABLE_AUTH
      provider: jwt
    authorization:
      enabled: false
      provider: none
EOF

if [[ ${ENABLE_AUTH} = "true" ]]; 
  then
    print_banner "Starting Feast core with auth"
    start_feast_core /tmp/auth.core.warehouse.application.yml
    print_banner "Starting Feast Serving with auth"
  else
    print_banner "Starting Feast core without auth"
    start_feast_core /tmp/core.warehouse.application.yml
    print_banner "Starting Feast Serving without auth"
fi


start_feast_serving /tmp/serving.warehouse.application.yml
install_python_with_miniconda_and_feast_sdk

print_banner "Running end-to-end tests with pytest at 'tests/e2e'"

# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts

ORIGINAL_DIR=$(pwd)
cd tests/e2e

set +e
export GOOGLE_APPLICATION_CREDENTIALS=/etc/gcloud/service-account.json
pytest redis/* --enable_auth=${ENABLE_AUTH} --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
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
