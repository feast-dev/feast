#!/usr/bin/env bash

# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts

set -o pipefail

usage()
{
    echo "Run test on a Feast component.

Usage: run_test.sh --component <feast_component>

<feast_component> is one of:
- core-ingestion (core depends on ingestion so they are tested together)
- serving 
- java-sdk
- python-sdk
- golang-sdk

This script also runs commands before and after the main test task, such as:
- Download cached Maven packages for faster tests
- Saving the test output report so it can be viewed with Spyglass UI in Prow.
  By default, the configured log path is "/logs" and test artifacts should
  be saved to "/logs/artifacts" directory.
"
}

while [ "$1" != "" ]; do
  case "$1" in
      --component )       COMPONENT="$2";    shift;;
      * )                 usage; exit 1
  esac
  shift
done

if [[ ! ${COMPONENT} ]]; then 
  usage; exit 1; 
fi

if [[ ${COMPONENT} == "core-ingestion" ]]; then

  .prow/scripts/prepare_maven_cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.2019-10-24.tar --output-dir /root/

  # Core depends on Ingestion so they are tested together
  # Skip Maven enforcer: https://stackoverflow.com/questions/50647223/maven-enforcer-issue-when-running-from-reactor-level
  mvn --projects core,ingestion --batch-mode --define skipTests=true --define enforcer.skip=true clean install
  mvn --projects core,ingestion --define enforcer.skip=true test
  TEST_EXIT_CODE=$?

  mkdir -p ${LOGS_ARTIFACT_PATH}/surefire-reports
  cp core/target/surefire-reports/* ${LOGS_ARTIFACT_PATH}/surefire-reports/
  cp ingestion/target/surefire-reports/* ${LOGS_ARTIFACT_PATH}/surefire-reports/

elif [[ ${COMPONENT} == "serving" ]]; then

  .prow/scripts/prepare_maven_cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.2019-10-24.tar --output-dir /root/

  # Skip Maven enforcer: https://stackoverflow.com/questions/50647223/maven-enforcer-issue-when-running-from-reactor-level
  mvn --projects serving --batch-mode --define skipTests=true --define enforcer.skip=true clean install
  mvn --projects serving --define enforcer.skip=true test
  TEST_EXIT_CODE=$?

  cp -r serving/target/surefire-reports ${LOGS_ARTIFACT_PATH}/surefire-reports

elif [[ ${COMPONENT} == "java-sdk" ]]; then

  .prow/scripts/prepare_maven_cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.2019-10-24.tar --output-dir /root/

  # Skip Maven enforcer: https://stackoverflow.com/questions/50647223/maven-enforcer-issue-when-running-from-reactor-level
  mvn --projects sdk/java --batch-mode --define skipTests=true --define enforcer.skip=true clean install
  mvn --projects sdk/java --define enforcer.skip=true test
  TEST_EXIT_CODE=$?

  cp -r sdk/java/target/surefire-reports ${LOGS_ARTIFACT_PATH}/surefire-reports

elif [[ ${COMPONENT} == "python-sdk" ]]; then

  cd sdk/python
  pip install -r requirements-test.txt
  pip install .
  pytest --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
  TEST_EXIT_CODE=$?

elif [[ ${COMPONENT} == "golang-sdk" ]]; then

  cd sdk/go
  go test -v 2>&1 | tee /tmp/test_output
  TEST_EXIT_CODE=$?

  go get -u github.com/jstemmer/go-junit-report
  cat /tmp/test_output | ${GOPATH}/bin/go-junit-report > ${LOGS_ARTIFACT_PATH}/golang-sdk-test-report.xml

elif [[ ${COMPONENT} == "end-to-end" ]]; then

  apt-get -qq update
  apt-get -y install redis-server
  apt-get -y install postgresql
  service postgresql start
  redis-server --daemonize yes

  wget -qO- https://www-eu.apache.org/dist/kafka/2.3.0/kafka_2.12-2.3.0.tgz | tar xz
  cd kafka_2.12-2.3.0/
  nohup bin/zookeeper-server-start.sh -daemon config/zookeeper.properties > /dev/null 2>&1 &
  sleep 5
  nohup bin/kafka-server-start.sh -daemon config/server.properties > /dev/null 2>&1 &
  sleep 5

  .prow/scripts/prepare_maven_cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.2019-10-24.tar --output-dir /root/
  
  mvn --batch-mode --define skipTests=true clean package


else
  usage; exit 1
fi

exit ${TEST_EXIT_CODE}