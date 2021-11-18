#!/usr/bin/env bash

apt-get -qq update
apt-get -y install build-essential

make lint-java

infra/scripts/download-maven-cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.2019-10-24.tar \
    --output-dir /root/

# Core depends on Ingestion so they are tested together
# Skip Maven enforcer: https://stackoverflow.com/questions/50647223/maven-enforcer-issue-when-running-from-reactor-level
mvn -f java/pom.xml --projects core,ingestion --batch-mode --define skipTests=true \
    --define enforcer.skip=true clean install
mvn -f java/pom.xml --projects core,ingestion --define enforcer.skip=true test
TEST_EXIT_CODE=$?

# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts
mkdir -p ${LOGS_ARTIFACT_PATH}/surefire-reports
cp core/target/surefire-reports/* ${LOGS_ARTIFACT_PATH}/surefire-reports/
cp ingestion/target/surefire-reports/* ${LOGS_ARTIFACT_PATH}/surefire-reports/

exit ${TEST_EXIT_CODE}