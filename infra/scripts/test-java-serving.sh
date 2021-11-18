#!/usr/bin/env bash

infra/scripts/download-maven-cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.2019-10-24.tar \
    --output-dir /root/

mvn -f java/pom.xml --batch-mode --also-make --projects serving test
TEST_EXIT_CODE=$?

# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts
cp -r serving/target/surefire-reports ${LOGS_ARTIFACT_PATH}/surefire-reports

exit ${TEST_EXIT_CODE}
