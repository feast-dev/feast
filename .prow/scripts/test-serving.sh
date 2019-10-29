#!/usr/bin/env bash

.prow/scripts/download-maven-cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.2019-10-24.tar \
    --output-dir /root/

# Skip Maven enforcer: https://stackoverflow.com/questions/50647223/maven-enforcer-issue-when-running-from-reactor-level
mvn --projects serving --batch-mode --define skipTests=true \
    --define enforcer.skip=true clean install
mvn --projects serving --define enforcer.skip=true test
TEST_EXIT_CODE=$?

# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts
cp -r serving/target/surefire-reports ${LOGS_ARTIFACT_PATH}/surefire-reports

exit ${TEST_EXIT_CODE}