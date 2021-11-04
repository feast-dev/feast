#!/usr/bin/env bash

# Skip Maven enforcer: https://stackoverflow.com/questions/50647223/maven-enforcer-issue-when-running-from-reactor-level
mvn -f java/pom.xml --projects sdk/java --batch-mode --define skipTests=true \
    --define enforcer.skip=true clean install
mvn -f java/pom.xml --projects sdk/java --define enforcer.skip=true test
TEST_EXIT_CODE=$?

# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts
cp -r java/sdk/java/target/surefire-reports ${LOGS_ARTIFACT_PATH}/surefire-reports

exit ${TEST_EXIT_CODE}