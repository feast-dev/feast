#!/usr/bin/env bash

set -o pipefail

make lint-go

cd sdk/go
go test -v 2>&1 | tee /tmp/test_output
TEST_EXIT_CODE=$?

# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts

go get -u github.com/jstemmer/go-junit-report
cat /tmp/test_output | ${GOPATH}/bin/go-junit-report > ${LOGS_ARTIFACT_PATH}/golang-sdk-test-report.xml

exit ${TEST_EXIT_CODE}