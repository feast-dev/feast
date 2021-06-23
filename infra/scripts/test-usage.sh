#!/usr/bin/env bash

set -e

# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts

cd sdk/python/
pip install -e ".[ci]"
cd usage_tests/
pytest --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
