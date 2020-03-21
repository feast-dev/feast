#!/usr/bin/env bash

set -e

# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts

pip install -r sdk/python/requirements-ci.txt
make compile-protos-python
make lint-python

cd sdk/python/
pip install -e .
pytest --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
