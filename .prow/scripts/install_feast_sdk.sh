#!/usr/bin/env bash
set -e

# This script ensures latest Feast Python SDK and Feast CLI are installed

pip install -qe ${FEAST_HOME}/sdk/python
pip install -qr ${FEAST_HOME}/integration-tests/testutils/requirements.txt
go build -o /usr/local/bin/feast ./cli/feast &> /dev/null
feast config set coreURI ${FEAST_CORE_URL}
