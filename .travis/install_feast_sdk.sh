#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# This assumes Feast CLI has been built and pushed to GCS in the previous build step
gsutil cp ${FEAST_CLI_GCS_URI} ${TRAVIS_BUILD_DIR}/cli/build/feast
chmod +x ${TRAVIS_BUILD_DIR}/cli/build/feast
export PATH=${TRAVIS_BUILD_DIR}/cli/build:$PATH
feast config set coreURI ${FEAST_CORE_URI}

pip install -qe ${TRAVIS_BUILD_DIR}/sdk/python
pip install -qr ${TRAVIS_BUILD_DIR}/integration-tests/testutils/requirements.txt
export GOOGLE_APPLICATION_CREDENTIALS=${SCRIPT_DIR}/service_account.json
