#!/usr/bin/env bash

set -ex

# Clone Feast repository into Jupyter container
git clone -b ${FEAST_REPOSITORY_VERSION} --single-branch https://github.com/gojek/feast.git || true

# Install CI requirements (only needed for running tests)
pip install -r feast/sdk/python/requirements-ci.txt

# Install Feast SDK
pip install -e feast/sdk/python -U

# Start Jupyter Notebook
start-notebook.sh --NotebookApp.token=''