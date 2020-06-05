#!/usr/bin/env bash

set -ex

# Install Python dependencies
make -C feast/ compile-protos-python

# Install CI requirements (only needed for running tests)
pip install -r feast/sdk/python/requirements-ci.txt

# Install Feast SDK
pip install -e feast/sdk/python -U

# Start Jupyter Notebook
start-notebook.sh --NotebookApp.token=''