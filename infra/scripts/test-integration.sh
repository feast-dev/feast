#!/usr/bin/env bash

python -m pip install --upgrade pip setuptools wheel pip-tools
make install-python
python -m pip install -qr tests/requirements.txt

export FEAST_USAGE="False"
pytest tests/integration --dataproc-cluster-name feast-e2e --dataproc-project kf-feast --dataproc-region us-central1  --dataproc-staging-location gs://feast-templocation-kf-feast
