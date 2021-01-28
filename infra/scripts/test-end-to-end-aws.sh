#!/usr/bin/env bash

set -euo pipefail

pip install "s3fs" "boto3" "urllib3>=1.25.4"

export DISABLE_FEAST_SERVICE_FIXTURES=1
export DISABLE_SERVICE_FIXTURES=1
export FEAST_TELEMETRY="False"

PYTHONPATH=sdk/python pytest tests/e2e/ \
      --feast-version develop \
      --core-url cicd-feast-core:6565 \
      --serving-url cicd-feast-online-serving:6566 \
      --env aws \
      --emr-cluster-id $CLUSTER_ID \
      --staging-path $STAGING_PATH \
      --redis-url $NODE_IP:32379 \
      --emr-region us-west-2 \
      --kafka-brokers $NODE_IP:30092 \
      -m "not bq"
