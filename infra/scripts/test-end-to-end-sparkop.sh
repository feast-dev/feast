#!/usr/bin/env bash

set -euo pipefail

pip install "s3fs" "boto3" "urllib3>=1.25.4"

export DISABLE_FEAST_SERVICE_FIXTURES=1
export DISABLE_SERVICE_FIXTURES=1
export FEAST_TELEMETRY="False"

export FEAST_SPARK_K8S_NAMESPACE=sparkop

PYTHONPATH=sdk/python pytest tests/e2e/ \
      --feast-version develop \
      --core-url sparkop-feast-core:6565 \
      --serving-url sparkop-feast-online-serving:6566 \
      --env k8s \
      --staging-path $STAGING_PATH \
      --redis-url sparkop-redis-master.sparkop.svc.cluster.local:6379 \
      --kafka-brokers sparkop-kafka.sparkop.svc.cluster.local:9092 \
      -m "not bq"
