#!/usr/bin/env bash

set -euo pipefail

export DISABLE_FEAST_SERVICE_FIXTURES=1
export DISABLE_SERVICE_FIXTURES=1

export FEAST_SPARK_K8S_NAMESPACE=sparkop
export FEAST_S3_ENDPOINT_URL=http://minio.minio.svc.cluster.local:9000

# Used by tests
export AWS_S3_ENDPOINT_URL=http://minio.minio.svc.cluster.local:9000

cat << SPARK_CONF_END >/tmp/spark_conf.yml
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: "gcr.io/kf-feast/spark-py:v3.0.1"
  imagePullPolicy: Always
  sparkVersion: "3.0.1"
  timeToLiveSeconds: 3600
  pythonVersion: "3"
  sparkConf:
    "spark.hadoop.fs.s3a.endpoint": http://minio.minio.svc.cluster.local:9000
    "spark.hadoop.fs.s3a.path.style.access": "true"
    "spark.hadoop.fs.s3a.access.key": ${AWS_ACCESS_KEY_ID}
    "spark.hadoop.fs.s3a.secret.key": ${AWS_SECRET_ACCESS_KEY}
  restartPolicy:
    type: Never
  volumes:
    - name: "test-volume"
      hostPath:
        path: "/tmp"
        type: Directory
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
    labels:
      version: 3.0.1
    serviceAccount: spark
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    labels:
      version: 3.0.1
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
SPARK_CONF_END
export FEAST_SPARK_K8S_JOB_TEMPLATE_PATH=/tmp/spark_conf.yml

PYTHONPATH=sdk/python pytest tests/e2e/ \
      --feast-version develop \
      --core-url sparkop-feast-core:6565 \
      --serving-url sparkop-feast-online-serving:6566 \
      --env k8s \
      --staging-path s3a://feast-staging \
      --redis-url sparkop-redis-master.sparkop.svc.cluster.local:6379 \
      --kafka-brokers sparkop-kafka.sparkop.svc.cluster.local:9092 \
      -m "not bq"