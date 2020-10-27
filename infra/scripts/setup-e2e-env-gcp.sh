#!/usr/bin/env bash

# GCloud, kubectl, helm should be already installed
# And kubernetes cluster already configured

test -z ${GCLOUD_REGION} && GCLOUD_REGION="us-central1"
test -z ${GCLOUD_NETWORK} && GCLOUD_NETWORK="default"
test -z ${GCLOUD_SUBNET} && GCLOUD_SUBNET="default"


feast_kafka_ip_name="feast-kafka"
feast_redis_1_ip_name="feast-redis-1"
feast_redis_2_ip_name="feast-redis-2"
feast_redis_3_ip_name="feast-redis-3"

helm repo add bitnami https://charts.bitnami.com/bitnami

gcloud compute addresses create \
      $feast_kafka_ip_name $feast_redis_1_ip_name $feast_redis_2_ip_name $feast_redis_3_ip_name \
      --region ${GCLOUD_REGION} --subnet ${GCLOUD_SUBNET}

export feast_kafka_ip=$(gcloud compute addresses describe $feast_kafka_ip_name --region=${GCLOUD_REGION} --format "value(address)")
export feast_redis_1_ip=$(gcloud compute addresses describe $feast_redis_1_ip_name --region=${GCLOUD_REGION} --format "value(address)")
export feast_redis_2_ip=$(gcloud compute addresses describe $feast_redis_2_ip_name --region=${GCLOUD_REGION} --format "value(address)")
export feast_redis_3_ip=$(gcloud compute addresses describe $feast_redis_3_ip_name --region=${GCLOUD_REGION} --format "value(address)")


envsubst '$feast_kafka_ip' < helm/kafka-values.tpl.yaml > helm/kafka-values.yaml
envsubst '$feast_redis_1_ip,$feast_redis_2_ip,$feast_redis_3_ip' < helm/redis-cluster-values.tpl.yaml > helm/redis-cluster-values.yaml

helm install e2e-kafka bitnami/kafka \
  --values helm/kafka-values.yaml --namespace infra --create-namespace

helm install e2e-redis-cluster bitnami/redis-cluster \
  --values helm/redis-cluster-values.yaml --namespace infra \
  --create-namespace