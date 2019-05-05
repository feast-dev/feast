#!/usr/bin/env bash

COLOR="\033[0;32m$(tput bold)"
NO_COLOR="\033[0m$(tput sgr0)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo -e "${COLOR}Starting Postgres...${NO_COLOR}"
docker run --net=host --env=POSTGRES_PASSWORD=password --detach postgres:11.2-alpine

echo -e "${COLOR}Starting Redis...${NO_COLOR}"
docker run --net=host --detach redis:5.0-alpine

echo -e "${COLOR}Starting Zookeeper...${NO_COLOR}"
docker run --net=host --env=ZOOKEEPER_CLIENT_PORT=2181 \
--detach confluentinc/cp-zookeeper:5.2.1

echo -e "${COLOR}Starting Kafka...${NO_COLOR}"
docker run --net=host \
--env=KAFKA_ZOOKEEPER_CONNECT=localhost:2181 \
--env=KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
--env=KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
--detach confluentinc/cp-kafka:5.2.1

echo -e "${COLOR}Starting Feast Core...${NO_COLOR}"
docker run --name core --net host \
--env=JOB_WORKSPACE=/tmp \
--env=STORE_WAREHOUSE_TYPE=bigquery \
--env=STORE_WAREHOUSE_OPTIONS='{"project":"kf-feast","dataset":"'${FEAST_WAREHOUSE_DATASET}'","tempLocation":"gs://feast-templocation-kf-feast"}' \
--env=STORE_SERVING_TYPE=redis \
--env=STORE_SERVING_OPTIONS='{"host": "localhost", "port": "6379"}' \
--env=GOOGLE_APPLICATION_CREDENTIALS=/etc/google_cloud/service_account.json \
--volume=${SCRIPT_DIR}/service_account.json:/etc/google_cloud/service_account.json \
--detach us.gcr.io/kf-feast/feast-core:${FEAST_IMAGE_TAG}
sleep 20

echo -e "${COLOR}Starting Feast Serving...${NO_COLOR}"
docker run --name serving --net host \
--env=FEAST_SERVING_HTTP_PORT=8081  \
--detach us.gcr.io/kf-feast/feast-serving:${FEAST_IMAGE_TAG}
