#!/usr/bin/env bash

set -e

echo "
============================================================
Running Docker Compose tests with pytest at 'tests/e2e'
============================================================
"
LATEST_GH_COMMIT_SHA=$1

clean_up () {
    ARG=$?

    # Shut down docker-compose images
    docker-compose down

    exit $ARG
}

trap clean_up EXIT

export PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
export COMPOSE_INTERACTIVE_NO_CLI=1

# Create Docker Compose configuration file
cd ${PROJECT_ROOT_DIR}/infra/docker-compose/
cp .env.sample .env

# Replace FEAST_VERSION with latest github image SHA
export FEAST_VERSION=$LATEST_GH_COMMIT_SHA
echo "Testing docker-compose setup with version SHA, $FEAST_VERSION."

# Start Docker Compose containers
docker-compose up -d

# Get Jupyter container IP address
export JUPYTER_DOCKER_CONTAINER_IP_ADDRESS=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' feast_jupyter_1)

# Print Jupyter container information
docker inspect feast_jupyter_1
docker logs feast_jupyter_1

# Wait for Jupyter Notebook Container to come online
${PROJECT_ROOT_DIR}/infra/scripts/wait-for-it.sh ${JUPYTER_DOCKER_CONTAINER_IP_ADDRESS}:8888 --timeout=60

# Get Feast Core container IP address
export FEAST_CORE_CONTAINER_IP_ADDRESS=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' feast_core_1)

# Wait for Feast Core to be ready
${PROJECT_ROOT_DIR}/infra/scripts/wait-for-it.sh ${FEAST_CORE_CONTAINER_IP_ADDRESS}:6565 --timeout=120

# Get Feast Online Serving container IP address
export FEAST_ONLINE_SERVING_CONTAINER_IP_ADDRESS=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' feast_online_serving_1)

# Wait for Feast Online Serving to be ready
${PROJECT_ROOT_DIR}/infra/scripts/wait-for-it.sh ${FEAST_ONLINE_SERVING_CONTAINER_IP_ADDRESS}:6566 --timeout=120

# Run e2e tests for Redis
docker exec -e FEAST_VERSION=${FEAST_VERSION} feast_jupyter_1 bash \
-c 'cd /feast/tests/e2e && unset GOOGLE_APPLICATION_CREDENTIALS && pytest *.py -m "not bq" --ingestion-jar gs://feast-jobs/spark/ingestion/feast-ingestion-spark-${FEAST_VERSION}.jar --redis-url redis:6379 --core_url core:6565 --serving_url=online_serving:6566 --kafka_brokers=kafka:9092'
