#!/usr/bin/env bash

set -e

echo "
============================================================
Running Docker Compose tests with pytest at 'tests/e2e'
============================================================
"

clean_up () {
    ARG=$?

    # Shut down docker-compose images
    docker-compose -f docker-compose.yml -f docker-compose.online.yml down

    # Remove configuration file
    rm -f .env

    exit $ARG
}

trap clean_up EXIT

export PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
export COMPOSE_INTERACTIVE_NO_CLI=1

# Create Docker Compose configuration file
cd ${PROJECT_ROOT_DIR}/infra/docker-compose/
cp .env.sample .env

# Start Docker Compose containers
docker-compose -f docker-compose.yml -f docker-compose.online.yml up -d

# Get Jupyter container IP address
export JUPYTER_DOCKER_CONTAINER_IP_ADDRESS=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' feast_jupyter_1)

# Print Jupyter container information
docker logs feast_jupyter_1

# Wait for Jupyter Notebook Container to come online
${PROJECT_ROOT_DIR}/infra/scripts/wait-for-it.sh ${JUPYTER_DOCKER_CONTAINER_IP_ADDRESS}:8888 --timeout=300

# Wait for Feast Core to come online
docker exec feast_core_1 grpc-health-probe -addr :6565 -connect-timeout 300s

# Run e2e tests for Redis
docker exec feast_jupyter_1 bash -c 'cd feast/tests/e2e/ && pytest -s basic-ingest-redis-serving.py --core_url core:6565 --serving_url=online-serving:6566'

# Run ingestion tests for FF Data Science scenarios
docker exec feast_jupyter_1 bash -c 'cd feast/tests/ds_scenarios/ && pytest -s test-ingest.py --core_url core:6565 --serving_url=online-serving:6566'
