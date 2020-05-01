#!/usr/bin/env bash

set -e

echo "
============================================================
Running Docker Compose tests with pytest at 'tests/e2e'
============================================================
"
PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
cd ${PROJECT_ROOT_DIR}/infra/docker-compose/

# Create Docker Compose configuration file
cp .env.sample .env

# Start all Docker Compose containers
docker-compose -f docker-compose.yml -f docker-compose.online.yml up -d

# Get Docker Container IP address
JUPYTER_DOCKER_CONTAINER_IP_ADDRESS=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' feast_jupyter_1)

# Wait for Jupyter Notebook Container to be online
timeout 5m bash -c 'until [ $(curl -s ${JUPYTER_DOCKER_CONTAINER_IP_ADDRESS}:8888/tree | grep notebook | wc -l) -gt 0 ]; do echo waiting for Jupyter to start...; curl -s ${JUPYTER_DOCKER_CONTAINER_IP_ADDRESS}:8888/tree; sleep 10; done'

# Run e2e tests for Redis
docker exec -it feast_jupyter_1  bash -c 'cd feast/tests/e2e/ && pytest -s basic-ingest-redis-serving.py --core_url core:6565 --serving_url=online-serving:6566'