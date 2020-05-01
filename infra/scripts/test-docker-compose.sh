#!/usr/bin/env bash

set -e
set -o pipefail

echo "
============================================================
Running Basic Redis end-to-end tests with pytest at 'tests/e2e'
============================================================
"
PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
cd ${PROJECT_ROOT_DIR}/infra/docker-compose/
cp .env.sample .env
docker-compose -f docker-compose.yml -f docker-compose.online.yml up -d
timeout 3m bash -c 'until [ $(curl -s localhost:8888/tree | grep notebook | wc -l) -gt 0 ]; do echo waiting for Jupyter to start..; sleep 10; done'
docker exec -it feast_jupyter_1  bash -c 'cd feast/tests/e2e/ && pytest -s basic-ingest-redis-serving.py --core_url core:6565 --serving_url=online-serving:6566'