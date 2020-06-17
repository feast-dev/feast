#!/usr/bin/env bash

set -e

export COMPOSE_ARGS=${COMPOSE_ARGS:-"-f docker-compose.yml -f docker-compose.online.yml -f docker-compose.databricks.yml"}

export FEAST_CORE_CONFIG=databricks.yml
export FEAST_DATABRICKS_EMULATOR_IMAGE=feast-databricks-emulator-dev

# Run e2e tests for Redis
export TEST_CMD="cd feast/tests/e2e/ && pytest -s -rA -x basic-ingest-redis-serving.py --core_url core:6565 --serving_url=online-serving:6566 --databricks"
infra/scripts/test-docker-compose.sh

# Run ingestion tests for FF Data Science scenarios
export TEST_CMD="cd feast/tests/ds_scenarios/ && pytest -s -rA -x test-ingest.py --core_url core:6565 --serving_url=online-serving:6566"
infra/scripts/test-docker-compose.sh
