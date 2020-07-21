#!/usr/bin/env bash

set -e

export COMPOSE_ARGS=${COMPOSE_ARGS:-"-f docker-compose.yml -f docker-compose.online.yml -f docker-compose.batch.yml -f docker-compose.databricks.yml"}

export FEAST_CORE_CONFIG=databricks.yml
export FEAST_BATCH_SERVING_CONFIG=databricks.yml
export FEAST_BATCH_JOB_STAGING_LOCATION=/tmp/databricks-emulator-staging

mkdir -p "$FEAST_BATCH_JOB_STAGING_LOCATION"

# Run e2e tests for Redis
export TEST_CMD="cd feast/tests/e2e/ && pytest -s -rA -x databricks-tests.py"
infra/scripts/test-docker-compose.sh

# Run ingestion tests for FF Data Science scenarios
export TEST_CMD="cd feast/tests/ds_scenarios/ && pytest -s -rA -x test-ingest.py"
infra/scripts/test-docker-compose.sh
