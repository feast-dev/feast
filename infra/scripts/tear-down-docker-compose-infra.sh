#!/usr/bin/env bash

set -e

if [ -z "$TEST_RUN_ID" ] ; then echo "TEST_RUN_ID must be set"; exit 1; fi
test -z ${BIGQUERY_DATASET_NAME} && BIGQUERY_DATASET_NAME=$TEST_RUN_ID
test -z ${GCLOUD_PROJECT} && GCLOUD_PROJECT="kf-feast"

printenv

export PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)

gcloud auth list
gcloud config list

# Shut down containers
cd "${PROJECT_ROOT_DIR}"/infra/docker-compose/
docker-compose down

# Delete BigQuery dataset only when execution is successful
if [ "$?" -eq 0 ]; then
  bq rm -r --project_id ${GCLOUD_PROJECT} --force "${BIGQUERY_DATASET_NAME}"
fi