#!/usr/bin/env bash
set -e

usage()
{
    echo "usage: prepare_integration_test.sh [--skip-build true]"
}

while [ "$1" != "" ]; do
  case "$1" in
      --skip-build )       SKIP_BUILD=true;              shift;;
      * )                  usage; exit 1
  esac
  shift
done

# Authenticate to Google Cloud and GKE
# ============================================================
GOOGLE_PROJECT_ID=kf-feast
KUBE_CLUSTER_NAME=primary-test-cluster
KUBE_CLUSTER_ZONE=us-central1-a
KEY_FILE=/etc/service-account/service-account.json

gcloud -q auth activate-service-account --key-file=${KEY_FILE}
gcloud -q auth configure-docker
gcloud -q config set project ${GOOGLE_PROJECT_ID}
gcloud -q container clusters get-credentials ${KUBE_CLUSTER_NAME} --zone ${KUBE_CLUSTER_ZONE} --project ${GOOGLE_PROJECT_ID}
export GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE}

# Install Python 3.6, Golang 1.12, Helm and Feast SDK
# ============================================================
. .prow/scripts/install_test_tools.sh
. .prow/scripts/install_feast_sdk.sh
.prow/scripts/prepare_maven_cache.sh --archive-uri gs://feast-templocation-kf-feast/.m2.tar --output-dir ${FEAST_HOME}

# Prepare Feast test data and config
# ============================================================

bq -q mk --dataset ${FEAST_WAREHOUSE_DATASET}
gsutil -q cp ${FEAST_HOME}/integration-tests/testdata/feature_values/ingestion_1.csv ${BATCH_IMPORT_DATA_GCS_PATH}

BUILD_ID=${BUILD_ID:0:5}
envsubst < integration-tests/feast-helm-values.yaml.template > integration-tests/feast-helm-values.yaml
cd ${FEAST_HOME}/integration-tests/testdata/import_specs
envsubst < batch_from_gcs.yaml.template > batch_from_gcs.yaml
envsubst < stream_from_kafka.yaml.template > stream_from_kafka.yaml

if [[ ! ${SKIP_BUILD} ]]; then

echo "============================================================"
echo "Building Feast for Testing"
echo "============================================================"
cd ${FEAST_HOME}
docker build -t us.gcr.io/kf-feast/feast-core:${FEAST_IMAGE_TAG} -f Dockerfiles/core/Dockerfile . &
docker build -t us.gcr.io/kf-feast/feast-serving:${FEAST_IMAGE_TAG} -f Dockerfiles/serving/Dockerfile . &
wait
docker push us.gcr.io/kf-feast/feast-core:${FEAST_IMAGE_TAG} &
docker push us.gcr.io/kf-feast/feast-serving:${FEAST_IMAGE_TAG} &
wait

fi

# Switch back context to original directory
set +ex
cd ${FEAST_HOME}