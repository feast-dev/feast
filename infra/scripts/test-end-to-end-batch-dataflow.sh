#!/usr/bin/env bash
echo "Preparing environment variables..."

set -e
set -o pipefail

test -z ${GOOGLE_APPLICATION_CREDENTIALS} && GOOGLE_APPLICATION_CREDENTIALS="/etc/service-account-df/service-account-df.json"
test -z ${GCLOUD_PROJECT} && GCLOUD_PROJECT="kf-feast"
test -z ${GCLOUD_REGION} && GCLOUD_REGION="us-central1"
test -z ${GCLOUD_NETWORK} && GCLOUD_NETWORK="default"
test -z ${GCLOUD_SUBNET} && GCLOUD_SUBNET="default"
test -z ${TEMP_BUCKET} && TEMP_BUCKET="kf-feast-dataflow-temp"
test -z ${K8_CLUSTER_NAME} && K8_CLUSTER_NAME="feast-e2e-dataflow"
test -z ${HELM_RELEASE_NAME} && HELM_RELEASE_NAME="pr-$PULL_NUMBER"
test -z ${HELM_COMMON_NAME} && HELM_COMMON_NAME="deps"
test -z ${DATASET_NAME} && DATASET_NAME=feast_e2e_$(date +%s)
test -z ${SPECS_TOPIC} && SPECS_TOPIC=feast-specs-$(date +%s)


feast_kafka_1_ip_name="feast-kafka-1"
feast_kafka_2_ip_name="feast-kafka-2"
feast_kafka_3_ip_name="feast-kafka-3"
feast_redis_ip_name="feast-redis"
feast_statsd_ip_name="feast-statsd"

echo "
This script will run end-to-end tests for Feast Core and Batch Serving using Dataflow Runner.

1. Setup K8s cluster (optional, if it was not created before)
2. Reuse existing IP addresses or generate new ones for stateful services
3. Install stateful services (kafka, redis, postgres, etc) (optional)
4. Build core & serving docker images (optional)
5. Create temporary BQ table for Feast Serving.
6. Rollout target images to cluster via helm in dedicated namespace (pr-{number})
7. Install Python 3.7.4, Feast Python SDK and run end-to-end tests from
   tests/e2e via pytest.
8. Tear down feast services, keep stateful services.
"

ORIGINAL_DIR=$(pwd)
echo $ORIGINAL_DIR

echo "Environment:"
printenv

export GOOGLE_APPLICATION_CREDENTIALS
gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}
gcloud -q auth configure-docker

gcloud config set project ${GCLOUD_PROJECT}
gcloud config set compute/region ${GCLOUD_REGION}
gcloud config list

apt-get -qq update
apt-get -y install wget build-essential gettext-base curl

curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
chmod 700 $ORIGINAL_DIR/get_helm.sh
$ORIGINAL_DIR/get_helm.sh


function getPublicAddresses() {
  existing_addresses=$(gcloud compute addresses list --filter="region:($GCLOUD_REGION) name:kafka" --format "list(name)")
  if [[ -z "$existing_addresses" ]]; then
    echo "
============================================================
Reserving IP addresses for Feast dependencies
============================================================
"

    gcloud compute addresses create \
      $feast_kafka_1_ip_name $feast_kafka_2_ip_name $feast_kafka_3_ip_name $feast_redis_ip_name $feast_statsd_ip_name \
      --region ${GCLOUD_REGION} --subnet ${GCLOUD_SUBNET}
  fi


  export feast_kafka_1_ip=$(gcloud compute addresses describe $feast_kafka_1_ip_name --region=${GCLOUD_REGION} --format "value(address)")
  export feast_kafka_2_ip=$(gcloud compute addresses describe $feast_kafka_2_ip_name --region=${GCLOUD_REGION} --format "value(address)")
  export feast_kafka_3_ip=$(gcloud compute addresses describe $feast_kafka_3_ip_name --region=${GCLOUD_REGION} --format "value(address)")
  export feast_redis_ip=$(gcloud compute addresses describe $feast_redis_ip_name --region=${GCLOUD_REGION} --format "value(address)")
  export feast_statsd_ip=$(gcloud compute addresses describe $feast_statsd_ip_name --region=${GCLOUD_REGION} --format "value(address)")
}

function createKubeCluster() {
  echo "
============================================================
Creating GKE nodepool for Feast e2e test with DataflowRunner
============================================================
"
  gcloud container clusters create ${K8_CLUSTER_NAME} --region ${GCLOUD_REGION} \
      --enable-cloud-logging \
      --enable-cloud-monitoring \
      --network ${GCLOUD_NETWORK} \
      --subnetwork ${GCLOUD_SUBNET} \
      --scopes https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,\
https://www.googleapis.com/auth/monitoring,https://www.googleapis.com/auth/service.management.readonly,\
https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/trace.append,\
https://www.googleapis.com/auth/bigquery \
      --machine-type n1-standard-2

  echo "
============================================================
Create feast-postgres-database Secret in GKE nodepool
============================================================
"
  kubectl create secret generic feast-postgresql --from-literal=postgresql-password=password

  echo "
============================================================
Create feast-gcp-service-account Secret in GKE nodepool
============================================================
"
  cd $ORIGINAL_DIR/infra/scripts
  kubectl create secret generic feast-gcp-service-account --from-file=credentials.json=${GOOGLE_APPLICATION_CREDENTIALS}
}

function installDependencies() {
  echo "
============================================================
Helm install common parts (kafka, redis, etc)
============================================================
"
  cd $ORIGINAL_DIR/infra/charts/feast

  helm install --wait --debug --values="values-end-to-end-batch-dataflow-updated.yaml" \
   --set "feast-core.enabled=false" \
   --set "feast-online-serving.enabled=false" \
   --set "feast-batch-serving.enabled=false" \
   --set "postgresql.enabled=false"
   "$HELM_COMMON_NAME" .

}

function buildAndPushImage()
{
  echo docker build -t $1:$2 --build-arg REVISION=$2 -f $3 $ORIGINAL_DIR
  docker build -t $1:$2 --build-arg REVISION=$2 -f $3 $ORIGINAL_DIR
  docker push $1:$2
}

function buildTarget() {
  buildAndPushImage "gcr.io/kf-feast/feast-core" "$PULL_NUMBER" "$ORIGINAL_DIR/infra/docker/core/Dockerfile"
  buildAndPushImage "gcr.io/kf-feast/feast-serving" "$PULL_NUMBER" "$ORIGINAL_DIR/infra/docker/serving/Dockerfile"
}

function installTarget() {
  echo "
============================================================
Helm install feast
============================================================
"
  cd $ORIGINAL_DIR/infra/charts/feast

  helm install --wait --timeout 300s --debug --values="values-end-to-end-batch-dataflow-updated.yaml" \
   --set "kafka.enabled=false" \
   --set "redis.enabled=false" \
   --set "prometheus-statsd-exporter.enabled=false" \
   --set "prometheus.enabled=false" \
    "$HELM_RELEASE_NAME" .

}

function clean() {
  echo "
  ============================================================
  Cleaning up
  ============================================================
  "
  cd $ORIGINAL_DIR/tests/e2e

  # Remove BQ Dataset
  bq rm -r -f ${GCLOUD_PROJECT}:${DATASET_NAME}

  # Uninstall helm release before clearing PVCs
  helm uninstall ${HELM_RELEASE_NAME}

  kubectl delete pvc data-${HELM_RELEASE_NAME}-postgresql-0

  # Stop Dataflow jobs from retrieved Dataflow job ids in ingesting_jobs.txt
  if [ -f ingesting_jobs.txt ]; then
    while read line
    do
        echo $line
        gcloud dataflow jobs cancel $line --region=${GCLOUD_REGION}
    done < ingesting_jobs.txt
  fi
}

# 1.
existing_cluster=$(gcloud container clusters list --format "list(name)" --filter "name:$K8_CLUSTER_NAME")
if [[ -z $existing_cluster ]]; then
  createKubeCluster "$@"
else
  gcloud container clusters get-credentials $K8_CLUSTER_NAME --region $GCLOUD_REGION --project $GCLOUD_PROJECT
fi

# 2.
getPublicAddresses "$@"

echo "
============================================================
Export required environment variables
============================================================
"

export TEMP_BUCKET=$TEMP_BUCKET/$HELM_RELEASE_NAME/$(date +%s)
export DATASET_NAME=$DATASET_NAME
export GCLOUD_PROJECT=$GCLOUD_PROJECT
export GCLOUD_NETWORK=$GCLOUD_NETWORK
export GCLOUD_SUBNET=$GCLOUD_SUBNET
export GCLOUD_REGION=$GCLOUD_REGION
export HELM_COMMON_NAME=$HELM_COMMON_NAME
export IMAGE_TAG=$PULL_PULL_SHA
export SPECS_TOPIC=$SPECS_TOPIC

export PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
export SCRIPTS_DIR=${PROJECT_ROOT_DIR}/infra/scripts
source ${SCRIPTS_DIR}/setup-common-functions.sh

wait_for_docker_image gcr.io/kf-feast/feast-core:"${IMAGE_TAG}"
wait_for_docker_image gcr.io/kf-feast/feast-serving:"${IMAGE_TAG}"

envsubst $'$TEMP_BUCKET $DATASET_NAME $GCLOUD_PROJECT $GCLOUD_NETWORK $SPECS_TOPIC \
  $GCLOUD_SUBNET $GCLOUD_REGION $IMAGE_TAG $HELM_COMMON_NAME $feast_kafka_1_ip
  $feast_kafka_2_ip $feast_kafka_3_ip $feast_redis_ip $feast_statsd_ip' < $ORIGINAL_DIR/infra/scripts/test-templates/values-end-to-end-batch-dataflow.yaml > $ORIGINAL_DIR/infra/charts/feast/values-end-to-end-batch-dataflow-updated.yaml


# 3.
existing_deps=$(helm list --filter deps -q)
if [[ -z $existing_deps ]]; then
  installDependencies "$@"
fi

# 4.
# buildTarget "$@"

# 5.
echo "
============================================================
Creating temp BQ table for Feast Serving
============================================================
"

bq --location=US --project_id=${GCLOUD_PROJECT} mk \
  --dataset \
  --default_table_expiration 86400 \
  ${GCLOUD_PROJECT}:${DATASET_NAME}


# 6.

set +e
installTarget "$@"

# 7.
echo "
============================================================
Installing Python 3.7 with Miniconda and Feast SDK
============================================================
"
cd $ORIGINAL_DIR
# Install Python 3.7 with Miniconda
wget -q https://repo.continuum.io/miniconda/Miniconda3-4.7.12-Linux-x86_64.sh \
   -O /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -p /root/miniconda -f
/root/miniconda/bin/conda init
source ~/.bashrc

# Install Feast Python SDK and test requirements
cd $ORIGINAL_DIR
make compile-protos-python
pip install -qe sdk/python
pip install -qr tests/e2e/requirements.txt

echo "
============================================================
Running end-to-end tests with pytest at 'tests/e2e'
============================================================
"
# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts

cd $ORIGINAL_DIR/tests/e2e

core_ip=$(kubectl get -o jsonpath="{.status.loadBalancer.ingress[0].ip}" service ${HELM_RELEASE_NAME}-feast-core)
serving_ip=$(kubectl get -o jsonpath="{.status.loadBalancer.ingress[0].ip}" service ${HELM_RELEASE_NAME}-feast-batch-serving)
jc_ip=$(kubectl get -o jsonpath="{.status.loadBalancer.ingress[0].ip}" service ${HELM_RELEASE_NAME}-feast-jc)

set +e
pytest -s -v bq/bq-batch-retrieval.py -m dataflow_runner --core_url "$core_ip:6565" --serving_url "$serving_ip:6566" \
 --jc_url "$jc_ip:6570" --gcs_path "gs://${TEMP_BUCKET}/" --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
TEST_EXIT_CODE=$?

if [[ ${TEST_EXIT_CODE} != 0 ]]; then
  echo "[DEBUG] Printing logs"
  ls -ltrh /var/log/feast*
  cat /var/log/feast-serving-warehouse.log /var/log/feast-core.log

  echo "[DEBUG] Printing Python packages list"
  pip list
fi

clean "$@"
exit ${TEST_EXIT_CODE}
