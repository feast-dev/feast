#!/usr/bin/env bash
echo "Preparing environment variables..."

set -e
set -o pipefail

test -z ${GOOGLE_APPLICATION_CREDENTIALS} && GOOGLE_APPLICATION_CREDENTIALS="/etc/service-account/service-account-df.json"
test -z ${GCLOUD_PROJECT} && GCLOUD_PROJECT="kf-feast"
test -z ${GCLOUD_REGION} && GCLOUD_REGION="us-central1"
test -z ${GCLOUD_NETWORK} && GCLOUD_NETWORK="default"
test -z ${GCLOUD_SUBNET} && GCLOUD_SUBNET="default"
test -z ${TEMP_BUCKET} && TEMP_BUCKET="feast-templocation-kf-feast"
test -z ${K8_CLUSTER_NAME} && K8_CLUSTER_NAME="feast-e2e-dataflow"
test -z ${HELM_RELEASE_NAME} && HELM_RELEASE_NAME="feast-e2e-release"

echo "
This script will run end-to-end tests for Feast Core and Batch Serving using Dataflow Runner.

1. Install gcloud SDK and required packages.
2. Create temporary BQ table for Feast Serving.
3. Generate valid names for IP addresses and k8s cluster, then store as environment variables.
4. Create GKE nodepool for Feast e2e test with DataflowRunner.
5. Setup Feast Core, Feast Serving and dependencies using Helm.
  - Redis as the job store for Feast Batch Serving.
  - Postgres for persisting Feast metadata.
  - Kafka and Zookeeper as the Source in Feast.
6. Install Python 3.7.4, Feast Python SDK and run end-to-end tests from
   tests/e2e via pytest.
7. Tear down infrastructure, including Dataflow jobs.
"

ORIGINAL_DIR=$(pwd)
echo $ORIGINAL_DIR

echo "
============================================================
Installing gcloud SDK and required packages
============================================================
"
apt-get -qq update
apt-get -y install wget netcat kafkacat build-essential gettext-base curl kubectl
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
chmod 700 $ORIGINAL_DIR/get_helm.sh
$ORIGINAL_DIR/get_helm.sh

if [[ ! $(command -v gsutil) ]]; then
  CURRENT_DIR=$(dirname "$BASH_SOURCE")
  . "${CURRENT_DIR}"/install-google-cloud-sdk.sh
fi

export GOOGLE_APPLICATION_CREDENTIALS
gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}

gcloud config set project ${GCLOUD_PROJECT}
gcloud config set compute/region ${GCLOUD_REGION}
gcloud config list

echo "
============================================================
Creating temp BQ table for Feast Serving
============================================================
"
DATASET_NAME=feast_e2e_$(date +%s)

bq --location=US --project_id=${GCLOUD_PROJECT} mk \
  --dataset \
  --default_table_expiration 86400 \
  ${GCLOUD_PROJECT}:${DATASET_NAME}

echo "
============================================================
Check and generate valid k8s cluster name
============================================================
"
count=0
for cluster_name in $(gcloud container clusters list --format "list(name)")
do
  if [[ $cluster_name == "feast-e2e-dataflow"* ]]; then
    count=$((count + 1))
  fi
done
temp="$K8_CLUSTER_NAME-$count"
export K8_CLUSTER_NAME=$temp
echo "Cluster name is $K8_CLUSTER_NAME"

echo "
============================================================
Reserving IP addresses for Feast dependencies
============================================================
"
feast_kafka_1_ip_name="feast-kafka-$((count*3 + 1))"
feast_kafka_2_ip_name="feast-kafka-$((count*3 + 2))"
feast_kafka_3_ip_name="feast-kafka-$((count*3 + 3))"
feast_redis_ip_name="feast-redis-$((count + 1))"
feast_statsd_ip_name="feast-statsd-$((count + 1))"
gcloud compute addresses create \
  $feast_kafka_1_ip_name $feast_kafka_2_ip_name $feast_kafka_3_ip_name $feast_redis_ip_name $feast_statsd_ip_name \
  --region ${GCLOUD_REGION} --subnet ${GCLOUD_SUBNET}

ip_count=0
for ip_addr_name in $feast_kafka_1_ip_name $feast_kafka_2_ip_name $feast_kafka_3_ip_name $feast_redis_ip_name $feast_statsd_ip_name
do
  if [[ "$ip_count" == 0 ]]; then
    export feast_kafka_1_ip=$(gcloud compute addresses describe ${ip_addr_name} --region=asia-east1 --format "value(address)")
  elif [[ "$ip_count" == 1 ]]; then
    export feast_kafka_2_ip=$(gcloud compute addresses describe ${ip_addr_name} --region=asia-east1 --format "value(address)")
  elif [[ "$ip_count" == 2 ]]; then
    export feast_kafka_3_ip=$(gcloud compute addresses describe ${ip_addr_name} --region=asia-east1 --format "value(address)")
  elif [[ "$ip_count" == 3 ]]; then
    export feast_redis_ip=$(gcloud compute addresses describe ${ip_addr_name} --region=asia-east1 --format "value(address)")
  elif [[ "$ip_count" == 4 ]]; then
    export feast_statsd_ip=$(gcloud compute addresses describe ${ip_addr_name} --region=asia-east1 --format "value(address)")
  fi
  ip_count=$((ip_count + 1))
  export "$(echo $ip_addr_name | tr '-' '_')=$(gcloud compute addresses describe ${ip_addr_name} --region=asia-east1 --format "value(address)")"
done

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
    --machine-type n1-standard-2
sleep 120

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
kubectl create secret generic feast-gcp-service-account --from-file=${GOOGLE_APPLICATION_CREDENTIALS}

echo "
============================================================
Export required environment variables
============================================================
"
export TEMP_BUCKET=$TEMP_BUCKET
export DATASET_NAME=$DATASET_NAME
export GCLOUD_PROJECT=$GCLOUD_PROJECT
export GCLOUD_NETWORK=$GCLOUD_NETWORK
export GCLOUD_SUBNET=$GCLOUD_SUBNET
export GCLOUD_REGION=$GCLOUD_REGION

echo "
============================================================
Helm install Feast and its dependencies
============================================================
"
cd $ORIGINAL_DIR/infra/scripts/test-templates
envsubst $'$TEMP_BUCKET $DATASET_NAME $GCLOUD_PROJECT $GCLOUD_NETWORK \
  $GCLOUD_SUBNET $GCLOUD_REGION $feast_kafka_1_ip
  $feast_kafka_2_ip $feast_kafka_3_ip $feast_redis_ip $feast_statsd_ip' < values-end-to-end-batch-dataflow.yaml > $ORIGINAL_DIR/infra/charts/feast/values-end-to-end-batch-dataflow-updated.yaml

cd $ORIGINAL_DIR/infra/charts
helm template feast

cd $ORIGINAL_DIR/infra/charts/feast
helm install --wait --timeout 600s --values="values-end-to-end-batch-dataflow-updated.yaml" ${HELM_RELEASE_NAME} .

echo "
============================================================
Installing Python 3.7 with Miniconda and Feast SDK
============================================================
"
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

core_ip=$(kubectl get -o jsonpath="{.spec.clusterIP}" service ${HELM_RELEASE_NAME}-feast-core)
serving_ip=$(kubectl get -o jsonpath="{.spec.clusterIP}" service ${HELM_RELEASE_NAME}-feast-batch-serving)

set +e
pytest bq-batch-retrieval.py -m dataflow_runner --core_url "$core_ip:6565" --serving_url "$serving_ip:6566" --gcs_path "gs://${TEMP_BUCKET}/" --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
TEST_EXIT_CODE=$?

if [[ ${TEST_EXIT_CODE} != 0 ]]; then
  echo "[DEBUG] Printing logs"
  ls -ltrh /var/log/feast*
  cat /var/log/feast-serving-warehouse.log /var/log/feast-core.log

  echo "[DEBUG] Printing Python packages list"
  pip list
fi

cd ${ORIGINAL_DIR}
exit ${TEST_EXIT_CODE}

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
kubectl delete pvc --all

# Release IP addresses
yes | gcloud compute addresses delete $feast_kafka_1_ip_name $feast_kafka_2_ip_name $feast_kafka_3_ip_name $feast_redis_ip_name $feast_statsd_ip_name --region=${GCLOUD_REGION}

# Tear down GKE infrastructure
gcloud container clusters delete --region=${GCLOUD_REGION} ${K8_CLUSTER_NAME}

# Stop Dataflow jobs from retrieved Dataflow job ids in ingesting_jobs.txt
while read line
do 
    echo $line
    gcloud dataflow jobs cancel $line --region=asia-east1
done < ingesting_jobs.txt