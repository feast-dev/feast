## Feast Installation

This is a step-by-step quickstart guide to install Feast for Feast administrators.

Feast is designed to run on Kubernetes and currently requires managed
services from Google Cloud Platform:
- **Dataflow** for running import jobs to load feature values into Feast
- **BigQuery** for storing feature values for model training. BigQuery is the default Feast Warehouse store
- **Pub/Sub** for providing streaming feature values. This is used only if you are running streaming import job
- **Cloud Storage** for storing log and temporary files

This guide assumes you are using Google Container Engine (GKE) and have enabled the services mentioned above.

#### Feast Setup Configuration

Open your terminal and set these variables (by executing all the lines below in the terminal) according to your Google Cloud environment and Feast configuration.

```bash
GCP_PROJECT=project-id
# Please use one of these available Dataflow regional endpoints:
# https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
GCP_REGION=asia-east1
GCP_ZONE=asia-east1-a
GCP_NETWORK=default
GCP_SUBNETWORK=default
GCP_DATAFLOW_MAX_NUM_WORKERS=4

FEAST_CLUSTER_NAME=feast
FEAST_SERVICE_ACCOUNT_NAME=feast
FEAST_REDIS_GCE_INSTANCE_NAME=feast-redis
# Set this to your preferred Postgres password.
# Postgres is used to save Feast entity and feature specs and other configuration.
FEAST_POSTGRES_PASSWORD=iK4aehe7aiso
FEAST_HELM_RELEASE_NAME=feast
# Make sure you follow this convention when naming your dataset
# https://cloud.google.com/bigquery/docs/datasets#dataset-naming
FEAST_WAREHOUSE_BIGQUERY_DATASET=feast
# Make sure this bucket has been created
FEAST_STAGING_LOCATION_GCS_URI=gs://bucket
```

#### Prepare Feast Cluster and Redis Instance

- Make sure you have `gcloud` command installed. If not follow this link https://cloud.google.com/sdk/install  
- Ensure you have `kubectl` command installed. If not follow this [link](https://kubernetes.io/docs/tasks/tools/install-kubectl/) or install via gcloud  
`gcloud components install kubectl`
- Ensure you have **Owner** role or **Editor** role with `Project IAM Admin`, `Kubernetes Engine Admin` and `BigQuery Admin` roles. If not, please ask the owners in your Google Cloud project to give you permissions.

Run the following command to create a new cluster in GKE (Google Kubernetes Engine).
> You can skip this step if you want to install Feast in an existing Kubernetes cluster
```
gcloud container --project "${GCP_PROJECT}" clusters create "${FEAST_CLUSTER_NAME}" \
  --zone "${GCP_ZONE}" --no-enable-basic-auth --cluster-version "1.12.7-gke.10" \
  --machine-type "n1-standard-4" --image-type "COS" --disk-type "pd-standard" --disk-size "200" \
  --metadata disable-legacy-endpoints=true --scopes "https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" \
  --num-nodes "1" --enable-cloud-logging --enable-cloud-monitoring --no-enable-ip-alias \
  --network "projects/${GCP_PROJECT}/global/networks/${GCP_NETWORK}" \
  --subnetwork "projects/${GCP_PROJECT}/regions/${GCP_REGION}/subnetworks/${GCP_SUBNETWORK}" \
  --enable-autoscaling --min-nodes "1" --max-nodes "4" \
  --addons HorizontalPodAutoscaling,HttpLoadBalancing --enable-autoupgrade --enable-autorepair
```

Ensure that you are authenticated to manage the Kubernetes cluster
> You can skip this step if you are already authenticated to the cluster
```bash
gcloud container clusters get-credentials ${FEAST_CLUSTER_NAME} \
  --zone ${GCP_ZONE} --project ${GCP_PROJECT}
```

Run the following to create a virtual machine (VM) in Google Compute Engine (GCE) running redis server
> You can skip this step if you want to store Feast feature values for serving in an existing (non-clustered and non-authenticated) Redis instance.

```bash
# Comment out "--no-address" option if you do not have a NAT router set up in your project
# With no external ip, the VM instance requires a NAT router to access internet

gcloud --project=${GCP_PROJECT} compute instances create ${FEAST_REDIS_GCE_INSTANCE_NAME} \
  --zone=${GCP_ZONE} \
  --boot-disk-size=200GB \
  --description="Redis instance for Feast serving store" \
  --machine-type=n1-highmem-2 \
  --network=${GCP_NETWORK} \
  --subnet=${GCP_SUBNETWORK} \
  --no-address \
  --metadata startup-script="apt-get -y install redis-server; echo 'bind 0.0.0.0' >> /etc/redis/redis.conf; sudo systemctl enable redis; sudo systemctl restart redis"
```

When the command above completes, it will output details of the newly created VM. Set the variable `FEAST_SERVING_REDIS_HOST` to the `INTERNAL_IP` value from the `gcloud` output.
```bash
# Example output:
# NAME         ZONE          MACHINE_TYPE   INTERNAL_IP   STATUS
# feast-redis  asia-east1-a  n1-highmem-2   10.148.1.122  RUNNING

FEAST_SERVING_REDIS_HOST=10.148.1.122
```

#### Feast Service Account

By default, clusters created in GKE have only basic permissions to read from Google Cloud Storage and write system logs. We need to create a new service account for Feast, which has permission to run Dataflow jobs, manage BigQuery and write to Google Cloud Storage.

```bash
gcloud --project=${GCP_PROJECT} iam service-accounts create ${FEAST_SERVICE_ACCOUNT_NAME} \
  --display-name="Feast service account"

for role in bigquery.dataEditor bigquery.jobUser storage.admin dataflow.admin; do
  gcloud projects add-iam-policy-binding ${GCP_PROJECT} \
    --member serviceAccount:${FEAST_SERVICE_ACCOUNT_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com \
    --role=roles/${role} | tail -n2
done

# Create a JSON service account key
gcloud iam service-accounts keys create /tmp/service-account.json \
  --iam-account ${FEAST_SERVICE_ACCOUNT_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com

# Create secret resources in the Kube cluster for the service account and Postgres password
kubectl create secret generic ${FEAST_HELM_RELEASE_NAME}-service-account \
  --from-file=/tmp/service-account.json

kubectl create secret generic ${FEAST_HELM_RELEASE_NAME}-postgresql \
  --from-literal=postgresql-password=${FEAST_POSTGRES_PASSWORD}
```

#### Install Feast Helm Release

> Make sure you have `Helm` command installed, if not follow this link  
> https://helm.sh/docs/using_helm/#installing-helm

Ensure you have installed Tiller (the server component of Helm) in your Kubernetes cluster. If not run the following commands:
```bash
# Create service account for Tiller and grant cluster admin permission
kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: tiller
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: tiller
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
  - kind: ServiceAccount
    name: tiller
    namespace: kube-system
EOF

# Initialize Tiller 
helm init --service-account tiller --history-max 80
```

Run the following command to create a helm values configuration file for Feast in `/tmp/feast-helm-values.yaml`.

> This is the **recommended** configuration for installing Feast in **production** where Core and Serving component of Feast are exposed **internally** via **LoadBalancer** service type.  
> 
> If you have not setup Virtual Private Connection (VPN) or you just want to test Feast, you may need to comment out the following fields below:
> - `core.service.annotations`
> - `core.service.loadBalancerSourceRanges`
> - `serving.service.annotations`
> - `serving.service.loadBalancerSourceRanges`
> 
> **IMPORTANT NOTE**  
> Commenting out those fields is **not** recommended, however, because it will **expose your Feast service to the public internet**.
>
> A better approach if you don't have VPN setup to your Google Cloud network is to create a new VM that will act as a [bastion host](https://en.wikipedia.org/wiki/Bastion_host). You will then access Feast services after SSH-ing to this bastion host (because you can then access Feast services via the internal load balancer IP).

```bash
cat <<EOF > /tmp/feast-helm-values.yaml
global:
  postgresql:
    existingSecret: ${FEAST_HELM_RELEASE_NAME}-postgresql

core:
  projectId: ${GCP_PROJECT}
  jobs:
    runner: DataflowRunner
    options: '{"project": "${GCP_PROJECT}","region": "${GCP_REGION}","subnetwork": "regions/${GCP_REGION}/subnetworks/${GCP_SUBNETWORK}", "maxNumWorkers": "${GCP_DATAFLOW_MAX_NUM_WORKERS}", "autoscalingAlgorithm": "THROUGHPUT_BASED"}'
  service:
    type: LoadBalancer
    annotations:
      cloud.google.com/load-balancer-type: Internal
    loadBalancerSourceRanges:
    - 10.0.0.0/8
    - 172.16.0.0/12
    - 192.168.0.0/16

serving:
  service:
    type: LoadBalancer
    annotations:
      cloud.google.com/load-balancer-type: Internal
    loadBalancerSourceRanges:
    - 10.0.0.0/8
    - 172.16.0.0/12
    - 192.168.0.0/16

dataflow:
  projectID: ${GCP_PROJECT}
  location: ${GCP_REGION}

store:
  serving:
    options: '{"host": "${FEAST_SERVING_REDIS_HOST}", "port": 6379}'
  warehouse:
    options: '{"project": "${GCP_PROJECT}", "dataset": "${FEAST_WAREHOUSE_BIGQUERY_DATASET}"}'

serviceAccount:
  name: ${FEAST_HELM_RELEASE_NAME}-service-account
  key: service-account.json
EOF
```

Install Feast using the values in `/tmp/feast-helm-values.yaml`.

> Make sure you have cloned Feast repository and your **current directory is Feast repository root folder**.
> Otherwise, run the following:  
> `git clone https://github.com/gojek/feast.git`  
> `cd feast`  

Then run the following to install Feast Helm release:

```bash
helm install --name ${FEAST_HELM_RELEASE_NAME} ./charts/feast -f /tmp/feast-helm-values.yaml
```

Wait for about 2 minutes :alarm_clock: then make sure Feast deployment is successful.
```bash
# Make sure all the pods have statuses "READY: 1/1" and "STATUS: RUNNING"
kubectl get pod

# Retrieve the internal load balancer IP for feast-core and feast-serving services
kubectl get svc --selector release=${FEAST_HELM_RELEASE_NAME} 
# NAME                        TYPE           CLUSTER-IP      EXTERNAL-IP   PORT(S)                       AGE
# feast-core                  LoadBalancer   10.71.250.224   10.148.2.69   80:31652/TCP,6565:30148/TCP   2m30s
# feast-postgresql            ClusterIP      10.71.249.72    <none>        5432/TCP                      2m30s
# feast-postgresql-headless   ClusterIP      None            <none>        5432/TCP                      2m30s
# feast-serving               LoadBalancer   10.71.244.137   10.148.2.71   6565:30540/TCP,80:31324/TCP   2m30s

# Set the following variables based on the "EXTERNAL-IP" values above 
# These variables will be used by Feast CLI and Feast SDK later
FEAST_CORE_URI=10.148.2.69:6565
FEAST_SERVING_URI=10.148.2.71:6565
```

#### Test the Installation

To ensure that Feast has been succesfully installed, we'll run the following tests:
- Create entity
- Create features for the entity
- Ingest feature values via batch import job
- Retrieve the values from Feast serving

Make sure you have installed `feast` command and `feast` Python package. If not follow this [link](../cli/README.md#installation) to install Feast CLI and run `pip3 install -U feast` to install the Python package.

```bash
# Configure feast CLI so it knows the URI for Feast Core
feast config set coreURI ${FEAST_CORE_URI}

# Ensure your working directory is Feast repository root folder
feast apply entity integration-tests/testdata/entity_specs/entity_1.yaml
feast apply feature integration-tests/testdata/feature_specs/entity_1.feature_*.yaml

# The batch import job needs to know the file path of the feature value file
# We'll upload it to Google Cloud Storage
gsutil cp integration-tests/testdata/feature_values/ingestion_1.csv \
  ${FEAST_STAGING_LOCATION_GCS_URI}/ingestion_1.csv

# Prepare the import job specification file
cat <<EOF > /tmp/feast-batch-import.yaml
type: file.csv

sourceOptions:
  path: ${FEAST_STAGING_LOCATION_GCS_URI}/ingestion_1.csv

entities:
- entity_1

schema:
  entityIdColumn: entity
  timestampColumn: ts
  fields:
  - name: entity
  - name: ts
  - name: feature_1
    featureId: entity_1.feature_1
  - name: feature_2
    featureId: entity_1.feature_2
  - name: feature_3
    featureId: entity_1.feature_3
  - name: feature_4
    featureId: entity_1.feature_4
EOF

# Start the import job with feast cli
# This will start a Dataflow job that ingests the csv file containing the feature values
# into Feast warehouse (in BigQuery) and serving store (in Redis)
feast jobs run /tmp/feast-batch-import.yaml --wait

# If the job fails, you can open Google Cloud Console and check the Dataflow dashboard
# for details

# After the import job completed successfully, we can try retrieving the feature values 
# from Feast serving using the Python SDK
python3 - <<EOF
from feast.sdk.client import Client
from feast.sdk.resources.feature_set import FeatureSet

feast_client = Client(core_url="${FEAST_CORE_URI}", serving_url="${FEAST_SERVING_URI}")
feature_set = FeatureSet(entity="entity_1", features=["entity_1.feature_1", "entity_1.feature_2"])

print(feast_client.get_serving_data(feature_set, entity_keys=["0","1"]))
EOF

# You should see the following results printed out
# entity_1  entity_1.feature_1  entity_1.feature_2
# 0        0            0.988739            0.981957
# 1        1            0.809156            0.517604
```

#### Clean Up Installation

If you want to clean up Feast installation, run the following commands:
```bash
bq --project_id ${GCP_PROJECT} rm -rf --dataset ${FEAST_WAREHOUSE_BIGQUERY_DATASET}
helm delete --purge ${FEAST_HELM_RELEASE_NAME}
kubectl delete pvc data-${FEAST_HELM_RELEASE_NAME}-postgresql-0
kubectl delete secret ${FEAST_HELM_RELEASE_NAME}-service-account
kubectl delete secret ${FEAST_HELM_RELEASE_NAME}-postgresql
gcloud --project=${GCP_PROJECT} --quiet iam service-accounts delete \
  ${FEAST_SERVICE_ACCOUNT_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com

for role in bigquery.dataEditor bigquery.jobUser storage.admin dataflow.admin; do
  gcloud projects remove-iam-policy-binding ${GCP_PROJECT} \
    --member serviceAccount:${FEAST_SERVICE_ACCOUNT_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com \
    --role=roles/${role} | tail -n2
done

gcloud --project=${GCP_PROJECT} --quiet compute instances \
  delete ${FEAST_REDIS_GCE_INSTANCE_NAME} --zone=${GCP_ZONE}
gcloud --project=${GCP_PROJECT} --quiet container --project "${GCP_PROJECT}" \
  clusters delete "${FEAST_CLUSTER_NAME}" --zone "${GCP_ZONE}"
```
