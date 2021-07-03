# IBM Cloud Kubernetes Service \(IKS\) and Red Hat OpenShift \(with Kustomize\)

## Overview

This guide installs Feast on an existing IBM Cloud Kubernetes cluster or Red Hat OpenShift on IBM Cloud , and ensures the following services are running:

* Feast Core
* Feast Online Serving
* Postgres
* Redis
* Kafka \(Optional\)
* Feast Jupyter \(Optional\)
* Prometheus \(Optional\)

## 1. Prerequisites

1. [IBM Cloud Kubernetes Service](https://www.ibm.com/cloud/kubernetes-service) or [Red Hat OpenShift on IBM Cloud](https://www.ibm.com/cloud/openshift)
2. Install [Kubectl](https://cloud.ibm.com/docs/containers?topic=containers-cs_cli_install#kubectl) that matches the major.minor versions of your IKS or Install the [OpenShift CLI](https://cloud.ibm.com/docs/openshift?topic=openshift-openshift-cli#cli_oc) that matches your local operating system and OpenShift cluster version.
3. Install [Helm 3](https://helm.sh/)
4. Install [Kustomize](https://kubectl.docs.kubernetes.io/installation/kustomize/)

## 2. Preparation

### IBM Cloud Block Storage Setup \(IKS only\)

:warning: If you have Red Hat OpenShift Cluster on IBM Cloud skip to this [section](ibm-cloud-iks-with-kustomize.md#Security-Context-Constraint-Setup).

By default, IBM Cloud Kubernetes cluster uses [IBM Cloud File Storage](https://www.ibm.com/cloud/file-storage) based on NFS as the default storage class, and non-root users do not have write permission on the volume mount path for NFS-backed storage. Some common container images in Feast, such as Redis, Postgres, and Kafka specify a non-root user to access the mount path in the images. When containers are deployed using these images, the containers fail to start due to insufficient permissions of the non-root user creating folders on the mount path.

[IBM Cloud Block Storage](https://www.ibm.com/cloud/block-storage) allows for the creation of raw storage volumes and provides faster performance without the permission restriction of NFS-backed storage

Therefore, to deploy Feast we need to set up [IBM Cloud Block Storage](https://cloud.ibm.com/docs/containers?topic=containers-block_storage#install_block) as the default storage class so that you can have all the functionalities working and get the best experience from Feast.

1. [Follow the instructions](https://helm.sh/docs/intro/install/) to install the Helm version 3 client on your local machine.
2. Add the IBM Cloud Helm chart repository to the cluster where you want to use the IBM Cloud Block Storage plug-in.

   ```text
    helm repo add iks-charts https://icr.io/helm/iks-charts
    helm repo update
   ```

3. Install the IBM Cloud Block Storage plug-in. When you install the plug-in, pre-defined block storage classes are added to your cluster.

   ```text
    helm install v2.0.2 iks-charts/ibmcloud-block-storage-plugin -n kube-system
   ```

   Example output:

   ```text
   NAME: v2.0.2
   LAST DEPLOYED: Fri Feb  5 12:29:50 2021
   NAMESPACE: kube-system
   STATUS: deployed
   REVISION: 1
   NOTES:
   Thank you for installing: ibmcloud-block-storage-plugin.   Your release is named: v2.0.2
    ...
   ```

4. Verify that all block storage plugin pods are in a "Running" state.

   ```text
    kubectl get pods -n kube-system | grep ibmcloud-block-storage
   ```

5. Verify that the storage classes for Block Storage were added to your cluster.

   ```text
    kubectl get storageclasses | grep ibmc-block
   ```

6. Set the Block Storage as the default storageclass.

   ```text
    kubectl patch storageclass ibmc-block-gold -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
    kubectl patch storageclass ibmc-file-gold -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'

    # Check the default storageclass is block storage
    kubectl get storageclass | grep \(default\)
   ```

   Example output:

   ```text
    ibmc-block-gold (default)   ibm.io/ibmc-block   65s
   ```

   **Security Context Constraint Setup \(OpenShift only\)**

By default, in OpenShift, all pods or containers will use the [Restricted SCC](https://docs.openshift.com/container-platform/4.6/authentication/managing-security-context-constraints.html) which limits the UIDs pods can run with, causing the Feast installation to fail. To overcome this, you can allow Feast pods to run with any UID by executing the following:

```text
oc adm policy add-scc-to-user anyuid -z default,kf-feast-kafka -n feast
```

## 3. Installation

Install Feast using kustomize. The pods may take a few minutes to initialize.

```bash
git clone https://github.com/kubeflow/manifests
cd manifests/contrib/feast/
kustomize build feast/base | kubectl apply -n feast -f -
```

### Optional: Enable Feast Jupyter and Kafka

You may optionally enable the Feast Jupyter component which contains code examples to demonstrate Feast. Some examples require Kafka to stream real time features to the Feast online serving. To enable, edit the following properties in the `values.yaml` under the `manifests/contrib/feast` folder:

```text
kafka.enabled: true
feast-jupyter.enabled: true
```

Then regenerate the resource manifests and deploy:

```text
make feast/base
kustomize build feast/base | kubectl apply -n feast -f -
```

## 4. Use Feast Jupyter Notebook Server to connect to Feast

After all the pods are in a `RUNNING` state, port-forward to the Jupyter Notebook Server in the cluster:

```bash
kubectl port-forward \
$(kubectl get pod -l app=feast-jupyter -o custom-columns=:metadata.name) 8888:8888 -n feast
```

```text
Forwarding from 127.0.0.1:8888 -> 8888
Forwarding from [::1]:8888 -> 8888
```

You can now connect to the bundled Jupyter Notebook Server at `localhost:8888` and follow the example Jupyter notebook.

{% embed url="http://localhost:8888/tree?" caption="" %}

## 5. Uninstall Feast

```text
kustomize build feast/base | kubectl delete -n feast -f -
```

## 6. Troubleshooting

When running the minimal\_ride\_hailing\_example Jupyter Notebook example the following errors may occur:

1. When running `job = client.get_historical_features(...)`:

   ```text
    KeyError: 'historical_feature_output_location'
   ```

   or

   ```text
    KeyError: 'spark_staging_location'
   ```

   Add the following environment variable:

   ```text
    os.environ["FEAST_HISTORICAL_FEATURE_OUTPUT_LOCATION"] = "file:///home/jovyan/historical_feature_output"
    os.environ["FEAST_SPARK_STAGING_LOCATION"] = "file:///home/jovyan/test_data"
   ```

2. When running `job.get_status()`

   ```text
    <SparkJobStatus.FAILED: 2>
   ```

   Add the following environment variable:

   ```text
    os.environ["FEAST_REDIS_HOST"] = "feast-release-redis-master"
   ```

3. When running `job = client.start_stream_to_online_ingestion(...)`

   ```text
    org.apache.kafka.vendor.common.KafkaException: Failed to construct kafka consumer
   ```

   Add the following environment variable:

   ```text
    os.environ["DEMO_KAFKA_BROKERS"] = "feast-release-kafka:9092"
   ```

