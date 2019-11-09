# Install Feast

## Overview

This guide will demonstrate installing Feast using [**Minikube**](install-feast.md#minikube)**.** This installation has no external dependencies. One downside is that this installation does not include a historical feature store. It is meant to get users up and running quickly.

## Minikube

### Overview

This guide will install Feast into [Minikube](https://github.com/kubernetes/minikube). Once Feast is installed you will be able to:

* Define and register features.
* Load feature data from both batch and streaming sources.
* Retrieve features for online serving.

{% hint style="warning" %}
This Minikube installation guide is for demonstration purposes only. It is not meant for production use, and does not install a historical feature store.
{% endhint %}

### 0. Requirements

The following software should be installed prior to starting:

1. [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/) should be installed.
2. [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) installed and configured to work with Minikube.
3. [Helm](https://helm.sh/3) \(2.16.0 or greater\).

### 1. Set up Minikube

a. Start Minikube. Note the minimum cpu and memory below:

```text
minikube start --cpus=3 --memory=4096 --kubernetes-version='v1.15.5'
```

b. Set up your Feast environmental variables

```text
export FEAST_IP=$(minikube ip)
export FEAST_CORE_URL=${FEAST_IP}:32090
export FEAST_SERVING_URL=${FEAST_IP}:32091
```

### 2. Install Feast with Helm

a. Clone the [Feast repository](https://github.com/gojek/feast/) and navigate to the `charts` sub-directory:

```text
git clone https://github.com/gojek/feast.git && \
cd feast && export FEAST_HOME_DIR=$(pwd) && \
cd infra/charts/feast
```

b. Copy the `values-demo.yaml` file for your installation:

```text
cp values-demo.yaml my-feast-values.yaml
```

c. Update all occurrences of the domain `feast.example.com` inside of  `my-feast-values.yaml` with your Minikube IP. This is to allow external access to the services in the cluster. You can find your Minikube IP by running the following command `minikube ip`, or simply replace the text from the command line:

```text
sed -i "s/feast.example.com/${FEAST_IP}/g" my-feast-values.yaml
```

d. Install the Feast Helm chart:

```text
helm install --name feast -f my-feast-values.yaml .
```

e. Ensure that the system comes online. This will take a few minutes

```text
watch kubectl get pods
```

```text
NAME                                           READY   STATUS      RESTARTS   AGE
pod/feast-feast-core-666fd46db4-l58l6          1/1     Running     0          5m
pod/feast-feast-serving-online-84d99ddcbd      1/1     Running     0          6m
pod/feast-kafka-0                              1/1     Running     0          3m
pod/feast-kafka-1                              1/1     Running     0          4m
pod/feast-kafka-2                              1/1     Running     0          4m
pod/feast-postgresql-0                         1/1     Running     0          5m
pod/feast-redis-master-0                       1/1     Running     0          5m
pod/feast-zookeeper-0                          1/1     Running     0          5m
pod/feast-zookeeper-1                          1/1     Running     0          5m
pod/feast-zookeeper-2                          1/1     Running     0          5m
```

### 3. Connect to Feast with the Python SDK

a. Install the Python SDK using pip:

```text
pip install -e ${FEAST_HOME_DIR}/sdk/python
```

b. Configure the Feast Python SDK:

```text
feast config set core_url ${FEAST_CORE_URL}
feast config set serving_url ${FEAST_SERVING_URL}
```

c. Make sure that both Feast Core and Feast Serving are connected:

```text
feast version
```

```text
{
  "sdk": {
    "version": "feast 0.3.0"
  },
  "core": {
    "url": "192.168.99.100:32090",
    "version": "0.3",
    "status": "connected"
  },
  "serving": {
    "url": "192.168.99.100:32091",
    "version": "0.3",
    "status": "connected"
  }
}
```

That's it! You can now start to use Feast!

