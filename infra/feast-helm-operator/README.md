# Feast Feature Server Helm-based Operator (Deprecated replaced by [feast-operator](../feast-operator/README.md))

This Operator was built with the [operator-sdk](https://github.com/operator-framework/operator-sdk) and leverages the [feast-feature-server helm chart](/infra/charts/feast-feature-server).

## Installation

1. __Install [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)__
2. __Install the Operator on a Kubernetes cluster__

```bash
make deploy
```

3. __Install a Feast Feature Server on Kubernetes__

A base64 encoded version of the `feature_store.yaml` file is required. FeastFeatureServer CR install example:
```bash
cat <<EOF | kubectl create -f -
apiVersion: charts.feast.dev/v1alpha1
kind: FeastFeatureServer
metadata:
  name: example
spec:
  feature_store_yaml_base64: $(cat feature_store.yaml | base64 | tr -d '\n\r')
EOF
```
Ensure it was successfully created on the cluster and that the `feature_store_yaml_base64` field was properly set. The following command should return output which is identical to your `feature_store.yaml`:
```bash
kubectl get feastfeatureserver example -o jsonpath={.spec.feature_store_yaml_base64} | base64 -d
```
Watch as the operator creates a running feast feature server:
```bash
kubectl get all
kubectl logs deploy/example-feast-feature-server
```

The `FeastFeatureServer.spec` allows one to configure the [same parameters](https://github.com/feast-dev/feast/tree/master/infra/charts/feast-feature-server#values) as the feast-feature-server helm chart. An example of this can be seen with the included sample [FeastFeatureServer](config/samples/charts_v1alpha1_feastfeatureserver.yaml).

> To install the aforementioned sample FeastFeatureServer, run this command - `kubectl create -f config/samples/charts_v1alpha1_feastfeatureserver.yaml`
