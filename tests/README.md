# Running full e2e test suite in Minikube

This doc describes how to to run the entire suite of e2e tests locally, with no external (cloud) dependencies. It will use minikube, spark k8s operator and minio for storage.

The tests will be run against your local copy of the Python SDK and ingestion jar. For other components like core and serving this setup will use docker images from the public GCR repo, built from the latest master.

## Prerequisites:
* Docker (highly recommend increasing default disk image size on OSX in docker settings)
* awscli
* kubectl
* minikube (tested using docker driver on OSX)
* helm 3
* bash 5.0+ (you'll need to brew install it on OSX)
* Java 11 toolchain with maven
* make

## Steps

1. Start minikube. We'll need more memory and disk than default.
```bash
minikube start --disk-size='40000mb' --memory 4096
```

2. Install minio. 
```bash
helm repo add minio https://helm.min.io/
helm install --namespace minio --create-namespace minio minio/minio --set resources.requests.memory=2Gi
```

3. Create k8s namespace to run tests in
```bash
kubectl create namespace sparkop
```

4. Install spark operator.
```bash
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install spark-op spark-operator/spark-operator \
	--namespace spark-operator \
	--create-namespace \
	--set "image.tag=v1beta2-1.1.2-2.4.5" \
	--set "sparkJobNamespace=sparkop" \
	--set "serviceAccounts.spark.name=spark"
```

5. Copy secret from minio into new sparkop namespace.
```bash
kubectl get secret minio --namespace=minio -oyaml | grep -v '^\s*namespace:\s' | kubectl apply --namespace=sparkop -f -
```

6. Install Feast. That may fail due to timeout, rerun again in that case
```bash
#
# NB!! make sure to use bash 5.0+ for the next step. You'd need to brew install it on OSX
DOCKER_REPOSITORY=gcr.io/kf-feast GIT_TAG=develop bash ./infra/scripts/setup-e2e-local.sh
```

7. Build the ingestion jar locally
```bash
make build-java-no-tests REVISION=develop
```

8. Create staging bucket using awscli. First, create a port-forward for minio:
```bash
# port forward minio in a separate terminal
export POD_NAME=$(kubectl get pods --namespace minio -l "release=minio" -o jsonpath="{.items[0].metadata.name}")

kubectl port-forward $POD_NAME 9000 --namespace minio
```
Then create the bucket
```bash

export AWS_ACCESS_KEY_ID=$(kubectl get secret minio -o jsonpath="{.data.accesskey}" -n minio | base64 --decode)
export AWS_SECRET_ACCESS_KEY=$(kubectl get secret minio -o jsonpath="{.data.secretkey}" -n minio | base64 --decode)
export AWS_DEFAULT_REGION=us-east-1

# Finally, create staging bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://feast-staging
```

9. Use minikube docker to build a docker image with tests from your working copy.
```bash
eval $(minikube docker-env)
make build-local-test-docker
```

10. Finally, run tests:
```bash
./infra/scripts/run-minikube-test.sh
```
