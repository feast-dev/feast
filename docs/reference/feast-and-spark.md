---
description: Configuring Feast to use Spark for ingestion.
---

# Feast and Spark

{% hint style="danger" %}
We strongly encourage all users to upgrade from Feast 0.9 to Feast 0.10+. Please see [this](https://docs.feast.dev/v/master/project/feast-0.9-vs-feast-0.10+) for an explanation of the differences between the two versions. A guide to upgrading can be found [here](https://docs.google.com/document/d/1AOsr_baczuARjCpmZgVd8mCqTF4AZ49OEyU4Cn-uTT0/edit#heading=h.9gb2523q4jlh). 
{% endhint %}

Feast relies on Spark to ingest data from the offline store to the online store, streaming ingestion, and running queries to retrieve historical data from the offline store. Feast supports several Spark deployment options.

## Option 1. Use Kubernetes Operator for Apache Spark

To install the Spark on K8s Operator

```bash
helm repo add spark-operator \
    https://googlecloudplatform.github.io/spark-on-k8s-operator

helm install my-release spark-operator/spark-operator \
    --namespace sparkop \
    --create-namespace \
    --set serviceAccounts.spark.name=spark
```

Currently Feast is tested using `v1beta2-1.1.2-2.4.5`version of the operator image. To configure Feast to use it, set the following options in Feast config:

| Feast Setting | Value |
| :--- | :--- |
| `SPARK_LAUNCHER` | `"k8s"` |
| `SPARK_K8S_NAMESPACE` | The name of the Kubernetes namespace to run Spark jobs in. This should match the value of `sparkJobNamespace` set on spark-on-k8s-operator Helm chart. Typically this is also the namespace Feast itself will run in. The example above uses `sparkop`. |
| `SPARK_STAGING_LOCATION` | S3 URL to use as a staging location, must be readable and writable by Feast. Use `s3a://` prefix here. Ex.: `s3a://some-bucket/some-prefix` |

Lastly, make sure that the service account used by Feast has permissions to manage Spark Application resources. This depends on your k8s setup, but typically you'd need to configure a Role and a RoleBinding like the one below:

```text
cat <<EOF | kubectl apply -f -
kind: Role
apiVersion: rbac.authorization.k8s.io/v1beta1
metadata:
  name: use-spark-operator
  namespace: <REPLACE ME>  # probably "sparkop"
rules:
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["create", "delete", "deletecollection", "get", "list", "update", "watch", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1beta1
kind: RoleBinding
metadata:
  name: use-spark-operator
  namespace: <REPLACE ME> # probably "sparkop"
roleRef:
  kind: Role
  name: use-spark-operator
  apiGroup: rbac.authorization.k8s.io
subjects:
  - kind: ServiceAccount
    name: default
EOF
```

## Option 2. Use GCP and Dataproc

If you're running Feast in Google Cloud, you can use Dataproc, a managed Spark platform. To configure Feast to use it, set the following options in Feast config:

| Feast Setting | Value |
| :--- | :--- |
| `SPARK_LAUNCHER` | `"dataproc"` |
| `DATAPROC_CLUSTER_NAME` | Dataproc cluster name |
| `DATAPROC_PROJECT` | Dataproc project name |
| `SPARK_STAGING_LOCATION` | GCS URL to use as a staging location, must be readable and writable by Feast. Ex.: `gs://some-bucket/some-prefix` |

See [Feast documentation](https://api.docs.feast.dev/python/#module-feast.constants) for more configuration options for Dataproc.

## Option 3. Use AWS and EMR

If you're running Feast in AWS, you can use EMR, a managed Spark platform. To configure Feast to use it, set at least the following options in Feast config:

| Feast Setting | Value |
| :--- | :--- |
| `SPARK_LAUNCHER` | `"emr"` |
| `SPARK_STAGING_LOCATION` | S3 URL to use as a staging location, must be readable and writable by Feast. Ex.: `s3://some-bucket/some-prefix` |

See [Feast documentation](https://api.docs.feast.dev/python/#module-feast.constants) for more configuration options for EMR.

