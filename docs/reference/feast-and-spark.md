---
description: Configuring Feast to use Spark for ingestion.
---

# Feast and Spark

Feast relies on Spark to ingest data from the offline store to the online store, streaming ingestion, and running queries to retrieve historical data from the offline store. Feast supports several Spark deployment options.

## Option 1. Use Kubernetes Operator for Apache Spark

To install the Spark on K8s Operator

```bash
helm repo add spark-operator \
    https://googlecloudplatform.github.io/spark-on-k8s-operator

helm install my-release spark-operator/spark-operator \
    --set serviceAccounts.spark.name=spark
```

Currently Feast is tested using `v1beta2-1.1.2-2.4.5`version of the operator image. To configure Feast to use it, set the following options in Feast config:

| Feast Setting | Value |
| :--- | :--- |
| `SPARK_LAUNCHER` | `"k8s"` |
| `SPARK_STAGING_LOCATION` | S3/GCS/Azure Blob Storage URL to use as a staging location, must be readable and writable by Feast. For S3, use `s3a://` prefix here. Ex.: `s3a://some-bucket/some-prefix/artifacts/` |
| `HISTORICAL_FEATURE_OUTPUT_LOCATION` | S3/GCS/Azure Blob Storage URL used to store results of historical retrieval queries, must be readable and writable by Feast. For S3, use `s3a://` prefix here. Ex.: `s3a://some-bucket/some-prefix/out/` |
| `SPARK_K8S_NAMESPACE` | Only needs to be set if you are customizing the spark-on-k8s-operator. The name of the Kubernetes namespace to run Spark jobs in. This should match the value of `sparkJobNamespace` set on spark-on-k8s-operator Helm chart. Typically this is also the namespace Feast itself will run in. |
| `SPARK_K8S_JOB_TEMPLATE_PATH` | Only needs to be set if you are customizing the Spark job template. Local file path with the template of the SparkApplication resource. No prefix required. Ex.: `/home/jovyan/work/sparkapp-template.yaml`. An example template is [here](https://github.com/feast-dev/feast/blob/4059a21dc4eba9cd27b2d5b0fabe476c07a8b3bd/sdk/python/feast/pyspark/launchers/k8s/k8s_utils.py#L280-L317) and the spec is defined in the [k8s-operator User Guide](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/user-guide.md). |

Lastly, make sure that the service account used by Feast has permissions to manage Spark Application resources. This depends on your k8s setup, but typically you'd need to configure a Role and a RoleBinding like the one below:

```text
cat <<EOF | kubectl apply -f -
kind: Role
apiVersion: rbac.authorization.k8s.io/v1beta1
metadata:
  name: use-spark-operator
  namespace: default  # replace if using different namespace
rules:
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["create", "delete", "deletecollection", "get", "list", "update", "watch", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1beta1
kind: RoleBinding
metadata:
  name: use-spark-operator
  namespace: default  # replace if using different namespace
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

