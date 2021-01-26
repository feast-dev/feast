---
description: Configuring Feast to use Spark for ingestion.
---

# Feast and Spark

Feast relies on Spark to ingest data from the offline store to the online store, streaming ingestion, and running queries to retrieve historical data from the offline store. Feast supports several Spark deployment options.

### Option 1. Use Kubernetes Operator for Apache Spark

To install Kubernetes Operator, follow [instructions](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator) in the repository. Currently Feast is tested using `v1beta2-1.1.2-2.4.5`version of the operator image. To configure Feast to use it, set the following options in Feast config:

<table>
  <thead>
    <tr>
      <th style="text-align:left">Feast Setting</th>
      <th style="text-align:left">Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">
        <p></p>
        <p><code>SPARK_LAUNCHER</code>
        </p>
      </td>
      <td style="text-align:left"><code>&quot;k8s&quot;</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p></p>
        <p><code>SPARK_K8S_NAMESPACE</code>
        </p>
      </td>
      <td style="text-align:left">The name of the Kubernetes namespace to run Spark jobs in. This should
        match the value of <code>sparkJobNamespace</code> set on spark-on-k8s-operator
        Helm chart. Typically this is also the namespace Feast itself will run
        in.</td>
    </tr>
    <tr>
      <td style="text-align:left"><code>SPARK_STAGING_LOCATION</code>
      </td>
      <td style="text-align:left">S3 URL to use as a staging location, must be readable and writable by
        Feast. Use <code>s3a://</code> prefix here. Ex.: <code>s3a://some-bucket/some-prefix</code>
      </td>
    </tr>
  </tbody>
</table>

Lastly, make sure that the service account used by Feast has permissions to manage Spark Application resources. This depends on your k8s setup, but typically you'd need to configure a Role and a RoleBinding like the one below: 

```text
cat <<EOF | kubectl apply -f -
kind: Role
apiVersion: rbac.authorization.k8s.io/v1beta1
metadata:
  name: use-spark-operator
  namespace: <REPLACE ME>
rules:
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["create", "delete", "deletecollection", "get", "list", "update", "watch", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1beta1
kind: RoleBinding
metadata:
  name: use-spark-operator
  namespace: <REPLACE ME>
roleRef:
  kind: Role
  name: use-spark-operator
  apiGroup: rbac.authorization.k8s.io
subjects:
  - kind: ServiceAccount
    name: default
EOF
```

### Option 2. Use GCP and Dataproc

If you're running Feast in Google Cloud, you can use Dataproc, a managed Spark platform. To configure Feast to use it, set the following options in Feast config:

| Feast Setting | Value |
| :--- | :--- |
| `SPARK_LAUNCHER` | `"dataproc"` |
| `DATAPROC_CLUSTER_NAME` | Dataproc cluster name |
| `DATAPROC_PROJECT` | Dataproc project name |
| `SPARK_STAGING_LOCATION` | GCS URL to use as a staging location, must be readable and writable by Feast. Ex.: `gs://some-bucket/some-prefix` |

See [Feast documentation](https://api.docs.feast.dev/python/#module-feast.constants) for more configuration options for Dataproc.

### Option 3. Use AWS and EMR

If you're running Feast in AWS, you can use EMR, a managed Spark platform. To configure Feast to use it, set at least the following options in Feast config:

| Feast Setting | Value |
| :--- | :--- |
| `SPARK_LAUNCHER` | `"emr"` |
| `SPARK_STAGING_LOCATION` | S3 URL to use as a staging location, must be readable and writable by Feast. Ex.: `s3://some-bucket/some-prefix` |

See [Feast documentation](https://api.docs.feast.dev/python/#module-feast.constants) for more configuration options for EMR.

