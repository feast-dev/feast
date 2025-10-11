# HDFS Registry

## Description

HDFS registry provides support for storing the protobuf representation of your feature store objects (data sources, feature views, feature services, etc.) in Hadoop Distributed File System (HDFS).

While it can be used in production, there are still inherent limitations with a file-based registries, since changing a single field in the registry requires re-writing the whole registry file. With multiple concurrent writers, this presents a risk of data loss, or bottlenecks writes to the registry since all changes have to be serialized (e.g. when running materialization for multiple feature views or time ranges concurrently).

### Authentication and User Configuration

The HDFS registry is using `pyarrow.fs.HadoopFileSystem` and **does not** support specifying HDFS users or Kerberos credentials directly in the `feature_store.yaml` configuration. It relies entirely on the Hadoop and system environment configuration available to the process running Feast.

By default, `pyarrow.fs.HadoopFileSystem` inherits authentication from the underlying Hadoop client libraries and environment variables, such as:

- `HADOOP_USER_NAME`
- `KRB5CCNAME`
- `hadoop.security.authentication`
- Any other relevant properties in `core-site.xml` and `hdfs-site.xml`

For more information, refer to:
- [pyarrow.fs.HadoopFileSystem API Reference](https://arrow.apache.org/docs/python/generated/pyarrow.fs.HadoopFileSystem.html)
- [Hadoop Security: Simple & Kerberos Authentication](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html)

## Example

An example of how to configure this would be:

{% code title="feature_store.yaml" %}
```yaml
project: feast_hdfs
registry:
  path: hdfs://[YOUR NAMENODE HOST]:[YOUR NAMENODE PORT]/[PATH TO REGISTRY]/registry.pb
  cache_ttl_seconds: 60
online_store: null
offline_store:
  type: dask
```
{% endcode %}

