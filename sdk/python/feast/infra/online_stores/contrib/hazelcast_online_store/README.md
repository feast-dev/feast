# Hazelcast Online Store

This contribution makes it possible to use [Hazelcast](https://hazelcast.com/) as online store for Feast.

Once the Hazelcast client configuration is given inside `feature_store.yaml` file, everything else
is handled as with any other online store: schema creation, read/write from/to Hazelcast and remove operations.

## Quick usage

The following refers to the [Feast quickstart](https://docs.feast.dev/getting-started/quickstart) page. 
Only the Step 2 is different from this tutorial since it requires you to configure your Hazelcast online store.

### Creating the feature repository

The easiest way to get started is to use the Feast CLI to initialize a new
feature store. Once Feast is installed, the command

```
feast init FEATURE_STORE_NAME -t hazelcast
```

will interactively help you create the `feature_store.yaml` with the
required configuration details to access your Hazelcast cluster.

Alternatively, you can run `feast init -t FEATURE_STORE_NAME`, as described
in the quickstart, and then manually edit the `online_store` section in
the `feature_store.yaml` file as detailed below.

The following steps (setup of feature definitions, deployment of the store,
generation of training data, materialization, fetching of online/offline
features) proceed exactly as in the general Feast quickstart instructions.

#### Hazelcast setup

In order to use [Hazelcast](https://hazelcast.com) as online store, you need to have a running Hazelcast cluster. 
You can create a cluster using Hazelcast Viridian Serverless easily or deploy one on your local/remote machine. 
See this [getting started](https://hazelcast.com/get-started/) page for more details.

Hazelcast online store provides capability to connect local/remote or Hazelcast Viridian Serverless cluster.
Following is an example to connect local cluster named "dev" running on port 5701 with TLS/SSL enabled.

```yaml
[...]
online_store:
    type: hazelcast
    cluster_name: dev
    cluster_members: ["localhost:5701"]
    ssl_cafile_path: /path/to/ca/file
    ssl_certfile_path: /path/to/cert/file
    ssl_keyfile_path: /path/to/key/file
    ssl_password: ${SSL_PASSWORD} # The password will be read form the `SSL_PASSWORD` environment variable.
    key_ttl_seconds: 86400 # The default is 0 and means infinite.
```

If you want to connect your Hazelcast Viridian cluster instead of local/remote one, specify your configuration as follows:

```yaml
[...]
online_store:
    type: hazelcast
    cluster_name: YOUR_CLUSTER_ID
    discovery_token: YOUR_DISCOVERY_TOKEN
    ssl_cafile_path: /path/to/ca/file
    ssl_certfile_path: /path/to/cert/file
    ssl_keyfile_path: /path/to/key/file
    ssl_password: ${SSL_PASSWORD} # The password will be read form the `SSL_PASSWORD` environment variable.
    key_ttl_seconds: 86400 # The default is 0 and means infinite.
```

#### TTL configuration

TTL is the maximum time in seconds for each feature to stay idle in the map.
It limits the lifetime of the features relative to the time of the last read or write access performed on them. 
The features whose idle period exceeds this limit are expired and evicted automatically. 
A feature is idle if no get or put is called on it. 
Valid values are integers between 0 and Integer.MAX_VALUE.
Its default value is 0, which means infinite.

```yaml
[...]
online_store:
    [...]
    key_ttl_seconds: 86400
```

### More info

You can learn about Hazelcast more from the [Hazelcast Documentation](https://docs.hazelcast.com/home/).

