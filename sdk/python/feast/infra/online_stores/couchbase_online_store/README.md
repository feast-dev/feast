# Couchbase Online Store
> NOTE:
> This is a community-contributed online store that is in alpha development. It is not officially supported by the Feast project.

This contribution makes it possible to use [Couchbase Capella Operational](https://docs.couchbase.com/cloud/get-started/intro.html) as an online store for Feast.


### Get Started with Couchbase Capella Operational
You'll need a Couchbase Capella Operational cluster to use this online store. Follow the steps below to get started:
1. [Create a Couchbase Capella account](https://docs.couchbase.com/cloud/get-started/create-account.html#sign-up-free-tier)
2. [Deploy an Operational cluster](https://docs.couchbase.com/cloud/get-started/create-account.html#getting-started)
3. [Create a bucket](https://docs.couchbase.com/cloud/clusters/data-service/manage-buckets.html#add-bucket)
    - This can be named anything, but must correspond to the bucket described in the `feature_store.yaml` configuration file.
    - The default bucket name is `feast`.
4. [Create cluster access credentials](https://docs.couchbase.com/cloud/clusters/manage-database-users.html#create-database-credentials)
    - These credentials should have full access to the bucket created in step 3.
5. [Configure allowed IP addresses](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html)
    - You must allow the IP address of the machine running Feast.

### Use Couchbase Online Store with Feast

#### Create a feature repository

```shell
feast init feature_repo
cd feature_repo
```

#### Edit `feature_store.yaml`

Set the `online_store` type to `couchbase.online`, and fill in the required fields as shown below.

```yaml
project: feature_repo
registry: data/registry.db
provider: local
online_store:
  type: couchbase.online
  connection_string: couchbase://127.0.0.1 # Couchbase connection string, copied from 'Connect' page in Couchbase Capella console
  user: Administrator  # Couchbase username from access credentials
  password: password  # Couchbase password from access credentials
  bucket_name: feast  # Couchbase bucket name, defaults to feast
  kv_port: 11210  # Couchbase key-value port, defaults to 11210. Required if custom ports are used. 
entity_key_serialization_version: 3
```

#### Apply the feature definitions in [`example.py`](https://github.com/feast-dev/feast/blob/master/go/internal/test/feature_repo/example.py)

```shell
feast -c feature_repo apply
```
##### Output
```
Registered entity driver_id
Registered feature view driver_hourly_stats_view
Deploying infrastructure for driver_hourly_stats_view
```

### Materialize Latest Data to Couchbase Online Feature Store
```shell
$ CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S") 
$ feast -c feature_repo materialize-incremental $CURRENT_TIME
```
#### Output
```
Materializing 1 feature views from 2022-04-16 15:30:39+05:30 to 2022-04-19 15:31:04+05:30 into the Couchbase online store.

driver_hourly_stats_view from 2022-04-16 15:30:39+05:30 to 2022-04-19 15:31:04+05:30:
100%|████████████████████████████████████████████████████████████████| 5/5 [00:00<00:00, 120.59it/s]
```

### Fetch the latest features for some entity id
```python
from pprint import pprint
from feast import FeatureStore

store = FeatureStore(repo_path=".")
feature_vector = store.get_online_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
    entity_rows=[
        {"driver_id": 1004},
        {"driver_id": 1005},
    ],
).to_dict()
pprint(feature_vector)

```
#### Output
```python
{'acc_rate': [0.01390857808291912, 0.4063614010810852],
 'avg_daily_trips': [69, 706],
 'conv_rate': [0.6624961495399475, 0.7595928311347961],
 'driver_id': [1004, 1005]}
```
