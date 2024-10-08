# Couchbase Online Store
Utilizes Couchbase Server as the underlying store for the online feature store.


#### Create a feature repository

```shell
feast init feature_repo
cd feature_repo
```

#### Edit `feature_store.yaml`

set `online_store` type to be `couchbase`

```yaml
project: feature_repo
registry: data/registry.db
provider: local
online_store:
  type: couchbase
  host: 127.0.0.1  # Couchbase host, default to 127.0.0.1
  user: Administrator  # Couchbase username, default to Administrator
  password: password  # Couchbase password, default to password
  bucket_name: feast  # Couchbase bucket name, default to feast
entity_key_serialization_version: 2
```

#### Apply the feature definitions in `example.py` (go/internal/test/feature_repo/example.py)
[//]: # (// todo: link for example.py)

```shell
feast -c feature_repo apply
```
##### Output
```
Registered entity driver_id
Registered feature view driver_hourly_stats_view
Deploying infrastructure for driver_hourly_stats_view
```

### Materialize Latest Data to Online Feature Store (Couchbase)
```
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
```
{'acc_rate': [0.01390857808291912, 0.4063614010810852],
 'avg_daily_trips': [69, 706],
 'conv_rate': [0.6624961495399475, 0.7595928311347961],
 'driver_id': [1004, 1005]}
```
