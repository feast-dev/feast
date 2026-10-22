# Feast Remote Offline Store Server 

This example demonstrates the steps using an  [Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) server/client as the remote Feast offline store. 

## Launch the offline server locally 

1. **Create Feast Project**: Using the `feast init` command for example the [offline_server](./offline_server) folder contains a sample Feast repository.

2. **Start Remote Offline Server**: Use the `feast server_offline` command to start remote offline requests. This command will:
  - Spin up an `Arrow Flight` server at the default port 8815.

3. **Initialize Offline Server**: The offline server can be initialized by providing the `feature_store.yml` file via an environment variable named `FEATURE_STORE_YAML_BASE64`. A temporary directory will be created with the provided YAML file named `feature_store.yml`.

Example 

```console
cd offline_server
feast -c feature_repo apply
```

```console
feast -c feature_repo serve_offline
```

Sample output:
```console
Serving on grpc+tcp://127.0.0.1:8815
```

## Launch a remote offline client 

The [offline_client](./offline_client) folder includes a test python function that uses an offline store of type `remote`, leveraging the remote server as the 
actual data provider. 


The test class is located under [offline_client](./offline_client/) and uses a remote configuration of the offline store to delegate the actual 
implementation to the offline store server:
```yaml
offline_store:
    type: remote
    host: localhost
    port: 8815
```

The test code in [test.py](./offline_client/test.py) initializes the store from the local configuration and then fetches the historical features
from the  store like any other Feast client, but the actual implementation is delegated to the offline server
```py
store = FeatureStore(repo_path=".")
training_df = store.get_historical_features(entity_df, features).to_df()
```


Run client  
`cd offline_client;
  python test.py`

Sample output:

```console
config.offline_store is <class 'feast.infra.offline_stores.remote.RemoteOfflineStoreConfig'>
----- Feature schema -----

<class 'pandas.core.frame.DataFrame'>
RangeIndex: 3 entries, 0 to 2
Data columns (total 10 columns):
 #   Column                              Non-Null Count  Dtype              
---  ------                              --------------  -----              
 0   driver_id                           3 non-null      int64              
 1   event_timestamp                     3 non-null      datetime64[ns, UTC]
 2   label_driver_reported_satisfaction  3 non-null      int64              
 3   val_to_add                          3 non-null      int64              
 4   val_to_add_2                        3 non-null      int64              
 5   conv_rate                           3 non-null      float32            
 6   acc_rate                            3 non-null      float32            
 7   avg_daily_trips                     3 non-null      int32              
 8   conv_rate_plus_val1                 3 non-null      float64            
 9   conv_rate_plus_val2                 3 non-null      float64            
dtypes: datetime64[ns, UTC](1), float32(2), float64(2), int32(1), int64(4)
memory usage: 332.0 bytes
None

-----  Features -----

   driver_id           event_timestamp  label_driver_reported_satisfaction  ...  avg_daily_trips  conv_rate_plus_val1  conv_rate_plus_val2
0       1001 2021-04-12 10:59:42+00:00                                   1  ...              590             1.022378            10.022378
1       1002 2021-04-12 08:12:10+00:00                                   5  ...              974             2.762213            20.762213
2       1003 2021-04-12 16:40:26+00:00                                   3  ...              127             3.419828            30.419828

[3 rows x 10 columns]
------training_df----
   driver_id           event_timestamp  label_driver_reported_satisfaction  ...  avg_daily_trips  conv_rate_plus_val1  conv_rate_plus_val2
0       1001 2021-04-12 10:59:42+00:00                                   1  ...              590             1.022378            10.022378
1       1002 2021-04-12 08:12:10+00:00                                   5  ...              974             2.762213            20.762213
2       1003 2021-04-12 16:40:26+00:00                                   3  ...              127             3.419828            30.419828

[3 rows x 10 columns]
```

