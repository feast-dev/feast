# Feast Offline store with Arrow Flight remote server

This POC proposes a solution to implement the issue [Remote offline feature server deployment](https://github.com/feast-dev/feast/issues/4032) 
using [Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) server as the remote Feast offline store.

## Architecture
The [offline_server](./offline_server) folder contains a sample Feast repository using the `feast init` command.
We can serve remote offline requests using the `feast server_offline` command that has been introduced as part of this new feature:
* Spins up an `Arrow Flight` server at default port 8815.
* The server accepts `do_get` requests for the `get_historical_features` command and delegates the implementation to the  
current `FeatureStore`.

The [offline_client](./offline_client) folder includes a test python function that uses an offline store of type `remote`, leveraging the remote server as the 
actual data provider. The offline store is implementated by the new `RemoteOfflineStore` class:
* For now it implements only the `get_historical_features` method.
* Since the implementation is lazy, the returned `RetrievalJob` does not run the `do_get` request until any method to synchronously execute
the underlying query is invoked (e.g., `to_df` or `to_arrow`).

## Parameter transfer protocol
The `Apache Arrow Flight` protocol defines generic APIs for efficient data streaming to and from the server, leveraging the gRPC communication framework.

The server exposes services to retrieve or push data streams associated with a particular `Flight` descriptor, using the `do_get` and `do_put` endpoints,
but the APIs are not sufficiently detailed for the use that we have in mind, e.g. **adopt the Arrow Flight server as a generic gRPC server
to serve the Feast offline APIs**.

Each API in the `OfflineStore` interface has multiple parameters that have to be transferred to the server to perform the actual implementation.
The way we implement the parameter transfer protocol is the following:
* The client, e.g. the `RemoteOfflineStore` instance, receives a call to the `get_historical_features` API with the required parameters 
(e.g., `entity_df` and `feature_refs`).
* The client creates a unique identifier for a new command, using the UUID format, and generates a `Flight Descriptor` to represent it.
* Then the client sends the received API parameters to the server using multiple calls to the `do_put` service
  * Each call includes the data stream of the parameter value (e.g. the `entity_df` DataFrame).
  * Each call also adds additional metadata values to the data schema to let the server identify the exact API to invoke:
      * A `command` metadata with the unique command identifier calculated before.
      * An `api` metadata with the name of the API to be invoked remotely, e.g. `get_historical_features`.
      * A `param` metadata with the name of each parameter of the API.
* When the server receives the `do_put` calls, it stores the data in memory, using an ad-hoc `flights` dictionary indexed by the unique 
`command` identifier and storing a document with the streamed metadata values:
```json
{
    "(b'8d6a366a-c8d3-4f96-b085-f9f686111815')": {
        "command": "8d6a366a-c8d3-4f96-b085-f9f686111815",
        "api": "get_historical_features",
        "entity_df": ".....",
        "features": "...."
    }
}
```
* Indexing a flight descriptor by a unique `command` identifier enables the server to efficiently handle concurrent requests from multiple clients
for the same service without any overlaps.
* Since the client implementation is lazy, the returned instance of `RemoteRetrievalJob` invokes the `do_get` service on the server only when
the data is requested, e.g. in the `_to_arrow_internal` method.
* When the server receives the `do_get` request, it unpacks the API parameters from the `flights` dictionary and, if the requested API is
set to `get_historical_features`, forwards the request to the internal instance of `FeatureStore`.
* Once the `do_get` request is consumed, the associated flight is removed from the `flights` dictionary, as we do not expect the same 
API request to be executed twice from any client.

Other APIs of the `OfflineStore` interface can be implemented the same way, assuming that both the client and the server implementation 
agree on the parameter transfer protocol to be used to let the server execute the service remotely.

As an alternative, APIs that do not have any returned data may be implemented as a `do_action` service in the server.

## Validating the POC
### Launch the offline server
A default Feast store has been defined in [offline_server/feature_repo](./offline_server/feature_repo/) using the `feast init` command
and must be first initialized with:
```console
cd offline_server
feast -c feature_repo apply
```

Then the offline server can be started at the default port 8815 with:
```console
feast -c feature_repo serve_offline
```

Sample output:
```console
Serving on grpc+tcp://127.0.0.1:8815
get_historical_features for: entity_df from 0 to 2, features from driver_hourly_stats:conv_rate to transformed_conv_rate:conv_rate_plus_val2
```

## Launch a remote offline client
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

Sample output of `cd offline_client; python test.py`:
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

