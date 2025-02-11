# Overview

## Functionality

Here are the methods exposed by the `OnlineStore` interface, along with the core functionality supported by the method:
* `online_write_batch`: write feature values to the online store
* `online_read`: read feature values from the online store
* `update`: update infrastructure (e.g. tables) in the online store
* `teardown`: teardown infrastructure (e.g. tables) in the online store
* `plan`: generate a plan of infrastructure changes based on feature repo changes

There is also additional functionality not properly captured by these interface methods:
* support for on-demand transforms
* readable by Python SDK
* readable by Java
* readable by Go
* support for entityless feature views
* support for concurrent writing to the same key
* support for ttl (time to live) at retrieval
* support for deleting expired data

Finally, there are multiple data models for storing the features in the online store. For example, features could be:
* collocated by feature view
* collocated by feature service
* collocated by entity key

See this [issue](https://github.com/feast-dev/feast/issues/2254) for a discussion around the tradeoffs of each of these data models.

## Functionality Matrix

There are currently five core online store implementations: `SqliteOnlineStore`, `RedisOnlineStore`, `DynamoDBOnlineStore`, `SnowflakeOnlineStore`, and `DatastoreOnlineStore`.
There are several additional implementations contributed by the Feast community  (`PostgreSQLOnlineStore`, `HbaseOnlineStore`, `CassandraOnlineStore` and `IKVOnlineStore`), which are not guaranteed to be stable or to match the functionality of the core implementations.
Details for each specific online store, such as how to configure it in a `feature_store.yaml`, can be found [here](README.md).

Below is a matrix indicating which online stores support what functionality.

| | Sqlite | Redis | DynamoDB | Snowflake | Datastore | Postgres | Hbase | [[Cassandra](https://cassandra.apache.org/_/index.html) / [Astra DB](https://www.datastax.com/products/datastax-astra?utm_source=feast)] | [IKV](https://inlined.io) | Milvus |
| :-------------------------------------------------------- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- |:-------|
| write feature values to the online store                  | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes    |
| read feature values from the online store                 | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes    |
| update infrastructure (e.g. tables) in the online store   | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes    |
| teardown infrastructure (e.g. tables) in the online store | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes    |
| generate a plan of infrastructure changes                 | yes | no  | no  | no  | no  | no  | no  | yes | no  | no     |
| support for on-demand transforms                          | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes    |
| readable by Python SDK                                    | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes    |
| readable by Java                                          | no  | yes | no  | no  | no  | no  | no  | no  | no  | no     |
| readable by Go                                            | yes | yes | no  | no  | no  | no  | no  | no  | no  | no     |
| support for entityless feature views                      | yes | yes | yes | yes | yes | yes | yes | yes | yes | no     |
| support for concurrent writing to the same key            | no  | yes | no  | no  | no  | no  | no  | no  | yes | no   |
| support for ttl (time to live) at retrieval               | no  | yes | no  | no  | no  | no  | no  | no  | no  | no  |
| support for deleting expired data                         | no  | yes | no  | no  | no  | no  | no  | no  | no  | no  |
| collocated by feature view                                | yes | no  | yes | yes | yes | yes | yes | yes | no  | no  |
| collocated by feature service                             | no  | no  | no  | no  | no  | no  | no  | no  | no  | no  |
| collocated by entity key                                  | no  | yes | no  | no  | no  | no  | no  | no  | yes | no  | 
