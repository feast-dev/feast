# MongoDB online store (Alpha)

## Description

The [MongoDB](https://www.mongodb.com/) online store provides support for materializing feature values into MongoDB for serving online features.

{% hint style="warning" %}
The MongoDB online store is currently in **alpha development**. Some functionality may be unstable, and breaking changes may occur in future releases.
{% endhint %}

## Features

* Supports both synchronous and asynchronous operations for high-performance feature retrieval
* Native async support uses PyMongo's `AsyncMongoClient` (no Motor dependency required)
* Flexible connection options supporting MongoDB Atlas, self-hosted MongoDB, and MongoDB replica sets
* Automatic index creation for optimized query performance
* Entity key collocation for efficient feature retrieval

## Getting started

In order to use this online store, you'll need to install the MongoDB extra (along with the dependency needed for the offline store of choice):

```bash
pip install 'feast[mongodb]'
```

You can get started by using any of the other templates (e.g. `feast init -t gcp` or `feast init -t snowflake` or `feast init -t aws`), and then swapping in MongoDB as the online store as seen below in the examples.

## Examples

### Basic configuration with MongoDB Atlas

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: mongodb
  connection_string: "mongodb+srv://username:password@cluster.mongodb.net/"
  database_name: feast_online_store
```
{% endcode %}

### Self-hosted MongoDB with authentication

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: mongodb
  connection_string: "mongodb://username:password@localhost:27017/"
  database_name: feast_online_store
  collection_suffix: features
```
{% endcode %}

### MongoDB replica set configuration

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: mongodb
  connection_string: "mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myReplicaSet"
  database_name: feast_online_store
  client_kwargs:
    retryWrites: true
    w: majority
```
{% endcode %}

### Advanced configuration with custom client options

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: mongodb
  connection_string: "mongodb+srv://cluster.mongodb.net/"
  database_name: feast_online_store
  collection_suffix: features
  client_kwargs:
    maxPoolSize: 50
    minPoolSize: 10
    serverSelectionTimeoutMS: 5000
    connectTimeoutMS: 10000
```
{% endcode %}

The full set of configuration options is available in [MongoDBOnlineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.contrib.mongodb_online_store.mongodb.MongoDBOnlineStoreConfig).

## Data Model

The MongoDB online store uses a **single collection per project** with entity key collocation. Features from multiple feature views for the same entity are stored together in a single document.

### Example Document Schema

The example shows a single entity. It contains 3 features from 2 feature views: "rating" and "trips_last7d" from Feature
View "driver_stats", and "surge_multiplier" from "pricing" view. 
Each feature view has its own event timestamp. 
The "created_timestamp" marks when the entity was materialized.

```javascript
{
  "_id": "<serialized_entity_key>",  // Binary entity key (bytes)
  "features": {
    "driver_stats": {
      "rating": 4.91,
      "trips_last_7d": 132
    },
    "pricing": {
      "surge_multiplier": 1.2
    }
  },
  "event_timestamps": {
    "driver_stats": ISODate("2026-01-20T12:00:00Z"),
    "pricing": ISODate("2026-01-21T08:30:00Z")
  },
  "created_timestamp": ISODate("2026-01-21T12:00:05Z")
}
```

### Key Design Decisions

* **`_id` field**: Uses the serialized entity key (bytes) as the primary key for efficient lookups
* **Nested features**: Features are organized by feature view name, allowing multiple feature views per entity
* **Event timestamps**: Stored per feature view to track when each feature set was last updated
* **Created timestamp**: Global timestamp for the entire document

### Indexes

The online store automatically creates the following index:
* Primary key index on `_id` (automatic in MongoDB), set to the serialized entity key.

No additional indexes are required for the online store operations.

## Async Support

The MongoDB online store provides native async support using PyMongo 4.13+'s stable `AsyncMongoClient`. This enables:

* **High concurrency**: Handle thousands of concurrent feature requests without thread pool limitations
* **True async I/O**: Non-blocking operations for better performance in async applications
* **10-20x performance improvement**: For concurrent workloads compared to sequential sync operations

Both sync and async methods are fully supported:
* `online_read` / `online_read_async`
* `online_write_batch` / `online_write_batch_async`

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the MongoDB online store.

|                                                           | MongoDB |
| :-------------------------------------------------------- | :------ |
| write feature values to the online store                  | yes     |
| read feature values from the online store                 | yes     |
| update infrastructure (e.g. tables) in the online store   | yes     |
| teardown infrastructure (e.g. tables) in the online store | yes     |
| generate a plan of infrastructure changes                 | no      |
| support for on-demand transforms                          | yes     |
| readable by Python SDK                                    | yes     |
| readable by Java                                          | no      |
| readable by Go                                            | no      |
| support for entityless feature views                      | yes     |
| support for concurrent writing to the same key            | yes     |
| support for ttl (time to live) at retrieval               | no      |
| support for deleting expired data                         | no      |
| collocated by feature view                                | no      |
| collocated by feature service                             | no      |
| collocated by entity key                                  | yes     |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).

