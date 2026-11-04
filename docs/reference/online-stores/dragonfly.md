# Dragonfly online store

## Description

[Dragonfly](https://github.com/dragonflydb/dragonfly) is a modern in-memory datastore that implements novel algorithms and data structures on top of a multi-threaded, share-nothing architecture. Thanks to its API compatibility, Dragonfly can act as a drop-in replacement for Redis. Due to Dragonfly's hardware efficiency, you can run a single node instance on a small 8GB instance or scale vertically to large 768GB machines with 64 cores. This greatly reduces infrastructure costs as well as architectural complexity.

Similar to Redis, Dragonfly can be used as an online feature store for Feast.

## Using Dragonfly as a drop-in Feast online store instead of Redis

Make sure you have Python and `pip` installed.

Install the Feast SDK and CLI

`pip install feast`

In order to use Dragonfly as the online store, you'll need to install the redis extra:

`pip install 'feast[redis]'`

### 1. Create a feature repository

Bootstrap a new feature repository:

```
feast init feast_dragonfly
cd feast_dragonfly/feature_repo
```

Update `feature_repo/feature_store.yaml` with the below contents:

```
project: feast_dragonfly
registry: data/registry.db
provider: local
online_store:
type: redis
connection_string: "localhost:6379"
```

### 2. Start Dragonfly

There are several options available to get Dragonfly up and running quickly. We will be using Docker for this tutorial.

`docker run --network=host --ulimit memlock=-1 docker.dragonflydb.io/dragonflydb/dragonfly`

### 3. Register feature definitions and deploy your feature store

`feast apply`

The `apply` command scans python files in the current directory (`feature_definitions.py` in this case) for feature view/entity definitions, registers the objects, and deploys infrastructure.
You should see the following output:

```
....
Created entity driver
Created feature view driver_hourly_stats_fresh
Created feature view driver_hourly_stats
Created on demand feature view transformed_conv_rate
Created on demand feature view transformed_conv_rate_fresh
Created feature service driver_activity_v1
Created feature service driver_activity_v3
Created feature service driver_activity_v2
```

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Redis online store.

|                                                           | Redis |
| :-------------------------------------------------------- | :---- |
| write feature values to the online store                  | yes   |
| read feature values from the online store                 | yes   |
| update infrastructure (e.g. tables) in the online store   | yes   |
| teardown infrastructure (e.g. tables) in the online store | yes   |
| generate a plan of infrastructure changes                 | no    |
| support for on-demand transforms                          | yes   |
| readable by Python SDK                                    | yes   |
| readable by Java                                          | yes   |
| readable by Go                                            | yes   |
| support for entityless feature views                      | yes   |
| support for concurrent writing to the same key            | yes   |
| support for ttl (time to live) at retrieval               | yes   |
| support for deleting expired data                         | yes   |
| collocated by feature view                                | no    |
| collocated by feature service                             | no    |
| collocated by entity key                                  | yes   |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
