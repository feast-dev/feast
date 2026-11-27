# Valkey online store

## Description

[Valkey](https://valkey.io/) is an open source (BSD-3-Clause), high-performance key/value datastore hosted by the Linux Foundation, created as a community fork of Redis. It maintains compatibility with the Redis wire protocol, so it can act as a drop-in replacement for Redis. Valkey is also offered as a managed engine by major cloud providers (for example, Amazon ElastiCache for Valkey).

Similar to Redis and [Dragonfly](dragonfly.md), Valkey can be used as an online feature store for Feast: Feast's Redis online store only issues core commands (hash reads/writes, scans, key expiry, pipelines), all of which Valkey implements.

Feast's standard online store operations have been verified against Valkey 8.1: `feast apply`, `feast materialize`, online retrieval via `get_online_features`, `feast teardown`, and key expiry via the `key_ttl_seconds` option. Features that depend on Redis modules (such as vector search) are outside the scope of this page.

## Using Valkey as a drop-in Feast online store instead of Redis

Make sure you have Python and `pip` installed.

Install the Feast SDK and CLI

`pip install feast`

In order to use Valkey as the online store, you'll need to install the redis extra:

`pip install 'feast[redis]'`

### 1. Create a feature repository

Bootstrap a new feature repository:

```
feast init feast_valkey
cd feast_valkey/feature_repo
```

Update `feature_repo/feature_store.yaml` with the below contents:

```
project: feast_valkey
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: "localhost:6379"
```

Note that the online store `type` remains `redis`: Feast talks to Valkey over the Redis protocol, and all options of the [Redis online store](redis.md) (such as `key_ttl_seconds`) apply unchanged.

### 2. Start Valkey

There are several options available to get Valkey up and running quickly. We will be using Docker for this tutorial.

`docker run -d -p 6379:6379 valkey/valkey:8.1`

### 3. Register feature definitions and deploy your feature store

`feast apply`

The `apply` command scans python files in the current directory for feature view/entity definitions, registers the objects, and deploys infrastructure.
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
Below is a matrix indicating which functionality is supported by the Redis online store, which Feast uses to communicate with Valkey.

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
| collocated by feature service                              | no    |
| collocated by entity key                                  | yes   |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
