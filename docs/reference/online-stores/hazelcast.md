# Hazelcast online store

## Description

Hazelcast online store is in alpha development.

The [Hazelcast](htpps://hazelcast.com) online store provides support for materializing feature values into a Hazelcast cluster for serving online features in real-time.
In order to use Hazelcast as online store, you need to have a running Hazelcast cluster. You can create a cluster using Hazelcast Viridian Serverless. See this [getting started](https://hazelcast.com/get-started/) page for more details.

* Each feature view is mapped one-to-one to a specific Hazelcast IMap
* This implementation inherits all strengths of Hazelcast such as high availability, fault-tolerance, and data distribution.
* Secure TSL/SSL connection is supported by Hazelcast online store.
* You can set TTL (Time-To-Live) setting for your features in Hazelcast cluster. 

Each feature view corresponds to an IMap in Hazelcast cluster and the entries in that IMap corresponds to features of entities.
Each feature value stored separately and can be retrieved individually.

## Getting started

In order to use Hazelcast online store, you'll need to run `pip install 'feast[hazelcast]'`. You can then get started with the command `feast init REPO_NAME -t hazelcast`.


## Examples

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: hazelcast
  cluster_name: dev
  cluster_members: ["localhost:5701"]
  key_ttl_seconds: 36000
```

## Functionality Matrix

|                                                           | Hazelcast |
| :-------------------------------------------------------- |:----------|
| write feature values to the online store                  | yes       |
| read feature values from the online store                 | yes       |
| update infrastructure (e.g. tables) in the online store   | yes       |
| teardown infrastructure (e.g. tables) in the online store | yes       |
| generate a plan of infrastructure changes                 | no        |
| support for on-demand transforms                          | yes       |
| readable by Python SDK                                    | yes       |
| readable by Java                                          | no        |
| readable by Go                                            | no        |
| support for entityless feature views                      | yes       |
| support for concurrent writing to the same key            | yes       |
| support for ttl (time to live) at retrieval               | yes       |
| support for deleting expired data                         | yes       |
| collocated by feature view                                | no        |
| collocated by feature service                             | no        |
| collocated by entity key                                  | yes       |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).

