# IKV (Inlined Key-Value Store) online store

## Description

[IKV](https://github.com/inlinedio/ikv-store) is a fully-managed embedded key-value store, primarily designed for storing ML features. Most key-value stores (think Redis or Cassandra) need a remote database cluster, whereas IKV allows you to utilize your existing application infrastructure to store data (cost efficient) and access it without any network calls (better performance). 

For provisioning API keys for using it as an online-store in Feast, go to [https://inlined.io](https://inlined.io)

## Getting started

Make sure you have Python and `pip` installed.

Install the Feast SDK and CLI

`pip install feast`

In order to use IKV as the online store, you'll need to install the extra:

`pip install 'feast[ikv]'`

### 1. Provision an IKV store
Go to [https://inlined.io](https://inlined.io) or email onboarding[at]inlined.io
IKV does not support docker deployment for local testing at the moment.

### 2. Create a feature repository

Bootstrap a new feature repository:

```
feast init my_feature_repo
cd my_feature_repo/feature_repo
```

Update `my_feature_repo/feature_store.yaml` with the below contents:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: ikv
    account_id: secret
    account_passkey: secret
    store_name: your-store-name
    mount_directory: /absolute/path/on/disk/for/ikv/embedded/index
```
{% endcode %}

The full set of configuration options is available in IKVOnlineStoreConfig at `sdk/python/feast/infra/online_stores/contrib/ikv_online_store/ikv.py`

### 3. Register feature definitions and deploy your feature store

`feast apply`

The `apply` command scans python files in the current directory (`example_repo.py` in this case) for feature view/entity definitions, registers the objects, and deploys infrastructure.
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
