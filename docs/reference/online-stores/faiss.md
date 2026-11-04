# Faiss online store

## Description

The [Faiss](https://github.com/facebookresearch/faiss) online store provides support for materializing feature values and performing vector similarity search using Facebook AI Similarity Search (Faiss). Faiss is a library for efficient similarity search and clustering of dense vectors, making it well-suited for use cases involving embeddings and nearest-neighbor lookups.

## Getting started
In order to use this online store, you'll need to install the Faiss dependency. E.g.

`pip install 'feast[faiss]'`

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: feast.infra.online_stores.faiss_online_store.FaissOnlineStore
  dimension: 128
  index_path: data/faiss_index
  index_type: IVFFlat    # optional, default: IVFFlat
  nlist: 100             # optional, default: 100
```
{% endcode %}

**Note:** Faiss is not registered as a named online store type. You must use the fully qualified class path as the `type` value.

The full set of configuration options is available in [FaissOnlineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.online_stores.faiss_online_store.FaissOnlineStoreConfig).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Faiss online store.

|                                                           | Faiss |
|:----------------------------------------------------------|:------|
| write feature values to the online store                  | yes   |
| read feature values from the online store                 | yes   |
| update infrastructure (e.g. tables) in the online store   | yes   |
| teardown infrastructure (e.g. tables) in the online store | yes   |
| generate a plan of infrastructure changes                 | no    |
| support for on-demand transforms                          | yes   |
| readable by Python SDK                                    | yes   |
| readable by Java                                          | no    |
| readable by Go                                            | no    |
| support for entityless feature views                      | yes   |
| support for concurrent writing to the same key            | no    |
| support for ttl (time to live) at retrieval               | no    |
| support for deleting expired data                         | no    |
| collocated by feature view                                | yes   |
| collocated by feature service                             | no    |
| collocated by entity key                                  | no    |
| vector similarity search                                  | yes   |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
