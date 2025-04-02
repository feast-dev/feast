# Qdrant online store

## Description

[Qdrant](http://qdrant.tech) is a vector similarity search engine. It provides a production-ready service with a convenient API to store, search, and manage vectors with additional payload and extended filtering support. It makes it useful for all sorts of neural network or semantic-based matching, faceted search, and other applications.

## Getting started

In order to use this online store, you'll need to run `pip install 'feast[qdrant]'`.

## Example

{% code title="feature_store.yaml" %}

```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: qdrant
    host: localhost
    port: 6333
    vector_len: 384
    write_batch_size: 100
```

{% endcode %}

The full set of configuration options is available in [QdrantOnlineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.online_stores.qdrant_online_store.QdrantOnlineStoreConfig).

## Functionality Matrix

|                                                           | Qdrant   |
| :-------------------------------------------------------- | :------- |
| write feature values to the online store                  | yes      |
| read feature values from the online store                 | yes      |
| update infrastructure (e.g. tables) in the online store   | yes      |
| teardown infrastructure (e.g. tables) in the online store | yes      |
| generate a plan of infrastructure changes                 | no       |
| support for on-demand transforms                          | yes      |
| readable by Python SDK                                    | yes      |
| readable by Java                                          | no       |
| readable by Go                                            | no       |
| support for entityless feature views                      | yes      |
| support for concurrent writing to the same key            | no       |
| support for ttl (time to live) at retrieval               | no       |
| support for deleting expired data                         | no       |
| collocated by feature view                                | yes      |
| collocated by feature service                             | no       |
| collocated by entity key                                  | no       |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).

## Retrieving online document vectors

The Qdrant online store supports retrieving document vectors for a given list of entity keys. The document vectors are returned as a dictionary where the key is the entity key and the value is the document vector. The document vector is a dense vector of floats.

{% code title="python" %}

```python
from feast import FeatureStore

feature_store = FeatureStore(repo_path="feature_store.yaml")

query_vector = [1.0, 2.0, 3.0, 4.0, 5.0]
top_k = 5

# Retrieve the top k closest features to the query vector
# Since Qdrant supports multiple vectors per entry,
# the vector to use can be specified in the repo config.
# Reference: https://qdrant.tech/documentation/concepts/vectors/#named-vectors
feature_values = feature_store.retrieve_online_documents(
    features=["my_feature"],
    query=query_vector,
    top_k=top_k
)
```

{% endcode %}

These APIs are subject to change in future versions of Feast to improve performance and usability.
