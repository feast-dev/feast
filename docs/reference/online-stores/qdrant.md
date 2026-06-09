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
| retrieve_online_documents_v2 (vector search)              | yes      |
| hybrid text search (`query_string`)                       | yes (opt-in) |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).

## Vector search with retrieve_online_documents_v2

Qdrant stores one point per feature. `retrieve_online_documents_v2` runs dense vector search on the embedding feature, then joins sibling feature points by `entity_key` so the response includes every requested feature in a single `Dict[str, ValueProto]`.

{% code title="python" %}

```python
from feast import FeatureStore

store = FeatureStore(repo_path="feature_store.yaml")

results = store.retrieve_online_documents_v2(
    features=["documents:embedding", "documents:text_field"],
    query=[0.1, 0.2, 0.3],  # embedding vector
    top_k=5,
)
```

{% endcode %}

## Opt-in hybrid search

Hybrid keyword + vector search is **disabled by default**. Enable it in `feature_store.yaml`, recreate the collection (`feast teardown` + `feast apply`), and install `fastembed` so the Qdrant client can encode sparse vectors at upload time (via `models.Document`):

{% code title="feature_store.yaml" %}

```yaml
online_store:
    type: qdrant
    host: localhost
    port: 6333
    vector_enabled: true
    text_search_enabled: true
    sparse_vector_name: sparse
    sparse_embedding_model: Qdrant/bm25
    text_feature: text_field  # optional; defaults to first STRING feature
```

{% endcode %}

{% code title="python" %}

```python
results = store.retrieve_online_documents_v2(
    features=["documents:embedding", "documents:text_field"],
    query=[0.1, 0.2, 0.3],
    query_string="searchable keywords",
    top_k=5,
)
```

{% endcode %}

Hybrid queries use Qdrant's Query API with reciprocal rank fusion (RRF) over dense and sparse vectors.

## Retrieving online document vectors (v1)

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
