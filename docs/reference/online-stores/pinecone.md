# Pinecone online store

## Description

The [Pinecone](https://www.pinecone.io/) online store provides support for materializing feature values into a Pinecone vector database. It is particularly suited for AI workloads that require both low-latency feature serving and vector similarity search for RAG (Retrieval-Augmented Generation) applications.

## Getting started

In order to use this online store, you'll need to install the Pinecone extra (along with the dependency needed for the offline store of choice). E.g.

`pip install 'feast[pinecone]'`

You will also need a Pinecone account and API key. Set your API key via the `PINECONE_API_KEY` environment variable or directly in `feature_store.yaml`.

## Examples

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: pinecone
  api_key: ${PINECONE_API_KEY}
  index_name: feast-online
  embedding_dim: 384
  metric: cosine
  cloud: aws
  region: us-east-1
  vector_enabled: true
```
{% endcode %}

### Using a custom namespace

By default, Feast maps each feature view to a Pinecone namespace (`{project}_{feature_view_name}`). You can override this with a fixed namespace:

{% code title="feature_store.yaml" %}
```yaml
online_store:
  type: pinecone
  api_key: ${PINECONE_API_KEY}
  index_name: feast-online
  namespace: my-custom-namespace
  embedding_dim: 384
```
{% endcode %}

### Vector similarity search

Pinecone supports the `retrieve_online_documents_v2` API for vector similarity search:

```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")

results = store.retrieve_online_documents_v2(
    features=[
        "city_embeddings:vector",
        "city_embeddings:sentence_chunks",
    ],
    query=query_embedding,
    top_k=5,
    distance_metric="cosine",
).to_df()
```

The full set of configuration options is available in [PineconeOnlineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStoreConfig).

## Configuration options

| Option | Type | Default | Description |
|:---|:---|:---|:---|
| `type` | string | `"pinecone"` | Must be `pinecone` |
| `api_key` | string | `None` | Pinecone API key (falls back to `PINECONE_API_KEY` env var) |
| `index_name` | string | `"feast-online"` | Name of the Pinecone index |
| `namespace` | string | `None` | Override namespace (default: `{project}_{fv_name}`) |
| `embedding_dim` | int | `128` | Vector dimension |
| `metric` | string | `"cosine"` | Distance metric: `cosine`, `euclidean`, or `dotproduct` |
| `cloud` | string | `"aws"` | Cloud provider for serverless indexes |
| `region` | string | `"us-east-1"` | Cloud region for serverless indexes |
| `vector_enabled` | bool | `true` | Enable vector similarity search |
| `batch_size` | int | `100` | Number of vectors per upsert batch |

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Pinecone online store.

|                                                           | Pinecone |
|:----------------------------------------------------------|:---------|
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
| support for concurrent writing to the same key            | yes      |
| support for ttl (time to live) at retrieval               | no       |
| support for deleting expired data                         | no       |
| collocated by feature view                                | no       |
| collocated by feature service                             | no       |
| collocated by entity key                                  | no       |
| vector similarity search                                  | yes      |
