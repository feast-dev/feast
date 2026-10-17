# Lance online store

## Description

The Lance online store keeps the latest feature values in [Lance](https://lancedb.github.io/lance/)
datasets, one dataset per feature view, on local disk or object storage. Point
lookups by entity key, vector search, metadata filters and full-text search all
run against the same dataset, with no server to operate.

Each write is an upsert keyed on the entity key. A stored row is replaced only by
a row whose event timestamp is not older, so materialization jobs that land out
of order cannot roll a value back.

## Getting started

In order to use this online store, you'll need to run `pip install 'feast[lance]'`.

## Example

{% code title="feature_store.yaml" %}

```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: lance
    path: data/online_store      # or s3://bucket/online_store
    vector_enabled: true         # enables retrieve_online_documents_v2 with an embedding
    text_search_enabled: false   # builds full-text indices over String features
    storage_options:             # optional, passed through to Lance
        aws_region: us-east-1
```

{% endcode %}

The full set of configuration options is available in [LanceOnlineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.online_stores.lance_online_store.lance_online_store.LanceOnlineStoreConfig).

## Functionality Matrix

|                                                           | Lance |
| :-------------------------------------------------------- | :---- |
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

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).

## Retrieving online documents

Mark one `Array(Float32)` field with `vector_index=True` and a `vector_length`,
and set `vector_enabled: true`. The vector is stored as a native Lance vector
column, so nearest-neighbour search runs inside Lance:

```python
from feast import FeatureStore
from feast.filter_models import ComparisonFilter

store = FeatureStore(repo_path=".")
documents = store.retrieve_online_documents_v2(
    features=["documents:title", "documents:embedding"],
    query=query_embedding,
    top_k=5,
    filters=ComparisonFilter(type="eq", key="language", value="en"),
).to_dict()
```

Supported distance metrics are `cosine`, `l2`, `dot` and `hamming`, taken from the
field's `vector_search_metric`.

Metadata filters are applied before ranking, so a filtered search still returns
up to `top_k` matching rows. They can reference scalar `Int32`, `Int64`,
`Float32`, `Float64`, `String` and `Bool` features.

With `text_search_enabled: true`, pass `query_string` instead of `query` to rank
rows by full-text relevance over the feature view's `String` features. The score
is returned as `text_rank`. Combining an embedding and a query string in one call
(hybrid search) is not supported yet.

## Schema changes

`feast apply` evolves an existing dataset in place: new features are added as
empty columns and removed features are dropped, so the values of unchanged
features are kept. If the type of a feature changes, the dataset is recreated
empty and has to be materialized again.

## Maintenance

Every write commits a new Lance version. For a frequently written feature view,
compact the dataset and remove old versions periodically with Lance's own
`dataset.optimize.compact_files()` and `dataset.cleanup_old_versions()`.
