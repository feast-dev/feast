# Milvus online store

## Description

The [Milvus](https://milvus.io/) online store provides support for materializing feature values into Milvus.

* The data model used to store feature values in Milvus is described in more detail [here](../../specs/online\_store\_format.md).

## Getting started
In order to use this online store, you'll need to install the Milvus extra (along with the dependency needed for the offline store of choice). E.g.

`pip install 'feast[milvus]'`

You can get started by using any of the other templates (e.g. `feast init -t gcp` or `feast init -t snowflake` or `feast init -t aws`), and then swapping in Redis as the online store as seen below in the examples.

## Examples

Connecting to a local MilvusDB instance:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: milvus
  path: "data/online_store.db"
  connection_string: "localhost:6379"
  embedding_dim: 128
  index_type: "FLAT"
  metric_type: "COSINE"
  username: "username"
  password: "password"
```
{% endcode %}

The full set of configuration options is available in [MilvusOnlineStoreConfig](https://github.com/ExpediaGroup/feast/blob/master/sdk/python/feast/expediagroup/vectordb/milvus_online_store.py#L40).

## Milvus Online Store Format

### Overview

[Milvus](https://milvus.io/docs/overview.md) is a vector database that is designed to handle queries over input vectors. It offers different index types and similarity metrics that enable fast searches on big vector datasets.

Milvus organizes data in _collections_. A collection is a table-like structure and supports all common datatypes for its columns (referred to as _fields_). There are two vector types supported: float and binary. An _index_ can be created for a vector field in order to perform vector similarity searches.

### Managing Milvus Resources Through FeatureView

A collection will reflect the schema of a FeatureView. The name of the FeatureView will also be the name of the collection that is created in Milvus. The online store implementation translates the schema of the _feature view_ into the equivalent schema of a collection (see example below). Therefore, the schema is **not optional** for feature views. Every feature view has to have one field marked as primary key.

Indexes are defined through _field_ level tags. The following tags are expected:

* index_type: defines the index for the collection. Refer to the [Milvus documentation](https://milvus.io/docs/index.md) on what the differences between index types are. The format is expected as string. You can refer to this overview [here](https://milvus.io/docs/build_index.md#Prepare-index-parameter).
* index_params: index types have different parameters that they expect. Follow the [documentation](https://milvus.io/docs/index.md) on which parameters are needed. Parameters are expected to be put in a dict that then is passed as a string.
    Example:
```python
    "index_params": """{
        "M": 64,
        "efConstruction": 512
    }""",
```
* metric_type: type of metrics used to measure the similarity of vectors. Check [here](https://milvus.io/docs/metric.md to learn which types exist and refer to [this page](https://milvus.io/docs/build_index.md#Prepare-index-parameter) to know which value to set. The value is expected to be a string. 

An example feature view:
```python
        FeatureView(
            name="books",
            schema= [
                    Field(
                    name="book_id",
                    dtype=Int64,
                ),
                Field(
                    name="book_embedding",
                    dtype=Array(Float32),
                    tags={
                        "description": "book embedding of the content",
                        "dimensions": "2200",
                        "index_type": "IVFLAT",
                        "index_params": {
                            "nlist": 1024,
                        },
                        "metric_type": "L2",
                    }
                ),
            ],
            source=SOURCE,
        )
```

This will create a collection named _books_ with two fields: _book_id_ and _book_embedding_. _book_id_ will be the primary key.

The full set of configuration options is available in [MilvusOnlineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.milvus.MilvusOnlineStoreConfig).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Milvus online store.

|                                                           | Milvus |
|:----------------------------------------------------------|:-------|
| write feature values to the online store                  | yes    |
| read feature values from the online store                 | yes    |
| update infrastructure (e.g. tables) in the online store   | yes    |
| teardown infrastructure (e.g. tables) in the online store | yes    |
| generate a plan of infrastructure changes                 | no     |
| support for on-demand transforms                          | yes    |
| readable by Python SDK                                    | yes    |
| readable by Java                                          | no     |
| readable by Go                                            | no     |
| support for entityless feature views                      | yes    |
| support for concurrent writing to the same key            | yes    |
| support for ttl (time to live) at retrieval               | yes    |
| support for deleting expired data                         | yes    |
| collocated by feature view                                | no     |
| collocated by feature service                             | no     |
| collocated by entity key                                  | no     |
| vector similarity search                                  | yes    |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
