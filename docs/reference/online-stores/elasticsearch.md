# Elasticsearch online store

## Description

The [Elasticsearch](https://www.elastic.co/) online store provides support for materializing feature values into an Elasticsearch database for serving online features.

Features:
* Creating indexes and documents through parameters in FeatureView
* Support for float and binary vector embeddings
* Full support for HNSW indexing for available metric types in Elasticsearch

## Getting started
In order to use this online store, you'll need to install the elasticsearch extra:
- `pip install 'feast[elasticsearch]'`

## Example

Connecting to an Elasticsearch instance:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: elasticsearch
  endpoint: ELASTICSEARCH_URL
  username: ELASTICSEARCH_USERNAME
  password: ELASTICSEARCH_PASSWORD
```
{% endcode %}

The full set of configuration options is available in [ElasticsearchOnlineStoreConfig](https://github.com/ExpediaGroup/feast/blob/master/sdk/python/feast/expediagroup/vectordb/elasticsearch_online_store.py#L40).

## Elasticsearch Online Store Format

### Overview

Elasticsearch is a document search engine with vector embedding indexing and knn support.

Elasticsearch organizes data into _indexes_. An index contains a series of object documents. A document can contain one vector field on the top-level of a document. A vector can be either an array of floats or binary (bytes). The vector is indexed for searching using a Hierarchical Navigable Small World (HNSW) graph.

### Managing Elasticsearch Resources Through Feature View

An index will reflect the schema of a FeatureView. The name of the FeatureView will also be the name of the index in Elasticsearch. The online store implementation translates the schema of the _feature view_ into the equivalent schema of a document.

Vector indexing is defined through the _field_ level tags. The following tags are expected:

* index_type: must be `hnsw` or `flat`. This tag is used to identify which field should be indexed as a vector embedding.
* index_params: optional parameters to override the default values (m=16, ef_construction=100)
  Example:
```python
    "index_params": """{
        "m": 64,
        "ef_construction": 512
    }""",
```
* metric_type: type of metrics used to measure the similarity of vectors. Check the [documentation](https://www.elastic.co/guide/en/elasticsearch/reference/8.8/dense-vector.html) to learn which types exist. The value is expected to be a lowercase string.


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
                        "dimensions": 2200,
                        "index_type": "hnsw",
                        "index_params": {
                            "m": 64,
                            "ef_construction": 512,
                        },
                        "metric_type": "l2_norm",
                    }
                ),
            ],
            source=SOURCE,
        )
```

This will create an index named _books_ with two fields: _book_id_ and _book_embedding_.

_book_embedding_ is the vector field of the collection. This field will have the following properties:
* float vectors with 2200 dimensions
* index type is HNSW with 16 member nodes (_m_) and 100 nearest neighbor candidates (_ef_construction_)
* metric type is Euclidian distance (_l2_norm_)

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Elasticsearch online store.

|                                                           | Elasticsearch |
| :-------------------------------------------------------- |:--------------|
| write feature values to the online store                  | yes           |
| read feature values from the online store                 | yes           |
| update infrastructure (e.g. tables) in the online store   | yes           |
| teardown infrastructure (e.g. tables) in the online store | yes           |
| generate a plan of infrastructure changes                 | no            |
| support for on-demand transforms                          | no            |
| readable by Python SDK                                    | yes           |
| readable by Java                                          | no            |
| readable by Go                                            | no            |
| support for entityless feature views                      | no            |
| support for concurrent writing to the same key            | yes           |
| support for ttl (time to live) at retrieval               | no            |
| support for deleting expired data                         | no            |
| collocated by feature view                                | yes           |
| collocated by feature service                             | no            |
| collocated by entity key                                  | no            |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
