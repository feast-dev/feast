# ElasticSearch online store

## Description

The ElasticSearch online store provides support for materializing tabular feature values, as well as embedding feature vectors, into an ElasticSearch index for serving online features. \
The embedding feature vectors are stored as dense vectors, and can be used for similarity search. More information on dense vectors can be found [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html).

## Getting started
In order to use this online store, you'll need to run `pip install 'feast[elasticsearch]'`. You can get started by then running `feast init -t elasticsearch`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: elasticsearch
    host: ES_HOST
    port: ES_PORT
    user: ES_USERNAME
    password: ES_PASSWORD
    write_batch_size: 1000
```
{% endcode %}

The full set of configuration options is available in [ElasticsearchOnlineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.online_stores.elasticsearch_online_store.ElasticsearchOnlineStoreConfig).

## Functionality Matrix


|                                                           | Postgres |
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

The ElasticSearch online store supports retrieving document vectors for a given list of entity keys. The document vectors are returned as a dictionary where the key is the entity key and the value is the document vector. The document vector is a dense vector of floats.

{% code title="python" %}
```python
from feast import FeatureStore

feature_store = FeatureStore(repo_path="feature_store.yaml")

query_vector = [1.0, 2.0, 3.0, 4.0, 5.0]
top_k = 5

# Retrieve the top k closest features to the query vector

feature_values = feature_store.retrieve_online_documents_v2(
    features=["my_feature"],
    query=query_vector,
    top_k=top_k,
)
```
{% endcode %}

## Indexing
Currently, the indexing mapping in the ElasticSearch online store is configured as:

{% code title="indexing_mapping" %}
```json
{
    "dynamic_templates": [
        {
            "feature_objects": {
                "match_mapping_type": "object",
                "match": "*",
                "mapping": {
                    "type": "object",
                    "properties": {
                        "feature_value": {"type": "binary"},
                        "value_text": {"type": "text"},
                        "vector_value": {
                            "type": "dense_vector",
                            "dims": vector_field_length,
                            "index": True,
                            "similarity": config.online_store.similarity,
                        },
                    },
                },
            }
        }
    ],
    "properties": {
        "entity_key": {"type": "keyword"},
        "timestamp": {"type": "date"},
        "created_ts": {"type": "date"},
    },
}
```
{% endcode %}
And the online_read API mapping is configured as:

{% code title="online_read_mapping" %}
```json
"query": {
    "bool": {
        "must": [
            {"terms": {"entity_key": entity_keys}},
            {"terms": {"feature_name": requested_features}},
        ]
    }
},
```
{% endcode %}

And the similarity search API mapping is configured as:

{% code title="similarity_search_mapping" %}
```json
{
    "field": "vector_value",
    "query_vector": embedding_vector,
    "k": top_k,
}
```
{% endcode %}

These APIs are subject to change in future versions of Feast to improve performance and usability.