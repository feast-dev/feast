# PostgreSQL online store

## Description

The PostgreSQL online store provides support for materializing feature values into a PostgreSQL database for serving online features.

* Only the latest feature values are persisted

* sslmode, sslkey_path, sslcert_path, and sslrootcert_path are optional

## Getting started
In order to use this online store, you'll need to run `pip install 'feast[postgres]'`. You can get started by then running `feast init -t postgres`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: postgres
    host: DB_HOST
    port: DB_PORT
    database: DB_NAME
    db_schema: DB_SCHEMA
    user: DB_USERNAME
    password: DB_PASSWORD
    sslmode: verify-ca
    sslkey_path: /path/to/client-key.pem
    sslcert_path: /path/to/client-cert.pem
    sslrootcert_path: /path/to/server-ca.pem
    vector_enabled: false
    vector_len: 512
```
{% endcode %}

The full set of configuration options is available in [PostgreSQLOnlineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.online_stores.postgres_online_store.PostgreSQLOnlineStoreConfig).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Postgres online store.

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

## PGVector
The Postgres online store supports the use of [PGVector](https://github.com/pgvector/pgvector) for storing feature values.
To enable PGVector, set `vector_enabled: true` in the online store configuration. 

The `vector_len` parameter can be used to specify the length of the vector. The default value is 512.

Please make sure to follow the instructions in the repository, which, as the time of this writing, requires you to 
run `CREATE EXTENSION vector;` in the database.


Then you can use `retrieve_online_documents` to retrieve the top k closest vectors to a query vector. 
For the Retrieval Augmented  Generation (RAG) use-case, you have to embed the query prior to passing the query vector.

{% code title="python" %}
```python
from feast import FeatureStore
from feast.infra.online_stores.postgres_online_store import retrieve_online_documents

feature_store = FeatureStore(repo_path=".")

query_vector = [0.1, 0.2, 0.3, 0.4, 0.5]
top_k = 5

feature_values = retrieve_online_documents(
    feature_store=feature_store,
    feature_view_name="document_fv:embedding_float",
    query_vector=query_vector,
    top_k=top_k,
)
```
{% endcode %}
