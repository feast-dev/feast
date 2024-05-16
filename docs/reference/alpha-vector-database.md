# [Alpha] Vector Database
**Warning**: This is an _experimental_ feature. To our knowledge, this is stable, but there are still rough edges in the experience. Contributions are welcome!

## Overview
Vector database allows user to store and retrieve embeddings. Feast provides general APIs to store and retrieve embeddings.

## Integration
Below are supported vector databases and implemented features:

| Vector Database | Retrieval | Indexing |
|-----------------|-----------|----------|
| Pgvector        | [x]       | [ ]      |
| Elasticsearch   | [x]       | [x]      |
| Milvus          | [ ]       | [ ]      |
| Faiss           | [ ]       | [ ]      |


## Example

See [https://github.com/feast-dev/feast-workshop/blob/rag/module_4_rag](https://github.com/feast-dev/feast-workshop/blob/rag/module_4_rag) for an example on how to use vector database.

### **Prepare offline embedding dataset**
Run the following commands to prepare the embedding dataset:
```shell
python pull_states.py
python batch_score_documents.py
```
The output will be stored in `data/city_wikipedia_summaries.csv.`

### **Initialize Feast feature store and materialize the data to the online store**
Use the feature_tore.yaml file to initialize the feature store. This will use the data as offline store, and Pgvector as online store.

```yaml
project: feast_demo_local
provider: local
registry:
  registry_type: sql
  path: postgresql://@localhost:5432/feast
online_store:
  type: postgres
  pgvector_enabled: true
  vector_len: 384
  host: 127.0.0.1
  port: 5432
  database: feast
  user: ""
  password: ""


offline_store:
  type: file
entity_key_serialization_version: 2
```
Run the following command in terminal to apply the feature store configuration:

```shell
feast apply
```

Note that when you run `feast apply` you are going to apply the following Feature View that we will use for retrieval later:  

```python
city_embeddings_feature_view = FeatureView(
    name="city_embeddings",
    entities=[item],
    schema=[
        Field(name="Embeddings", dtype=Array(Float32)),
    ],
    source=source,
    ttl=timedelta(hours=2),
)
```

Then run the following command in the terminal to materialize the data to the online store:  

```shell  
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")  
feast materialize-incremental $CURRENT_TIME  
```

### **Prepare a query embedding**
```python
from batch_score_documents import run_model, TOKENIZER, MODEL
from transformers import AutoTokenizer, AutoModel

question = "the most populous city in the U.S. state of Texas?"

tokenizer = AutoTokenizer.from_pretrained(TOKENIZER)
model = AutoModel.from_pretrained(MODEL)
query_embedding = run_model(question, tokenizer, model)
query = query_embedding.detach().cpu().numpy().tolist()[0]
```

### **Retrieve the top 5 similar documents**
First create a feature store instance, and use the `retrieve_online_documents` API to retrieve the top 5 similar documents to the specified query.

```python
from feast import FeatureStore
store = FeatureStore(repo_path=".")
features = store.retrieve_online_documents(
    feature="city_embeddings:Embeddings",
    query=query,
    top_k=5
).to_dict()

def print_online_features(features):
    for key, value in sorted(features.items()):
        print(key, " : ", value)

print_online_features(features)
```

### Configuration
We offer two Online Store options for Vector Databases. PGVector and SQLite.

#### Installation with SQLite
If you are using `pyenv` to manage your Python versions, you can install the SQLite extension with the following command:
```bash
PYTHON_CONFIGURE_OPTS="--enable-loadable-sqlite-extensions" \
    LDFLAGS="-L/opt/homebrew/opt/sqlite/lib" \
    CPPFLAGS="-I/opt/homebrew/opt/sqlite/include" \
    pyenv install
```