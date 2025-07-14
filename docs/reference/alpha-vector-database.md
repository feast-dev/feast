# [Alpha] Vector Database
**Warning**: This is an _experimental_ feature. To our knowledge, this is stable, but there are still rough edges in the experience. Contributions are welcome!

## Overview
Vector database allows user to store and retrieve embeddings. Feast provides general APIs to store and retrieve embeddings.

## Integration
Below are supported vector databases and implemented features:

| Vector Database | Retrieval | Indexing | V2 Support* | Online Read |
|-----------------|-----------|----------|-------------|-------------|
| Pgvector        | [x]       | [ ]      | []          | []          |
| Elasticsearch   | [x]       | [x]      | []          | []          |
| Milvus          | [x]       | [x]      | [x]         | [x]         |
| Faiss           | [ ]       | [ ]      | []          | []          |
| SQLite          | [x]       | [ ]      | [x]         | [x]         |
| Qdrant          | [x]       | [x]      | []          | []          |
| Redis           | [x]       | [x]      | [x]         | [x]         |

*Note: V2 Support means the SDK supports retrieval of features along with vector embeddings from vector similarity search.

Note: SQLite is in limited access and only working on Python 3.10. It will be updated as [sqlite_vec](https://github.com/asg017/sqlite-vec/) progresses.

{% hint style="danger" %}
We will be deprecating the `retrieve_online_documents` method in the SDK in the future. 
We recommend using the `retrieve_online_documents_v2` method instead, which offers easier vector index configuration 
directly in the Feature View and the ability to retrieve standard features alongside your vector embeddings for richer context injection. 

Long term we will collapse the two methods into one, but for now, we recommend using the `retrieve_online_documents_v2` method.
Beyond that, we will then have `retrieve_online_documents` and `retrieve_online_documents_v2` simply point to `get_online_features` for 
backwards compatibility and the adopt industry standard naming conventions.
{% endhint %}

**Note**: Milvus and SQLite implement the v2 `retrieve_online_documents_v2` method in the SDK. This will be the longer-term solution so that Data Scientists can easily enable vector similarity search by just flipping a flag.

## Examples

- See the v0 [Rag Demo](https://github.com/feast-dev/feast-workshop/blob/rag/module_4_rag) for an example on how to use vector database using the `retrieve_online_documents` method (planning migration and deprecation (planning migration and deprecation).
- See the v1 [Milvus Quickstart](../../examples/rag/milvus-quickstart.ipynb) for a quickstart guide on how to use Feast with Milvus using the `retrieve_online_documents_v2` method.

### **Prepare offline embedding dataset**
Run the following commands to prepare the embedding dataset:
```shell
python pull_states.py
python batch_score_documents.py
```
The output will be stored in `data/city_wikipedia_summaries.csv.`

### **Initialize Feast feature store and materialize the data to the online store**
Use the feature_store.yaml file to initialize the feature store. This will use the data as offline store, and Milvus as online store.

```yaml
project: local_rag
provider: local
registry: data/registry.db
online_store:
  type: milvus
  path: data/online_store.db
  vector_enabled: true
  embedding_dim: 384
  index_type: "IVF_FLAT"


offline_store:
  type: file
entity_key_serialization_version: 3
# By default, no_auth for authentication and authorization, other possible values kubernetes and oidc. Refer the documentation for more details.
auth:
    type: no_auth
```
Run the following command in terminal to apply the feature store configuration:

```shell
feast apply
```

Note that when you run `feast apply` you are going to apply the following Feature View that we will use for retrieval later:  

```python
document_embeddings = FeatureView(
    name="embedded_documents",
    entities=[item, author],
    schema=[
        Field(
            name="vector",
            dtype=Array(Float32),
            # Look how easy it is to enable RAG!
            vector_index=True,
            vector_search_metric="COSINE",
        ),
        Field(name="item_id", dtype=Int64),
        Field(name="author_id", dtype=String),
        Field(name="created_timestamp", dtype=UnixTimestamp),
        Field(name="sentence_chunks", dtype=String),
        Field(name="event_timestamp", dtype=UnixTimestamp),
    ],
    source=rag_documents_source,
    ttl=timedelta(hours=24),
)
```

Let's use the SDK to write a data frame of embeddings to the online store:
```python
store.write_to_online_store(feature_view_name='city_embeddings', df=df)
```

### **Prepare a query embedding**
During inference (e.g., during when a user submits a chat message) we need to embed the input text. This can be thought of as a feature transformation of the input data. In this example, we'll do this with a small Sentence Transformer from Hugging Face.

```python
import torch
import torch.nn.functional as F
from feast import FeatureStore
from pymilvus import MilvusClient, DataType, FieldSchema
from transformers import AutoTokenizer, AutoModel
from example_repo import city_embeddings_feature_view, item

TOKENIZER = "sentence-transformers/all-MiniLM-L6-v2"
MODEL = "sentence-transformers/all-MiniLM-L6-v2"

def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[
        0
    ]  # First element of model_output contains all token embeddings
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    )
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
        input_mask_expanded.sum(1), min=1e-9
    )

def run_model(sentences, tokenizer, model):
    encoded_input = tokenizer(
        sentences, padding=True, truncation=True, return_tensors="pt"
    )
    # Compute token embeddings
    with torch.no_grad():
        model_output = model(**encoded_input)

    sentence_embeddings = mean_pooling(model_output, encoded_input["attention_mask"])
    sentence_embeddings = F.normalize(sentence_embeddings, p=2, dim=1)
    return sentence_embeddings

question = "Which city has the largest population in New York?"

tokenizer = AutoTokenizer.from_pretrained(TOKENIZER)
model = AutoModel.from_pretrained(MODEL)
query_embedding = run_model(question, tokenizer, model).detach().cpu().numpy().tolist()[0]
```

### **Retrieve the top K similar documents**
First create a feature store instance, and use the `retrieve_online_documents_v2` API to retrieve the top 5 similar documents to the specified query.

```python
context_data = store.retrieve_online_documents_v2(
    features=[
        "city_embeddings:vector",
        "city_embeddings:item_id",
        "city_embeddings:state",
        "city_embeddings:sentence_chunks",
        "city_embeddings:wiki_summary",
    ],
    query=query_embedding,
    top_k=3,
    distance_metric='COSINE',
).to_df()
```
### **Generate the Response** 
Let's assume we have a base prompt and a function that formats the retrieved documents called `format_documents` that we 
can then use to generate the response with OpenAI's chat completion API.
```python 
FULL_PROMPT = format_documents(rag_context_data, BASE_PROMPT)

from openai import OpenAI

client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
)
response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": FULL_PROMPT},
        {"role": "user", "content": question}
    ],
)

# And this will print the content. Look at the examples/rag/milvus-quickstart.ipynb for an end-to-end example.
print('\n'.join([c.message.content for c in response.choices]))
```

### Configuration and Installation

We offer [Milvus](https://milvus.io/), [PGVector](https://github.com/pgvector/pgvector), [SQLite](https://github.com/asg017/sqlite-vec), [Elasticsearch](https://www.elastic.co), [Qdrant](https://qdrant.tech/), and [Redis](https://redis.io/) as Online Store options for Vector Databases.

Milvus offers a convenient local implementation for vector similarity search. To use Milvus, you can install the Feast package with the Milvus extra.

#### Installation with Milvus

```bash
pip install feast[milvus]
```
#### Installation with Elasticsearch

```bash
pip install feast[elasticsearch]
```

#### Installation with Qdrant

```bash
pip install feast[qdrant]
```

#### Installation with Redis

```bash
pip install feast[redis]
```

Note: Redis vector search requires Redis with RediSearch module. You can use:
- Redis Stack (includes RediSearch)
- Redis Enterprise with RediSearch module
- Redis with RediSearch module installed

#### Installation with SQLite

If you are using `pyenv` to manage your Python versions, you can install the SQLite extension with the following command:
```bash
PYTHON_CONFIGURE_OPTS="--enable-loadable-sqlite-extensions" \
    LDFLAGS="-L/opt/homebrew/opt/sqlite/lib" \
    CPPFLAGS="-I/opt/homebrew/opt/sqlite/include" \
    pyenv install 3.10.14
```

And you can the Feast install package via:
```bash
pip install feast[sqlite_vec]
```
