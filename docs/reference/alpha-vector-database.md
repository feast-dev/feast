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
| ScyllaDB        | [x]       | [x]      | [x]         | [x]         |

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

**Note**: Milvus, SQLite, and ScyllaDB implement the v2 `retrieve_online_documents_v2` method in the SDK. This will be the longer-term solution so that Data Scientists can easily enable vector similarity search by just flipping a flag.

## Feature server search endpoints

| Endpoint | Use when |
|----------|----------|
| `POST /search` | You have an embedding vector (or use `api_version: 2` with `query_string`) and want Feast's native online-features response format. |
| `GET /v1/vector_stores` | You want to discover available vector stores and their `vs_{hash}` IDs (OpenAI-compatible). |
| `GET /v1/vector_stores/{id}` | You want metadata for a specific vector store (OpenAI-compatible). |
| `POST /v1/vector_stores/{id}/search` | You want plain-text queries with server-side embedding and an OpenAI-compatible response. |

`POST /retrieve-online-documents` is deprecated; use `POST /search` instead.

## [Alpha] OpenAI-Compatible Vector Store API

{% hint style="warning" %}
**Alpha feature.** This API surface is functional and tested, but may change in future releases. Feedback and contributions are welcome.
{% endhint %}

Feast exposes a set of [OpenAI-compatible vector store endpoints](https://platform.openai.com/docs/api-reference/vector-stores) that let clients discover, inspect, and search vector stores using plain text queries with server-side embedding. This enables integration with AI agents, LLM tool-calling frameworks, and any OpenAI-compatible client without requiring the caller to produce raw embedding vectors.

### Vector store IDs

Each feature view with at least one `vector_index=True` field is automatically assigned a deterministic identifier of the form `vs_{hash}`, where `{hash}` is the first 24 characters of `SHA-256(project + ":" + feature_view_name)`. These IDs are stable across server restarts and registry refreshes.

For example, a feature view named `product_catalog` in project `my_project` always maps to the same `vs_...` identifier. The listing endpoints return these IDs so clients can discover stores at runtime.

### Endpoints

| Method | Path | Permission | Description |
|--------|------|------------|-------------|
| `GET` | `/v1/vector_stores` | `DESCRIBE` | List all vector stores the caller has access to |
| `GET` | `/v1/vector_stores/{vector_store_id}` | `DESCRIBE` | Get metadata for a single vector store |
| `POST` | `/v1/vector_stores/{vector_store_id}/search` | `READ_ONLINE` | Search a vector store with a plain text query |

All endpoints enforce RBAC when authentication is configured. The listing endpoint filters out stores the caller cannot `DESCRIBE`.

### Requirements

1. **Embedding model** — an `embedding_model` section in `feature_store.yaml`. Feast uses [Sentence Transformers](https://www.sbert.net/) by default for local embedding — no external API key required (`pip install sentence-transformers`):

   ```yaml
   embedding_model:
     provider: sentence_transformers   # default; can be omitted
     model: all-MiniLM-L6-v2
   ```

2. **Vector-indexed feature view** — at least one feature view with `vector_index=True` on a vector field, materialized to an online store that supports vector search.

3. **Numeric filtering (optional)** — for metadata filters that use numeric or boolean comparisons, set `enable_openai_compatible_store: true` on your online store config and run `feast apply` to add the required `value_num` column.

### Custom embedding providers

The built-in Sentence Transformers provider works for most use cases. To use a different embedding backend (OpenAI, Cohere, a custom model, etc.), implement the `EmbeddingProvider` protocol and pass an instance to `FeatureStore`:

```python
from feast.embedder import EmbeddingProvider

class MyEmbeddingProvider:
    def embed(self, texts: list[str]) -> list[list[float]]:
        # Call your embedding API here
        return my_model.encode(texts)

    async def aembed(self, texts: list[str]) -> list[list[float]]:
        return await my_model.aencode(texts)

store = FeatureStore(
    repo_path=".",
    embedding_provider=MyEmbeddingProvider(),
)
```

### Numeric storage (`enable_openai_compatible_store`)

By default, feature values are stored as text in the online store. This means string-ordered comparisons apply (e.g., `'9' > '100'` is `true`). When `enable_openai_compatible_store: true` is set on the online store config, Feast adds a `value_num` column that stores `int`, `float`, `double`, and `bool` values natively so that numeric filters produce correct results.

```yaml
online_store:
  type: postgres  # or sqlite
  # ... connection settings ...
  enable_openai_compatible_store: true
```

After changing this setting, run `feast apply` to update the database schema.

### List vector stores

```bash
curl http://localhost:6566/v1/vector_stores
```

```json
{
  "object": "list",
  "data": [
    {
      "id": "vs_a1b2c3d4e5f6a1b2c3d4e5f6",
      "object": "vector_store",
      "name": "product_catalog",
      "status": "completed",
      "created_at": 1717200000
    }
  ]
}
```

### Get a single vector store

```bash
curl http://localhost:6566/v1/vector_stores/vs_a1b2c3d4e5f6a1b2c3d4e5f6
```

Returns the same object shape as a single entry in the list response. Returns `404` if the ID does not match any vector-indexed feature view.

### Search

Start the feature server with `feast serve`, then send a search request:

```bash
curl -X POST http://localhost:6566/v1/vector_stores/vs_a1b2c3d4e5f6a1b2c3d4e5f6/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "wireless noise-cancelling headphones",
    "max_num_results": 5
  }'
```

#### Request fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `query` | `string` or `list[string]` | (required) | Plain text search query. Lists are joined with spaces before embedding. |
| `max_num_results` | `int` | `10` | Maximum number of results to return. |
| `filters` | `object` | `null` | OpenAI-style filters (see below). |
| `ranking_options` | `object` | `null` | Accepted for forward compatibility, but currently ignored. Setting `score_threshold` or `ranker` inside it will return a 422 error. |
| `rewrite_query` | `bool` | `null` | `false` (the default/no-op) is accepted. `true` is not yet supported and will return a 422 error. |
| `metadata` | `object` | `null` | Optional. `metadata.features_to_retrieve` selects specific features. |

### Filters

The endpoint supports OpenAI-style filters for narrowing results beyond vector similarity.

**Comparison operators:** `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `nin`

```json
{"type": "eq", "key": "category", "value": "Electronics"}
```

**Compound operators:** `and`, `or` (nest to arbitrary depth)

```json
{
  "type": "and",
  "filters": [
    {"type": "eq", "key": "category", "value": "Electronics"},
    {"type": "gte", "key": "rating", "value": 4.5}
  ]
}
```

For Postgres and SQLite backends, all filtering (including string equality) requires `enable_openai_compatible_store: true` in the online store config. After enabling, run `feast apply` to update the database schema.

ScyllaDB supports vector retrieval via `retrieve_online_documents_v2`, but OpenAI-style metadata filtering is not implemented yet. Passing `filters` raises `NotImplementedError`.

### Response format

Responses follow the OpenAI `vector_store.search_results.page` schema:

```json
{
  "object": "vector_store.search_results.page",
  "search_query": ["wireless noise-cancelling headphones"],
  "data": [
    {
      "file_id": "vs_a1b2c3d4e5f6a1b2c3d4e5f6_42",
      "filename": "vs_a1b2c3d4e5f6a1b2c3d4e5f6",
      "score": 0.92,
      "attributes": {"name": "...", "category": "..."},
      "content": [
        {"type": "text", "text": "..."}
      ]
    }
  ],
  "has_more": false,
  "next_page": null
}
```

The `file_id` and `filename` fields use the `vs_{hash}` identifier, not raw feature view names.

The `score` field is a higher-is-better relevance score derived from the raw vector distance using a metric-dependent conversion:

| Distance metric | Conversion | Range |
|----------------|------------|-------|
| L2 (default) | `1 / (1 + distance)` | (0, 1] |
| Cosine | `1 - distance` | [0, 1] |
| Inner product / dot | `-distance` | varies |

The metric is determined by `vector_search_metric` on the feature view's vector field, not by an API parameter. When `features_to_retrieve` is omitted, all non-vector features are returned by default (vector embedding columns are excluded).

Pagination is not yet implemented; `has_more` is always `false`.

### SDK usage

The OpenAI-compatible search is also available directly via the Python SDK:

```python
import asyncio
from feast import FeatureStore

store = FeatureStore(repo_path=".")

result = asyncio.run(store.openai_search(
    vector_store_id="product_catalog",
    query="wireless noise-cancelling headphones",
    max_num_results=5,
    filters={"type": "eq", "key": "category", "value": "Electronics"},
))

for item in result["data"]:
    print(f"{item['score']:.3f}  {item['attributes']}")
```

### Supported online stores

The OpenAI-compatible filtering has been implemented for the following online stores:

| Online Store | Vector Search | Metadata Filtering | Notes |
|-------------|--------------|-------------------|-------|
| Milvus | Yes | Yes | Boolean expressions |
| Elasticsearch | Yes | Yes | Query DSL clauses |
| Postgres (pgvector) | Yes | Yes | Requires `enable_openai_compatible_store: true` |
| SQLite (sqlite-vec) | Yes | Yes | Requires `enable_openai_compatible_store: true` |
| MongoDB | Yes | Yes | Aggregation pipeline |
| ScyllaDB | Yes | No | Vector search only; metadata filters are not supported yet |

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

We offer [Milvus](https://milvus.io/), [PGVector](https://github.com/pgvector/pgvector), [SQLite](https://github.com/asg017/sqlite-vec), [Elasticsearch](https://www.elastic.co) and [Qdrant](https://qdrant.tech/) as Online Store options for Vector Databases.

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
