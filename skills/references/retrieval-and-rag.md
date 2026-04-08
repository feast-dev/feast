# Retrieval & RAG Reference

## Table of Contents
- [FeatureStore Construction](#featurestore-construction)
- [Online Feature Retrieval](#online-feature-retrieval)
- [Historical Feature Retrieval](#historical-feature-retrieval)
- [Push and Write Operations](#push-and-write-operations)
- [Vector Similarity Search](#vector-similarity-search)
- [RAG Retriever](#rag-retriever)
- [FeatureStore API Quick Reference](#featurestore-api-quick-reference)

## FeatureStore Construction

```python
from feast import FeatureStore

# From repo path (looks for feature_store.yaml)
store = FeatureStore(repo_path="path/to/feature_repo")

# From config object
from feast.repo_config import RepoConfig
store = FeatureStore(config=RepoConfig(
    project="my_project",
    registry="data/registry.db",
    provider="local",
    online_store={"type": "sqlite", "path": "data/online.db"},
))

# From explicit YAML path
from pathlib import Path
store = FeatureStore(fs_yaml_file=Path("custom/feature_store.yaml"))
```

## Online Feature Retrieval

Low-latency lookup from the online store. Features must be materialized first.

### By feature references
```python
result = store.get_online_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
    entity_rows=[
        {"driver_id": 1001},
        {"driver_id": 1002},
    ],
)

feature_dict = result.to_dict()
feature_df = result.to_df()
```

### By FeatureService
```python
result = store.get_online_features(
    features=driver_ranking_service,
    entity_rows=[{"driver_id": 1001}],
)
```

### Feature reference format
`"feature_view_name:feature_name"` — e.g., `"driver_hourly_stats:conv_rate"`

## Historical Feature Retrieval

Point-in-time correct joins for training data. Prevents data leakage by joining features based on event timestamps.

### Basic usage
```python
import pandas as pd
from datetime import datetime

entity_df = pd.DataFrame({
    "driver_id": [1001, 1002, 1003],
    "event_timestamp": [
        datetime(2023, 6, 1),
        datetime(2023, 6, 15),
        datetime(2023, 7, 1),
    ],
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
    ],
).to_df()
```

### With FeatureService
```python
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=driver_ranking_service,
).to_df()
```

### Output
Returns a `RetrievalJob` with methods:
- `.to_df()` — pandas DataFrame
- `.to_arrow()` — PyArrow Table
- `.to_sql_string()` — SQL query (for SQL-based offline stores)

## Push and Write Operations

### Push (for PushSource/StreamFeatureView)
```python
store.push(
    push_source_name="driver_push",
    df=pd.DataFrame({
        "driver_id": [1001],
        "trips_today": [15],
        "event_timestamp": [datetime.utcnow()],
    }),
)
```

### Write to online store
```python
store.write_to_online_store(
    feature_view_name="driver_hourly_stats",
    df=features_df,
)
```

### Write to offline store
```python
store.write_to_offline_store(
    feature_view_name="driver_hourly_stats",
    df=features_df,
)
```

## Vector Similarity Search

Requires a FeatureView with a `vector_index=True` field and an online store that supports vector search (e.g., Milvus, Qdrant, PostgreSQL with pgvector).

### Define vector feature view
```python
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Array, Float32, String

passage_entity = Entity(name="passage_id", join_keys=["passage_id"])

wiki_passages = FeatureView(
    name="wiki_passages",
    entities=[passage_entity],
    schema=[
        Field(name="passage_text", dtype=String),
        Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=384,
            vector_search_metric="COSINE",
        ),
    ],
    source=passages_source,
    online=True,
)
```

### Retrieve similar documents
```python
# v1 API
results = store.retrieve_online_documents(
    feature="wiki_passages:embedding",
    query=query_embedding_vector,
    top_k=5,
)

# v2 API (supports text, vector, and image queries)
results = store.retrieve_online_documents_v2(
    feature_view_name="wiki_passages",
    query_string="What is machine learning?",
    top_k=5,
)
```

### Search metrics
- `"COSINE"` — Cosine similarity (default, best for normalized embeddings)
- `"L2"` — Euclidean distance
- `"INNER_PRODUCT"` — Dot product

## RAG Retriever

`FeastRAGRetriever` integrates Feast with HuggingFace for retrieval-augmented generation.

### Prerequisites
- A FeatureView with a `vector_index=True` embedding field
- Features materialized to the online store
- HuggingFace `transformers` installed

### Setup
```python
from feast.rag_retriever import FeastRAGRetriever
from transformers import AutoTokenizer, AutoModel, AutoModelForSeq2SeqLM

question_tokenizer = AutoTokenizer.from_pretrained("facebook/dpr-question_encoder-single-nq-base")
question_encoder = AutoModel.from_pretrained("facebook/dpr-question_encoder-single-nq-base")
generator_tokenizer = AutoTokenizer.from_pretrained("facebook/bart-large")
generator_model = AutoModelForSeq2SeqLM.from_pretrained("facebook/bart-large")

retriever = FeastRAGRetriever(
    question_encoder_tokenizer=question_tokenizer,
    question_encoder=question_encoder,
    generator_tokenizer=generator_tokenizer,
    generator_model=generator_model,
    feast_repo_path="path/to/feature_repo",
    feature_view="wiki_passages",
    features=["passage_text", "embedding"],
    search_type="vector",   # "text", "vector", or "hybrid"
    id_field="passage_id",
    text_field="passage_text",
)
```

### Retrieve documents
```python
doc_embeddings, doc_ids, doc_dicts = retriever.retrieve(
    question_input_ids=question_tokenizer("What is ML?", return_tensors="pt")["input_ids"],
    n_docs=5,
)
```

### End-to-end answer generation
```python
answer = retriever.generate_answer(
    query="What is machine learning?",
    top_k=5,
    max_new_tokens=200,
)
print(answer)
```

### FeastVectorStore (lower-level)

```python
from feast.vector_store import FeastVectorStore

vector_store = FeastVectorStore(feast_repo_path="path/to/feature_repo")

results = vector_store.query(
    query_vector=embedding_list,
    top_k=10,
)
```

Supports `query_vector`, `query_string`, and `query_image_bytes` for different search modalities.

## FeatureStore API Quick Reference

| Method | Purpose |
|--------|---------|
| `apply(objects)` | Register entities, FVs, ODFVs, SFVs, services, sources |
| `plan(desired_registry)` | Preview apply changes |
| `get_online_features(features, entity_rows)` | Low-latency online lookup |
| `get_historical_features(entity_df, features)` | Point-in-time training data |
| `materialize(start_date, end_date)` | Load offline → online store |
| `materialize_incremental(end_date)` | Incremental materialization |
| `push(push_source_name, df)` | Push data to online/offline store |
| `write_to_online_store(fv_name, df)` | Direct write to online store |
| `write_to_offline_store(fv_name, df)` | Direct write to offline store |
| `retrieve_online_documents(feature, query, top_k)` | Vector similarity search |
| `retrieve_online_documents_v2(...)` | Vector search v2 (text/vector/image) |
| `list_entities()` | List all entities |
| `list_feature_views()` | List all feature views |
| `list_on_demand_feature_views()` | List on-demand feature views |
| `list_stream_feature_views()` | List stream feature views |
| `list_feature_services()` | List feature services |
| `list_data_sources()` | List data sources |
| `get_entity(name)` | Get entity by name |
| `get_feature_view(name)` | Get feature view by name |
| `get_feature_service(name)` | Get feature service by name |
| `delete_feature_view(name)` | Delete a feature view |
| `delete_feature_service(name)` | Delete a feature service |
| `create_saved_dataset(...)` | Save a dataset for reuse |
| `refresh_registry()` | Force refresh registry cache |
| `teardown()` | Remove all infrastructure resources |
| `serve(port)` | Start feature server |
| `serve_ui(port)` | Start Feast UI |
| `serve_registry(port)` | Start registry server |
| `serve_offline(port)` | Start offline server |
