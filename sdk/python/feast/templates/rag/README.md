# City Information Q&A — RAG Demo with Feast

A complete Retrieval-Augmented Generation (RAG) demo using Feast for feature management and Milvus for vector search.


## What You'll See in the Feast UI

After running `feast apply` and `feast ui`, you'll see:

| Resource | Name | Description |
|----------|------|-------------|
| **Data Source** | `city_summaries_source` | Wikipedia summaries with embeddings (parquet) |
| **Data Source** | `city_summaries_push_source` | Push source for real-time doc/embedding updates |
| **Data Source** | `rag_request_source` | Request-time inputs (query_text, user_id) |
| **Entity** | `city_id` | Unique ID for each city document |
| **Feature View** | `city_summary_embeddings` | Embeddings + chunks for vector search (batch) |
| **Feature View** | `city_metadata` | Scalar metadata (state, wiki_summary) |
| **Feature View** | `city_summary_embeddings_realtime` | Same embeddings with real-time ingestion |
| **On-Demand Feature View** | `rag_request_context` | Request-time features (query_text_length, has_user_context) |
| **Feature Service** | `city_qa_v1` | Bundles embeddings + metadata (vector search + lookups) |
| **Feature Service** | `city_qa_v2` | Bundles realtime embeddings + metadata + request context |
| **Features** | `vector`, `sentence_chunks`, `state`, `wiki_summary`, `query_text_length`, `has_user_context` | Across views |

**Lineage:** Data Sources (3) → Feature Views (3 batch/realtime + 1 on-demand) → Feature Services (2).

## Project Structure

```
rag/
├── feature_repo/
│   ├── data/
│   │   └── city_wikipedia_summaries_with_embeddings.parquet  # Sample data (US cities)
│   ├── example_repo.py       # Entity, Feature Views, Feature Service definitions
│   ├── feature_store.yaml    # Feast config (Milvus online store, file offline store)
│   └── test_workflow.py      # End-to-end demo: apply → materialize → search
└── README.md
```

**Key files:**

- `example_repo.py` — Defines:
  - `city` entity (join key: `city_id`, value_type INT64)
  - Data sources: `city_summaries_source` (FileSource), `city_summaries_push_source` (PushSource), `rag_request_source` (RequestSource)
  - Feature views: `city_summary_embeddings` (vector search), `city_metadata` (scalar lookups), `city_summary_embeddings_realtime` (real-time ingestion)
  - On-demand feature view: `rag_request_context` (query_text_length, has_user_context from request)
  - Feature services: `city_qa_v1` (embeddings + metadata), `city_qa_v2` (realtime embeddings + metadata + request context)
- `feature_store.yaml` — Configures Milvus for vector search (384-dim embeddings, cosine similarity)
- `test_workflow.py` — End-to-end demo: apply → materialize → vector search → metadata lookup

## Quick Start

### 1. Initialize the template

```bash
feast init -t rag my_city_qa
cd my_city_qa/feature_repo
```

### 2. Install dependencies

```bash
pip install feast torch transformers
```

### 3. Apply feature definitions

```bash
feast apply
```

### 4. Explore in the Feast UI

```bash
feast ui
```

Open http://localhost:8888 and explore data sources, entities, feature views, feature services, and lineage.

### 5. Run the demo workflow

```bash
python test_workflow.py
```

This script applies feature definitions, loads sample city data, writes to the online store, runs a semantic search (embed question → retrieve top-k cities), and demonstrates metadata lookup for the top result.

## Key Commands

| Command | Description |
|---------|-------------|
| `feast apply` | Register entities, feature views, and feature services |
| `feast feature-views list` | List registered feature views |
| `feast entities list` | List registered entities |
| `feast feature-services list` | List registered feature services |
| `feast ui` | Start the Feast UI at http://localhost:8888 |


## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        City Q&A Pipeline                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  User Question                                                  │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────┐                                                │
│  │ Embed Query │ (MiniLM 384-dim)                               │
│  └─────────────┘                                                │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────────────────────────────┐                        │
│  │ city_summary_embeddings (Milvus)    │ ← Vector Search        │
│  │   - vector (COSINE similarity)      │                        │
│  │   - sentence_chunks                 │                        │
│  └─────────────────────────────────────┘                        │
│       │                                                         │
│       ▼ (top-k city_ids)                                        │
│  ┌─────────────────────────────────────┐                        │
│  │ city_metadata (Feast Online Store)  │ ← Metadata Lookup      │
│  │   - state                           │                        │
│  │   - wiki_summary                    │                        │
│  └─────────────────────────────────────┘                        │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────┐                                                │
│  │ LLM Answer  │ (optional: GPT/Claude)                         │
│  └─────────────┘                                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```
