# City Information Q&A — RAG Demo with Feast

A complete Retrieval-Augmented Generation (RAG) demo using Feast for feature management and Milvus for vector search.


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

### 5. Run the demo workflow

```bash
python test_workflow.py
```


## Key Commands

| Command | Description |
|---------|-------------|
| `feast apply` | Register entities, feature views, and feature services |
| `feast materialize --disable-event-timestamp` | Load parquet data into the online store (Milvus) for vector search. Optionally add `-v city_summary_embeddings -v city_metadata` to materialize only those views. |
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
