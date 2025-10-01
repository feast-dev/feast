# Feast Ray RAG Template - Batch Embedding at scale for RAG with Ray

RAG (Retrieval-Augmented Generation) template using Feast with Ray for distributed processing and Milvus for vector search.

## 🚀 What This Template Provides

- **🎬 Real IMDB Data**: 48K+ movies from Kaggle for realistic demonstrations
- **⚡ Ray Distributed Processing**: Parallel embedding generation across workers
- **🔍 Vector Search**: Milvus integration for semantic similarity
- **🎯 Complete Pipeline**: Data → Embeddings → Search in one workflow

## 📁 Template Structure

```
ray_rag/
├── feature_repo/
│   ├── feature_store.yaml      # Ray + Milvus configuration
│   ├── example_repo.py         # Feature definitions with Ray UDF
│   ├── test_workflow.py        # End-to-end demo
│   └── data/                   # IMDB dataset
├── bootstrap.py                # Downloads IMDB data from Kaggle
└── README.md
```

## 🚦 Quick Start

### 1. Initialize Template

```bash
feast init -t ray_rag my_rag_project
cd my_rag_project/feature_repo
```

The template automatically downloads the IMDB dataset from Kaggle during initialization.

### 2. Install Dependencies

```bash
# Core dependencies
pip install feast[ray] sentence-transformers

# Optional: For Kaggle API (template works without it)
pip install kaggle
```

### 3. Apply Feature Definitions

```bash
feast apply
```

### 4. Materialize Features

```bash
# Generate embeddings for all 48K+ movies
feast materialize --disable-event-timestamp 
```

### 5. Test the Pipeline

```bash
python test_workflow.py
```

Expected output:
- ✅ ~47K embeddings materialized
- ✅ Vector search working with relevant results
- ✅ Similarity scores for relevant matches


## 📊 Architecture

```
Raw Data (IMDB CSV)
    ↓
Ray Offline Store (Distributed I/O)
    ↓
Ray Compute Engine (Parallel Embedding Generation)
    ↓  
Milvus Online Store (Vector Search)
    ↓
RAG Application
```


## 🎬 Example Workflow

```python
from feast import FeatureStore
from sentence_transformers import SentenceTransformer

# 1. Initialize
store = FeatureStore(repo_path=".")

# 2. Materialize (embeddings generated in parallel)
store.materialize_incremental(end_date)

# 3. Search using Feast API
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
query_embedding = model.encode(["sci-fi movie about space"])[0].tolist()

results = store.retrieve_online_documents_v2(
    features=[
        "document_embeddings:embedding",
        "document_embeddings:movie_name",
        "document_embeddings:movie_director",
    ],
    query=query_embedding,
    top_k=5,
).to_dict()

# Display results with metadata
for i in range(len(results["document_id_pk"])):
    print(f"{i+1}. {results['movie_name'][i]}")
    print(f"   Director: {results['movie_director'][i]}")
    print(f"   Distance: {results['distance'][i]:.3f}")
```
