# Feast Ray RAG Template - Batch Embedding at scale for RAG with Ray

RAG (Retrieval-Augmented Generation) template using Feast with Ray for distributed processing and Milvus for vector search.

## 🚀 What This Template Provides

- **🎬 Sample IMDB Data**: 10 curated movies included for quick demos
- **⚡ Ray Distributed Processing**: Parallel embedding generation across workers
- **🔍 Vector Search**: Milvus integration for semantic similarity
- **🎯 Complete Pipeline**: Data → Embeddings → Search in one workflow
- **📦 Ready to Scale**: Easy upgrade to full dataset (48K+ movies) if needed

## 📁 Template Structure

```
ray_rag/
├── feature_repo/
│   ├── feature_store.yaml      # Ray + Milvus configuration
│   ├── example_repo.py         # Feature definitions with Ray UDF
│   ├── test_workflow.py        # End-to-end demo
│   └── data/                   
│       └── raw_movies.parquet  # Sample IMDB dataset (10 movies)
├── bootstrap.py                # Template initialization
└── README.md
```

## 🚦 Quick Start

### 1. Initialize Template

```bash
feast init -t ray_rag my_rag_project
cd my_rag_project/feature_repo
```

The template includes a sample dataset with 10 movies for quick testing.

### 2. Install Dependencies

```bash
# Core dependencies
pip install feast[ray] sentence-transformers
```

### 3. Apply Feature Definitions

```bash
feast apply
```

### 4. Materialize Features

```bash
# Generate embeddings for sample movies
feast materialize --disable-event-timestamp 
```

### 5. Test the Pipeline

```bash
python test_workflow.py
```

Expected output with sample dataset:
- ✅ 10 embeddings materialized
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

## 📥 Using the Full IMDB Dataset (Optional)

The template includes a small sample dataset (10 movies) for quick testing. To work with the full dataset containing 48K+ movies:

### Option 1: Download via Kaggle API

1. **Setup Kaggle credentials:**
   ```bash
   # Get API credentials from https://www.kaggle.com/account
   # Place kaggle.json in ~/.kaggle/
   chmod 600 ~/.kaggle/kaggle.json
   ```

2. **Install Kaggle API and download dataset:**
   ```bash
   pip install kaggle
   
   # Download to your feature_repo/data directory
   cd feature_repo
   kaggle datasets download -d yashgupta24/48000-movies-dataset -p ./data --unzip
   ```

3. **Convert to parquet format:**
   ```python
   import pandas as pd
   import pyarrow as pa
   import pyarrow.parquet as pq
   from pathlib import Path
   
   # Read the CSV file (filename may vary)
   data_path = Path("./data")
   csv_files = list(data_path.glob("*.csv"))
   df = pd.read_csv(csv_files[0])
   
   # Convert DatePublished to datetime with UTC timezone
   df = df.dropna(subset=["DatePublished"])
   df["DatePublished"] = pd.to_datetime(df["DatePublished"], errors="coerce", utc=True)
   
   # Write to parquet
   table = pa.Table.from_pandas(df)
   pq.write_table(table, data_path / "raw_movies.parquet")
   print(f"✅ Converted {len(df)} movies to parquet format")
   ```

4. **Run the full pipeline:**
   ```bash
   feast apply
   feast materialize --disable-event-timestamp
   python test_workflow.py
   ```

### Option 2: Use Your Own Dataset

Replace `feature_repo/data/raw_movies.parquet` with your own dataset. Required schema:

- `id`: Unique identifier (string)
- `Name`: Movie name (string)
- `Description`: Movie description for embedding (string)
- `Director`: Director name (string)
- `Genres`: Comma-separated genres (string)
- `RatingValue`: Rating score (float)
- `DatePublished`: Publication date (datetime with UTC timezone)

The Ray embedding pipeline will automatically process your dataset in parallel.
