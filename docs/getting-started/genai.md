# Feast for Generative AI

## Overview

Feast provides robust support for Generative AI applications, enabling teams to build, deploy, and manage feature infrastructure for Large Language Models (LLMs) and other generative AI systems. With Feast's vector database integrations and feature management capabilities, teams can implement production-ready Retrieval Augmented Generation (RAG) systems and other GenAI applications with the same reliability and operational excellence as traditional ML systems.

## Key Capabilities for GenAI

### Vector Database Support

Feast integrates with popular vector databases to store and retrieve embedding vectors efficiently:

* **Milvus**: Full support for vector similarity search with the `retrieve_online_documents_v2` method
* **SQLite**: Local vector storage and retrieval for development and testing
* **Elasticsearch**: Scalable vector search capabilities
* **Postgres with PGVector**: SQL-based vector operations
* **Qdrant**: Purpose-built vector database integration

These integrations allow you to:
- Store embeddings as features
- Perform vector similarity search to find relevant context
- Retrieve both vector embeddings and traditional features in a single API call

### Retrieval Augmented Generation (RAG)

Feast simplifies building RAG applications by providing:

1. **Document embedding storage**: Store and version document embeddings alongside your other features
2. **Vector similarity search**: Find the most relevant documents for a given query
3. **Feature retrieval**: Combine document embeddings with structured features for richer context
4. **Versioning and governance**: Track changes to your document repository over time

The typical RAG workflow with Feast involves:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Document   │     │  Document   │     │    Feast    │     │     LLM     │
│  Processing │────▶│  Embedding  │────▶│   Feature   │────▶│   Context   │
│             │     │             │     │    Store    │     │  Generation  │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

### Transforming Unstructured Data to Structured Data

Feast provides powerful capabilities for transforming unstructured data (like PDFs, text documents, and images) into structured embeddings that can be used for RAG applications:

* **Document Processing Pipelines**: Integrate with document processing tools like Docling to extract text from PDFs and other document formats
* **Chunking and Embedding Generation**: Process documents into smaller chunks and generate embeddings using models like Sentence Transformers
* **On-Demand Transformations**: Use `@on_demand_feature_view` decorator to transform raw documents into embeddings in real-time
* **Batch Processing with Spark**: Scale document processing for large datasets using Spark integration

The transformation workflow typically involves:

1. **Raw Data Ingestion**: Load documents or other data from various sources (file systems, databases, etc.)
2. **Text Extraction**: Extract text content from unstructured documents
3. **Chunking**: Split documents into smaller, semantically meaningful chunks
4. **Embedding Generation**: Convert text chunks into vector embeddings
5. **Storage**: Store embeddings and metadata in Feast's feature store

### Feature Transformation for LLMs

Feast supports on-demand transformations that can be used to:

* Process raw text into embeddings
* Chunk documents for more effective retrieval
* Normalize and preprocess features before serving to LLMs

### Getting Started with Feast for GenAI

#### Installation

To use Feast with vector database support, install with the appropriate extras:

```bash
# For Milvus support
pip install feast[milvus,nlp]

# For Elasticsearch support
pip install feast[elasticsearch]

# For Qdrant support
pip install feast[qdrant]

# For SQLite support (Python 3.10 only)
pip install feast[sqlite_vec]
```

#### Configuration

Configure your feature store to use a vector database as the online store:

```yaml
project: genai-project
provider: local
registry: data/registry.db
online_store:
  type: milvus
  path: data/online_store.db
  vector_enabled: true
  embedding_dim: 384  # Adjust based on your embedding model
  index_type: "IVF_FLAT"

offline_store:
  type: file
entity_key_serialization_version: 3
```

#### Defining Vector Features

Create feature views with vector index support:

```python
from feast import FeatureView, Field, Entity
from feast.types import Array, Float32, String

document = Entity(
    name="document_id",
    description="Document identifier",
    join_keys=["document_id"],
)

document_embeddings = FeatureView(
    name="document_embeddings",
    entities=[document],
    schema=[
        Field(
            name="vector",
            dtype=Array(Float32),
            vector_index=True,  # Enable vector search
            vector_search_metric="COSINE",  # Similarity metric
        ),
        Field(name="document_id", dtype=String),
        Field(name="content", dtype=String),
    ],
    source=document_source,
    ttl=timedelta(days=30),
)
```

#### Retrieving Similar Documents

Use the `retrieve_online_documents_v2` method to find similar documents:

```python
# Generate query embedding
query = "How does Feast support vector databases?"
query_embedding = embed_text(query)  # Your embedding function

# Retrieve similar documents
context_data = store.retrieve_online_documents_v2(
    features=[
        "document_embeddings:vector",
        "document_embeddings:document_id",
        "document_embeddings:content",
    ],
    query=query_embedding,
    top_k=3,
    distance_metric='COSINE',
).to_df()
```

## Use Cases

### Document Question-Answering

Build document Q&A systems by:
1. Storing document chunks and their embeddings in Feast
2. Converting user questions to embeddings
3. Retrieving relevant document chunks
4. Providing these chunks as context to an LLM

### Knowledge Base Augmentation

Enhance your LLM's knowledge by:
1. Storing company-specific information as embeddings
2. Retrieving relevant information based on user queries
3. Injecting this information into the LLM's context

### Semantic Search

Implement semantic search by:
1. Storing document embeddings in Feast
2. Converting search queries to embeddings
3. Finding semantically similar documents using vector search

### Scaling with Spark Integration

Feast integrates with Apache Spark to enable large-scale processing of unstructured data for GenAI applications:

* **Spark Data Source**: Load data from Spark tables, files, or SQL queries for feature generation
* **Spark Offline Store**: Process large document collections and generate embeddings at scale
* **Spark Batch Materialization**: Efficiently materialize features from offline to online stores
* **Distributed Processing**: Handle gigabytes of documents and millions of embeddings

This integration enables:
- Processing large document collections in parallel
- Generating embeddings for millions of text chunks
- Efficiently materializing features to vector databases
- Scaling RAG applications to enterprise-level document repositories

## Learn More

For more detailed information and examples:

* [Vector Database Reference](../reference/alpha-vector-database.md)
* [RAG Tutorial with Docling](../tutorials/rag-with-docling.md)
* [Milvus Quickstart Example](https://github.com/feast-dev/feast/tree/master/examples/rag/milvus-quickstart.ipynb)
* [Spark Data Source](../reference/data-sources/spark.md)
* [Spark Offline Store](../reference/offline-stores/spark.md)
* [Spark Batch Materialization](../reference/batch-materialization/spark.md)
