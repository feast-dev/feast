# Feast for Generative AI

## Overview

Feast provides robust support for Generative AI applications, enabling teams to build, deploy, and manage feature infrastructure for Large Language Models (LLMs) and other Generative AI (GenAI) applications. With Feast's vector database integrations and feature management capabilities, teams can implement production-ready Retrieval Augmented Generation (RAG) systems and other GenAI applications with the same reliability and operational excellence as traditional ML systems.

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

1. **Embedding storage**: Store and version embeddings alongside your other features
2. **Vector similarity search**: Find the most relevant data/documents for a given query
3. **Feature retrieval**: Combine embeddings with structured features for richer context
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

### DocEmbedder: End-to-End Document Ingestion Pipeline

The `DocEmbedder` class provides an end-to-end pipeline for ingesting documents into Feast's online vector store. It handles chunking, embedding generation, and writing results -- all in a single step.

#### Key Components

* **`DocEmbedder`**: High-level orchestrator that runs the full pipeline: chunk → embed → schema transform → write to online store
* **`BaseChunker` / `TextChunker`**: Pluggable chunking layer. `TextChunker` splits text by word count with configurable `chunk_size`, `chunk_overlap`, `min_chunk_size`, and `max_chunk_chars`
* **`BaseEmbedder` / `MultiModalEmbedder`**: Pluggable embedding layer with modality routing. `MultiModalEmbedder` supports text (via sentence-transformers) and image (via CLIP) with lazy model loading
* **`SchemaTransformFn`**: A user-defined function that transforms the chunked + embedded DataFrame into the format expected by the FeatureView schema

#### Quick Example

```python
from feast import DocEmbedder
import pandas as pd

# Prepare your documents
df = pd.DataFrame({
    "id": ["doc1", "doc2"],
    "text": ["First document content...", "Second document content..."],
})

# Create DocEmbedder -- automatically generates a FeatureView and applies the repo
embedder = DocEmbedder(
    repo_path="feature_repo/",
    feature_view_name="text_feature_view",
)

# Embed and ingest documents in one step
result = embedder.embed_documents(
    documents=df,
    id_column="id",
    source_column="text",
    column_mapping=("text", "text_embedding"),
)
```

#### Features

* **Auto-generates FeatureView**: Creates a Python file with Entity and FeatureView definitions compatible with `feast apply`
* **Auto-applies repo**: Registers the generated FeatureView in the registry automatically
* **Custom schema transform**: Provide your own `SchemaTransformFn` to control how chunked + embedded data maps to your FeatureView schema
* **Extensible**: Subclass `BaseChunker` or `BaseEmbedder` to plug in your own chunking or embedding strategies

For a complete walkthrough, see the [DocEmbedder tutorial notebook](../../examples/rag-retriever/rag_feast_docembedder.ipynb).
### Feature Transformation for LLMs

Feast supports transformations that can be used to:

* Process raw text into embeddings
* Chunk documents for more effective retrieval
* Normalize and preprocess features before serving to LLMs
* Apply custom transformations to adapt features for specific LLM requirements

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

### AI Agents with Context and Memory

Feast can serve as both the **context provider** and **persistent memory layer** for AI agents. Unlike stateless RAG pipelines, agents make autonomous decisions about which tools to call and can write state back to the feature store:

1. **Structured context**: Retrieve customer profiles, account data, and other entity-keyed features
2. **Knowledge retrieval**: Search vector embeddings for relevant documents
3. **Persistent memory**: Store and recall per-entity interaction history (last topic, resolution, preferences) using `write_to_online_store`
4. **Governed access**: All reads and writes are subject to the same RBAC, TTL, and audit policies as any other feature

With MCP enabled, agents built with any framework (LangChain, LlamaIndex, CrewAI, AutoGen, or custom) can discover and call Feast tools dynamically. See the [Feast-Powered AI Agent example](../../examples/agent_feature_store/) and the blog post [Building AI Agents with Feast](https://feast.dev/blog/feast-agents-mcp/) for a complete walkthrough.

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

### Scaling with Ray Integration

Feast integrates with Ray to enable distributed processing for RAG applications:

* **Ray Compute Engine**: Distributed feature computation using Ray's task and actor model
* **Ray Offline Store**: Process large document collections and generate embeddings at scale
* **Ray Batch Materialization**: Efficiently materialize features from offline to online stores
* **Distributed Embedding Generation**: Scale embedding generation across multiple nodes

This integration enables:
- Distributed processing of large document collections
- Parallel embedding generation for millions of text chunks
- Kubernetes-native scaling for RAG applications
- Efficient resource utilization across multiple nodes
- Production-ready distributed RAG pipelines

For detailed information on building distributed RAG applications with Feast and Ray, see [Feast + Ray: Distributed Processing for RAG Applications](https://feast.dev/blog/feast-ray-distributed-processing/).

## Model Context Protocol (MCP) Support

Feast supports the Model Context Protocol (MCP), which enables AI agents and applications to interact with your feature store through standardized MCP interfaces. This allows seamless integration with LLMs and AI agents for GenAI applications.

### Key Benefits of MCP Support

* **Standardized AI Integration**: Enable AI agents to discover and use features dynamically without hardcoded definitions
* **Easy Setup**: Add MCP support with a simple configuration change and `pip install feast[mcp]`
* **Agent-Friendly APIs**: Expose feature store capabilities through MCP tools that AI agents can understand and use
* **Production Ready**: Built on top of Feast's proven feature serving infrastructure

### Getting Started with MCP

1. **Install MCP support**:
   ```bash
   pip install feast[mcp]
   ```

2. **Configure your feature store** to use MCP:
   ```yaml
   feature_server:
     type: mcp
     enabled: true
     mcp_enabled: true
     mcp_transport: http
     mcp_server_name: "feast-feature-store"
     mcp_server_version: "1.0.0"
   ```

By default, Feast uses the SSE-based MCP transport (`mcp_transport: sse`). Streamable HTTP (`mcp_transport: http`) is recommended for improved compatibility with some MCP clients.

### How It Works

The MCP integration uses the `fastapi_mcp` library to automatically transform your Feast feature server's FastAPI endpoints into MCP-compatible tools. When you enable MCP support:

1. **Automatic Discovery**: The integration scans your FastAPI application and discovers all available endpoints
2. **Tool Generation**: Each endpoint becomes an MCP tool with auto-generated schemas and descriptions
3. **Dynamic Access**: AI agents can discover and call these tools dynamically without hardcoded definitions
4. **Standard Protocol**: Uses the Model Context Protocol for standardized AI-to-API communication

### Available MCP Tools

The fastapi_mcp integration automatically exposes your Feast feature server's FastAPI endpoints as MCP tools. This means AI assistants can:

* **Call `/get-online-features`** to retrieve features from the feature store
* **Call `/retrieve-online-documents`** to perform vector similarity search
* **Call `/write-to-online-store`** to persist agent state (memory, notes, interaction history)
* **Use `/health`** to check server status  

For a basic MCP example, see the [MCP Feature Store Example](../../examples/mcp_feature_store/). For a full agent with persistent memory, see the [Feast-Powered AI Agent Example](../../examples/agent_feature_store/).

## Learn More

For more detailed information and examples:

* [Vector Database Reference](../reference/alpha-vector-database.md)
* [RAG Tutorial with Docling](../tutorials/rag-with-docling.md)
* [DocEmbedder Tutorial Notebook](../../examples/rag-retriever/rag_feast_docembedder.ipynb)
* [RAG Fine Tuning with Feast and Milvus](../../examples/rag-retriever/README.md)
* [Milvus Quickstart Example](https://github.com/feast-dev/feast/tree/master/examples/rag/milvus-quickstart.ipynb)
* [Feast + Ray: Distributed Processing for RAG Applications](https://feast.dev/blog/feast-ray-distributed-processing/)
* [MCP Feature Store Example](../../examples/mcp_feature_store/)
* [Feast-Powered AI Agent Example (with Memory)](../../examples/agent_feature_store/)
* [Blog: Building AI Agents with Feast](https://feast.dev/blog/feast-agents-mcp/)
* [MCP Feature Server Reference](../reference/feature-servers/mcp-feature-server.md)
* [Spark Data Source](../reference/data-sources/spark.md)
* [Spark Offline Store](../reference/offline-stores/spark.md)
* [Spark Compute Engine](../reference/compute-engine/spark.md)
