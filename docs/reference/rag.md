# Retrieval Augmented Generation (RAG)

Feast provides built-in support for Retrieval Augmented Generation (RAG) through its `FeastRAGRetriever` class, which integrates with HuggingFace's transformers library. This functionality allows you to use your feature store as a knowledge base for LLM applications.

## Installation

To use the RAG functionality, install Feast with the RAG extras:

```bash
pip install feast[rag]
```

This will install the necessary dependencies including `transformers`, `sentence-transformers`, and `torch`.

## Overview

The RAG implementation in Feast consists of two main components:

1. **Vector Store**: Abstract interface and Feast implementation for storing and querying vector embeddings
2. **RAG Retriever**: Implementation that extends HuggingFace's `RagRetriever` class

## Components

### Vector Store

The `VectorStore` abstract base class defines the interface for vector storage and retrieval:

```python
from feast import VectorStore

class CustomVectorStore(VectorStore):
    def query(
        self,
        query_vector: Optional[np.ndarray] = None,
        query_string: Optional[str] = None,
        top_k: int = 10,
    ):
        # Implement vector/text search logic here
        pass
```

Feast provides a built-in implementation `FeastVectorStore` that uses Feast's feature store capabilities:

```python
from feast import FeastVectorStore

vector_store = FeastVectorStore(
    store=feature_store,
    rag_view=document_feature_view,
    features=["embedding", "text"]
)
```

### RAG Retriever

The `FeastRAGRetriever` class extends HuggingFace's `RagRetriever` to provide seamless integration with Feast:

```python
from feast import FeastRAGRetriever, FeastIndex

retriever = FeastRAGRetriever(
    question_encoder_tokenizer=tokenizer,
    question_encoder=encoder,
    generator_tokenizer=generator_tokenizer,
    generator_model=generator_model,
    feast_repo_path="./feature_repo",
    vector_store=vector_store,
    search_type="hybrid",  # Can be "text", "vector", or "hybrid"
    config=config,
    index=FeastIndex(vector_store)
)
```

## Usage Example

Here's a complete example of setting up and using RAG with Feast:

```python
from feast import FeatureStore, FeatureView, Field, FeastVectorStore, FeastRAGRetriever, FeastIndex
from feast.types import Array, Float32, String
from transformers import T5Tokenizer, T5ForConditionalGeneration
from sentence_transformers import SentenceTransformer

# 1. Set up your feature view for document storage
document_view = FeatureView(
    name="document_store",
    schema=[
        Field(name="text", dtype=String),
        Field(name="embedding", dtype=Array(Float32, (384,)))
    ],
    # ... other feature view configuration
)

# 2. Initialize the vector store
store = FeatureStore(repo_path="./feature_repo")
vector_store = FeastVectorStore(
    store=store,
    rag_view=document_view,
    features=["embedding", "text"]
)

# 3. Initialize models
tokenizer = T5Tokenizer.from_pretrained("t5-small")
model = T5ForConditionalGeneration.from_pretrained("t5-small")
query_encoder = SentenceTransformer("all-MiniLM-L6-v2")

# 4. Create the RAG retriever
retriever = FeastRAGRetriever(
    question_encoder_tokenizer=tokenizer,
    question_encoder=model,
    generator_tokenizer=tokenizer,
    generator_model=model,
    feast_repo_path="./feature_repo",
    vector_store=vector_store,
    search_type="hybrid",
    config={"index_name": "docs"},
    index=FeastIndex(vector_store),
    query_encoder_model=query_encoder
)

# 5. Use the retriever
# For retrieval only
doc_scores, doc_dicts = retriever.retrieve(
    question_hidden_states,
    n_docs=5
)

# For generation with retrieved context
answer = retriever.generate_answer(
    "What is machine learning?",
    top_k=3,
    max_new_tokens=100
)
```

## Search Types

The `FeastRAGRetriever` supports three types of search:

1. **text**: Pure text-based search using the query string
2. **vector**: Pure vector similarity search using encoded query embeddings
3. **hybrid**: Combination of text and vector search

## Configuration

Key configuration options for `FeastRAGRetriever`:

- `search_type`: The type of search to perform ("text", "vector", or "hybrid")
- `query_encoder_model`: Model to use for encoding queries (string path or SentenceTransformer instance)
- `format_document`: Optional function to customize document formatting (defaults to key-value format)
- `id_field`: Field to use as document ID

## Performance Considerations

1. For optimal performance, ensure your feature view containing document embeddings has appropriate indexing
2. Consider using a more powerful model than T5-small for production use cases
3. The quality of retrieval depends heavily on the choice of embedding model

## Limitations

1. The current implementation requires the entire document corpus to be stored in Feast
2. Vector similarity search performance depends on the underlying feature store implementation
3. Document retrieval is optimized for cosine similarity scoring

## See Also

- [Feature Views Documentation](feature-repository.md)
- [Online Store Documentation](online-stores/) 