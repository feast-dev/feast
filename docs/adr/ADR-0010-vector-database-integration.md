# ADR-0010: Vector Database Integration for LLM/RAG Support

## Status

Accepted

## Context

Feast is an abstraction layer for ML infrastructure that integrates with diverse online and offline data sources. With the rise of Large Language Model (LLM) applications, particularly Retrieval Augmented Generation (RAG), there was a need to support:

- Transforming document data into embeddings (features).
- Loading embeddings into vector-capable databases (online stores).
- Retrieving the most similar documents given a query embedding at serving time.

These capabilities align naturally with Feast's existing concepts of feature views, materialization, and online serving, but required a new retrieval interface for similarity search.

## Decision

Extend Feast's online store interface with a `retrieve_online_documents` method that performs approximate nearest neighbor (ANN) search.

### Core Design

Treat embeddings/vectors as features within existing feature views. Add a new retrieval interface to online stores:

```python
class OnlineStore:
    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_feature: str,
        embedding: List[float],
        top_k: int,
        distance_metric: Optional[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        ...
```

### Supported Stores

Online stores that implement vector search:

- **PostgreSQL with pgvector**: ANN search using HNSW and IVFFlat indexes.
- **Elasticsearch**: Vector similarity search with hybrid search capabilities.
- **Milvus**: Dedicated vector database with large-scale ANN support.
- **Qdrant**: Vector similarity search engine.
- **SQLite with sqlite-vec**: Lightweight local vector search.

### Usage

```python
from feast import FeatureStore

store = FeatureStore(".")

# Retrieve top-k similar documents
results = store.retrieve_online_documents(
    feature="document_embeddings:embedding",
    query=query_embedding,
    top_k=5,
)
```

### Key Decisions

- **Embeddings as features**: Rather than introducing a new primitive, embeddings are stored as features in existing feature views. This reuses Feast's materialization, versioning, and serving infrastructure.
- **Interface on OnlineStore**: The `retrieve_online_documents` method is added to the `OnlineStore` interface, allowing each store implementation to use its native vector search capabilities.
- **Incremental store support**: Not all online stores support vector search. Stores that don't implement the method raise a clear error. New stores are added based on community demand and contributions.

## Consequences

### Positive

- Feast naturally extends from MLops to LLMops/RAG use cases.
- Reuses existing Feast concepts (feature views, materialization, online stores) without introducing new primitives.
- Multiple vector database backends supported, giving users flexibility.
- RAG applications can use Feast as a unified feature and document store.

### Negative

- Vector search capabilities vary significantly across stores (e.g., hybrid search in Elasticsearch vs. pure ANN in others). Feast's interface targets the lowest common denominator.
- Embedding pipeline (encoding documents into vectors) is not fully managed by Feast; users handle this externally.

## References

- Original RFC: [Feast RFC-040: Document Store / LLM Extension](https://docs.google.com/document/d/18IWzLEA9i2lDWnbfbwXnMCg3StlqaLVI-uRpQjr_Vos/edit)
- GitHub Issue: [#3965](https://github.com/feast-dev/feast/issues/3965)
- Implementation: Online store implementations in `sdk/python/feast/infra/online_stores/`
- Examples: `examples/rag/`, `examples/online_store/pgvector_tutorial/`, `examples/online_store/milvus_tutorial/`
