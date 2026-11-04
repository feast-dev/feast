from typing import Optional


class VectorStoreConfig:
    # Whether to enable the online store for vector similarity search,
    # This is only applicable for online store.
    vector_enabled: Optional[bool] = False

    # The vector similarity metric to use in KNN search
    # It is helpful for vector database that does not support config at retrieval runtime
    # E.g.
    # Elasticsearch:
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html
    # Qdrant:
    # https://qdrant.tech/documentation/concepts/search/#metrics
    similarity: Optional[str] = "cosine"
