from typing import Optional


class VectorStoreConfig:
    # Whether to enable the vector for vector similarity search,
    # This is only applicable for online store.
    vector_enabled: Optional[bool] = False

    # If vector is enabled, the length of the vector field
    vector_len: Optional[int] = 512

    # The vector similarity metric to use in KNN search
    # more details: https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html
    similarity: Optional[str] = "cosine"
