from typing import Optional


class VectorStoreConfig:
    # Whether to enable the online store for vector similarity search,
    # This is only applicable for online store.
    vector_enabled: Optional[bool] = False

    # If vector is enabled, the length of the vector field
    vector_len: Optional[int] = 512

    # The vector similarity metric to use in KNN search
    # It is helpful for vector database that does not support config at retrieval runtime
    # E.g. Elasticsearch dense_vector field at
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html
    similarity: Optional[str] = "cosine"
