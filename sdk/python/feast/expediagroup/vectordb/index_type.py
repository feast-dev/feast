from enum import Enum


class IndexType(Enum):
    # Milvus: https://milvus.io/docs/build_index.md
    # floating point
    flat = "flat"
    ivf_flat = "ivf_flat"
    ivf_sq8 = "ivf_sq8"
    annoy = "annoy"
    diskann = "disk_ann"
    # binary:
    bin_flat = "bin_flat"
    bin_ivf_flat = "bin_ivf_flat"
    # ElasticSearch and Milvus
    hnsw = "hnsw"
    # hnsw in ElasticSearch will just translate to "index": true,
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html#index-vectors-knn-search
