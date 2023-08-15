from enum import Enum

from pymilvus.client.types import IndexType as MilvusIndexType


class IndexType(Enum):
    # Milvus: https://milvus.io/docs/build_index.md
    # floating point
    flat = "flat", MilvusIndexType.FLAT
    ivf_flat = "ivf_flat", MilvusIndexType.IVF_FLAT
    ivf_sq8 = "ivf_sq8", MilvusIndexType.IVF_SQ8
    ivf_sq8_h = "ivf_sq8_h", MilvusIndexType.IVF_SQ8_H
    annoy = "annoy", MilvusIndexType.ANNOY
    # todo diskann = "disk_ann" cannot be mapped to a Milvus index type
    # binary:
    bin_flat = "bin_flat", MilvusIndexType.FLAT
    bin_ivf_flat = "bin_ivf_flat", MilvusIndexType.IVF_FLAT
    # ElasticSearch and Milvus
    hnsw = "hnsw", MilvusIndexType.HNSW
    # hnsw in ElasticSearch will just translate to "index": true,
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html#index-vectors-knn-search

    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    def __init__(self, _: str, milvus_type: MilvusIndexType):
        self._milvus_index_type = milvus_type

    def __str__(self):
        return self.value

    @property
    def milvus_index_type(self):
        return self._milvus_index_type
