from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Array, Float32, Int64, UnixTimestamp

# This is for Milvus
# Note that file source paths are not validated, so there doesn't actually need to be any data
# at the paths for these file sources. Since these paths are effectively fake, this example
# feature repo should not be used for historical retrieval.

rag_documents_source = FileSource(
    path="data/embedded_documents.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

item = Entity(
    name="item_id",  # The name is derived from this argument, not object name.
    join_keys=["item_id"],
)

document_embeddings = FeatureView(
    name="embedded_documents",
    entities=[item],
    schema=[
        Field(
            name="vector",
            dtype=Array(Float32),
            vector_index=True,
            vector_search_metric="L2",
        ),
        Field(name="item_id", dtype=Int64),
        Field(name="created_timestamp", dtype=UnixTimestamp),
        Field(name="event_timestamp", dtype=UnixTimestamp),
    ],
    source=rag_documents_source,
    ttl=timedelta(hours=24),
)
