from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Array, Float32, Int64, String, UnixTimestamp

# Define entities
item = Entity(name="item_id", join_keys=["item_id"])

# Define data source
documents_source = FileSource(
    name="documents_source",
    path="data/documents.parquet",
    timestamp_field="event_timestamp",
)

# Define feature view with vector field
document_embeddings = FeatureView(
    name="document_embeddings",
    entities=[item],
    schema=[
        Field(
            name="vector",
            dtype=Array(Float32),
            vector_index=True,
            vector_search_metric="COSINE",
        ),
        Field(name="item_id", dtype=Int64),
        Field(name="content", dtype=String),
        Field(name="event_timestamp", dtype=UnixTimestamp),
    ],
    source=documents_source,
    ttl=timedelta(hours=24),
)
