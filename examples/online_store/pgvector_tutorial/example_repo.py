
# This file defines the feature repository for the pgvector tutorial

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource
from feast.data_format import ParquetFormat
from feast.types import Float32, Array, String

# Define entity
product = Entity(
    name="product_id",
    description="Product ID",
    join_keys=["id"],
)

# Define data source
source = FileSource(
    file_format=ParquetFormat(),
    path="data/sample_data.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

# Define feature view with vector embeddings
product_embeddings = FeatureView(
    name="product_embeddings",
    entities=[product],
    ttl=timedelta(days=30),
    schema=[
        Field(
            name="embedding", 
            dtype=Array(Float32), 
            vector_index=True,  # Mark as vector field
            vector_search_metric="L2"  # Use L2 distance for similarity
        ),
        Field(name="name", dtype=String),
        Field(name="description", dtype=String),
    ],
    source=source,
    online=True,
)
