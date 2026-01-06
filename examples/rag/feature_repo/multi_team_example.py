"""
Example demonstrating multi-team usage of Milvus with different vector configurations.

This example shows how multiple teams (e.g., marketing and sales) can share the same
Milvus online store while using different vector embeddings with different dimensions,
metrics, and index types.
"""

from datetime import timedelta

from feast import (
    FeatureView,
    Field,
    FileSource,
)
from feast.data_format import ParquetFormat
from feast.types import Float32, Array, String, ValueType
from feast import Entity

# Marketing team uses item embeddings
marketing_item = Entity(
    name="marketing_item_id",
    description="Marketing Item ID",
    value_type=ValueType.INT64,
)

# Sales team uses customer embeddings
sales_customer = Entity(
    name="sales_customer_id",
    description="Sales Customer ID",
    value_type=ValueType.INT64,
)

marketing_source = FileSource(
    file_format=ParquetFormat(),
    path="./data/marketing_embeddings.parquet",
    timestamp_field="event_timestamp",
)

sales_source = FileSource(
    file_format=ParquetFormat(),
    path="./data/sales_embeddings.parquet",
    timestamp_field="event_timestamp",
)

# Marketing team: 768-dimension vectors with COSINE similarity and HNSW index
marketing_embeddings_fv = FeatureView(
    name="marketing_embeddings",
    entities=[marketing_item],
    schema=[
        Field(
            name="product_embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=768,  # Marketing uses 768-dim embeddings
            vector_search_metric="COSINE",  # COSINE similarity for marketing
            vector_index_type="HNSW",  # HNSW for fast approximate search
        ),
        Field(name="product_name", dtype=String),
        Field(name="category", dtype=String),
    ],
    source=marketing_source,
    ttl=timedelta(days=1),
)

# Sales team: 128-dimension vectors with L2 distance and IVF_FLAT index
sales_embeddings_fv = FeatureView(
    name="sales_embeddings",
    entities=[sales_customer],
    schema=[
        Field(
            name="customer_embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=128,  # Sales uses 128-dim embeddings
            vector_search_metric="L2",  # L2 distance for sales
            vector_index_type="IVF_FLAT",  # IVF_FLAT for balanced speed/accuracy
        ),
        Field(name="customer_name", dtype=String),
        Field(name="segment", dtype=String),
    ],
    source=sales_source,
    ttl=timedelta(days=7),
)
