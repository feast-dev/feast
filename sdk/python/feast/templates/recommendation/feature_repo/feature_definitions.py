from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource, Project
from feast.types import Array, Float32, String

project = Project(
    name="%PROJECT_NAME%",
    description="A project for product recommendations using vector similarity search",
)

product = Entity(name="product", join_keys=["product_id"])

products_source = FileSource(
    name="products_source",
    path="%PARQUET_PATH%",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# test_workflow.py writes model embeddings before materialization.
product_embeddings = FeatureView(
    name="product_embeddings",
    entities=[product],
    ttl=timedelta(days=365),
    schema=[
        Field(
            name="embedding", dtype=Array(Float32), vector_index=True, vector_length=384
        ),
        Field(name="product_name", dtype=String),
        Field(name="description", dtype=String),
        Field(name="category", dtype=String),
        Field(name="price", dtype=Float32),
        Field(name="rating", dtype=Float32),
    ],
    online=True,
    source=products_source,
    tags={"team": "recommendations"},
)

recommendation_service = FeatureService(
    name="recommendation_service",
    features=[product_embeddings],
)
