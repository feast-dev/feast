from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource
from feast.data_format import ParquetFormat
from feast.types import Array, Float32, Float64, Int64, String, ValueType

customer = Entity(
    name="customer_id",
    description="Unique customer identifier",
    value_type=ValueType.STRING,
)

document = Entity(
    name="doc_id",
    description="Knowledge-base document chunk identifier",
    value_type=ValueType.INT64,
)

customer_profile_source = FileSource(
    file_format=ParquetFormat(),
    path="data/customer_profiles.parquet",
    timestamp_field="event_timestamp",
)

knowledge_base_source = FileSource(
    file_format=ParquetFormat(),
    path="data/knowledge_base.parquet",
    timestamp_field="event_timestamp",
)

agent_memory_source = FileSource(
    file_format=ParquetFormat(),
    path="data/agent_memory.parquet",
    timestamp_field="event_timestamp",
)

customer_profile = FeatureView(
    name="customer_profile",
    entities=[customer],
    schema=[
        Field(name="name", dtype=String),
        Field(name="email", dtype=String),
        Field(name="plan_tier", dtype=String),
        Field(name="account_age_days", dtype=Int64),
        Field(name="total_spend", dtype=Float64),
        Field(name="open_tickets", dtype=Int64),
        Field(name="satisfaction_score", dtype=Float64),
    ],
    source=customer_profile_source,
    ttl=timedelta(days=1),
)

knowledge_base = FeatureView(
    name="knowledge_base",
    entities=[document],
    schema=[
        Field(
            name="vector",
            dtype=Array(Float32),
            vector_index=True,
            vector_search_metric="COSINE",
        ),
        Field(name="title", dtype=String),
        Field(name="content", dtype=String),
        Field(name="category", dtype=String),
    ],
    source=knowledge_base_source,
    ttl=timedelta(days=7),
)

agent_memory = FeatureView(
    name="agent_memory",
    entities=[customer],
    schema=[
        Field(name="last_topic", dtype=String),
        Field(name="last_resolution", dtype=String),
        Field(name="interaction_count", dtype=Int64),
        Field(name="preferences", dtype=String),
        Field(name="open_issue", dtype=String),
    ],
    source=agent_memory_source,
    ttl=timedelta(days=30),
)
