from datetime import timedelta

import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
)
from feast.data_format import ParquetFormat
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Array, Float32, Int64, String
from feast.value_type import ValueType

# Entity: Identifies each city document/chunk in the knowledge base
city = Entity(
    name="city_id",
    value_type=ValueType.INT64,
    description="Unique identifier for each city Wikipedia summary (document chunk ID).",
    join_keys=["city_id"],
)

# Data Source: Parquet file containing city summaries with pre-computed embeddings
city_summaries_source = FileSource(
    name="city_summaries_source",
    file_format=ParquetFormat(),
    path="./data/city_wikipedia_summaries_with_embeddings.parquet",
    timestamp_field="event_timestamp",
    description="Wikipedia summaries of US cities (batch).",
)

# Push Source: same schema as batch; allows near real-time ingestion of new/updated docs
city_summaries_push_source = PushSource(
    name="city_summaries_push_source",
    batch_source=city_summaries_source,
    description="Push source for real-time updates to city summaries/embeddings.",
)

# Request Source: values known only at request time (e.g. user query, user id)
rag_request_source = RequestSource(
    name="rag_request_source",
    schema=[
        Field(name="query_text", dtype=String, description="User query text at request time."),
        Field(name="user_id", dtype=String, description="Optional user/session id for personalization or logging."),
    ],
    description="Request-time inputs for RAG (query, user context).",
)


# Feature View 1: City embeddings for semantic/vector search (RAG retrieval)
city_summary_embeddings = FeatureView(
    name="city_summary_embeddings",
    description="City Wikipedia summaries with embeddings for semantic search. ",
    entities=[city],
    schema=[
        Field(
            name="vector",
            dtype=Array(Float32),
            description="384-dimensional sentence embedding for semantic similarity search (MiniLM).",
            vector_index=True,
            vector_search_metric="COSINE",
        ),
        Field(
            name="sentence_chunks",
            dtype=String,
            description="Chunked sentences from the Wikipedia summary.",
        ),
    ],
    source=city_summaries_source,
    ttl=timedelta(hours=2),
    online=True,
    tags={"team": "ml-platform", "use_case": "city_qa", "type": "vector"},
)

# Feature View 2: City metadata for scalar lookups (no vector search)
city_metadata = FeatureView(
    name="city_metadata",
    description="City metadata including state and full Wikipedia summary. ",
    entities=[city],
    schema=[
        Field(
            name="state",
            dtype=String,
            description="US state where the city is located (e.g., 'New York, New York').",
        ),
        Field(
            name="wiki_summary",
            dtype=String,
            description="Full Wikipedia summary of the city.",
        ),
    ],
    source=city_summaries_source,
    ttl=timedelta(hours=2),
    online=True,
    tags={"team": "ml-platform", "use_case": "city_qa", "type": "metadata"},
)

# Feature View 3: Fresh embeddings (PushSource) for near real-time doc updates
city_summary_embeddings_realtime = FeatureView(
    name="city_summary_embeddings_realtime",
    description="Same as city_summary_embeddings but with real-time ingestion (PushSource).",
    entities=[city],
    schema=[
        Field(
            name="vector",
            dtype=Array(Float32),
            description="384-dimensional sentence embedding for semantic similarity search.",
            vector_index=True,
            vector_search_metric="COSINE",
        ),
        Field(
            name="sentence_chunks",
            dtype=String,
            description="Chunked sentences from the Wikipedia summary.",
        ),
    ],
    source=city_summaries_push_source,
    ttl=timedelta(hours=2),
    online=True,
    tags={"team": "ml-platform", "use_case": "city_qa", "type": "vector", "ingestion": "push"},
)

# On-demand Feature View: request-time derived features for RAG
@on_demand_feature_view(
    sources=[city_metadata, rag_request_source],
    schema=[
        Field(name="query_text_length", dtype=Int64, description="Length of the request query (e.g. for logging or routing)."),
        Field(name="has_user_context", dtype=Int64, description="1 if user_id was provided, else 0."),
    ],
)
def rag_request_context(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["query_text_length"] = inputs["query_text"].str.len().astype("int64")
    df["has_user_context"] = (inputs["user_id"].fillna("").str.len() > 0).astype("int64")
    return df


# Feature Service: Bundles features for the City Q&A retrieval endpoint
city_qa_v1 = FeatureService(
    name="city_qa_v1",
    features=[
        city_summary_embeddings,  
        city_metadata,  
    ],
    description="Feature service for City Information Q&A. ",
    tags={"team": "ml-platform", "version": "v1"},
)

# Feature service that includes push-backed and request-time features
city_qa_v2 = FeatureService(
    name="city_qa_v2",
    features=[
        city_summary_embeddings_realtime,  
        city_metadata,
        rag_request_context,  # Request-time derived features
    ],
    description="City Q&A with push ingestion and request-time context (query_text, user_id).",
    tags={"team": "ml-platform", "version": "v2"},
)
