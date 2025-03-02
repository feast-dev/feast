from datetime import timedelta

from feast import (
    FeatureView,
    Field,
    FileSource,
)
from feast.data_format import ParquetFormat
from feast.types import Float32, Array, String, ValueType
from feast import Entity, RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from sentence_transformers import SentenceTransformer
from typing import Dict, Any, List

chunk = Entity(
    name="chunk_id",
    description="Chunk ID",
    value_type=ValueType.STRING,
)

parquet_file_path = "./data/docling_samples.parquet"

source = FileSource(
    file_format=ParquetFormat(),
    path=parquet_file_path,
    timestamp_field="created",
)

input_request = RequestSource(
    name="request_source",
    schema=[
        Field(name="query_string", dtype=String),
    ],
)

EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
embedding_model = SentenceTransformer(EMBED_MODEL_ID)


@on_demand_feature_view(
    sources=[input_request],
    schema=[
        Field(name="chunk_embedding", dtype=Array(Float32)),
    ],
    mode="python",
    singleton=True,
    write_to_online_store=True,
)
def embed_chunk(inputs: Dict[str, Any]) -> dict[str, List]:
    output = {
        "conv_rate_plus_acc_singleton": embedding_model.encode([
            inputs["query_string"]], normalize_embeddings=True,
        ).tolist()[0]
    }
    return output


docling_example_feature_view = FeatureView(
    name="docling_feature_view",
    entities=[chunk],
    schema=[
        Field(name="file_name", dtype=String),
        # Field(name="full_document_markdown", dtype=String),
        Field(name="raw_chunk_markdown", dtype=String),
        Field(
            name="chunk_embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_search_metric="COSINE",
        ),
        # Field(name="bytes", dtype=String),
        Field(name="chunk_id", dtype=String),
    ],
    source=source,
    ttl=timedelta(hours=2),
)
