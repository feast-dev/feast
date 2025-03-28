from datetime import timedelta

import pandas as pd
from feast import (
    FeatureView,
    Field,
    FileSource,
)
from feast.data_format import ParquetFormat
from feast.types import Float32, Array, String, ValueType, PdfBytes
from feast import Entity, RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from sentence_transformers import SentenceTransformer
from typing import Dict, Any, List

chunk = Entity(
    name="chunk_id",
    description="Chunk ID",
    value_type=ValueType.STRING,
    join_keys=["chunk_id"],
)

document = Entity(
    name="document_id",
    description="Document ID",
    value_type=ValueType.STRING,
    join_keys=["document_id"],
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


def embed_chunk(inputs: Dict[str, Any]) -> Dict[str, List[float]]:
    output = {
        "query_embedding": embedding_model.encode([
            inputs["query_string"]
        ], normalize_embeddings=True).tolist()[0]
    }
    return output

def embed_text(text: str) -> list[float]:
    """Generate an embedding for a given text."""
    return embedding_model.encode([text], normalize_embeddings=True).tolist()[0]


docling_example_feature_view = FeatureView(
    name="docling_feature_view",
    entities=[chunk],
    schema=[
        Field(name="file_name", dtype=String),
        Field(name="raw_chunk_markdown", dtype=String),
        Field(
            name="vector",
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

import hashlib

def generate_chunk_id(file_name: str, raw_chunk_markdown: str="") -> str:
    """Generate a unique chunk ID based on file_name and raw_chunk_markdown."""
    unique_string = f"{file_name}-{raw_chunk_markdown}" if raw_chunk_markdown != "" else f"{file_name}"
    return hashlib.sha256(unique_string.encode()).hexdigest()
    
from docling.datamodel.base_models import DocumentStream
# Load tokenizer and embedding model
EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
MAX_TOKENS = 64  # Small token limit for demonstration

import io
from docling.document_converter import DocumentConverter
from transformers import AutoTokenizer
from sentence_transformers import SentenceTransformer
from docling.chunking import HybridChunker

tokenizer = AutoTokenizer.from_pretrained(EMBED_MODEL_ID)
embedding_model = SentenceTransformer(EMBED_MODEL_ID)
chunker = HybridChunker(tokenizer=tokenizer, max_tokens=MAX_TOKENS, merge_peers=True)

input_request_pdf = RequestSource(
    name="pdf_request_source",
    schema=[
        Field(name="document_id", dtype=String),        
        Field(name="pdf_bytes", dtype=PdfBytes),
        Field(name="file_name", dtype=String),
    ],
)

@on_demand_feature_view(
    sources=[input_request_pdf],
    schema=[
        Field(name="document_id", dtype=String),
        Field(name="chunk_id", dtype=String),
        Field(name="chunk_text", dtype=String),
        Field(
            name="vector", 
            dtype=Array(Float32), 
            vector_index=False
        ),
    ],
    mode="python",
    singleton=True,
    write_to_online_store=True,
)
def docling_transform_docs(inputs: Dict[str, Any]):
    document_ids, chunks, embeddings, chunk_ids = [], [], [], []
    buf = io.BytesIO(
        inputs["pdf_bytes"],
    )
    source = DocumentStream(name=inputs["file_name"], stream=buf)
    converter = DocumentConverter()
    result = converter.convert(source)
    for i, chunk in enumerate(chunker.chunk(dl_doc=result.document)):
        raw_chunk = chunker.serialize(chunk=chunk)
        # embedding = embed_chunk(raw_chunk).get("query_embedding", [])
        embedding = embed_text(raw_chunk)
        chunk_id = f"chunk-{i}"
        document_ids.append(inputs["document_id"])
        chunks.append(raw_chunk)
        chunk_ids.append(chunk_id)
        embeddings.append(embedding)
    return {
        "document_id": document_ids,
        "chunk_id": chunk_ids,
        "vector": embeddings,
        "chunk_text": chunks,
    }
