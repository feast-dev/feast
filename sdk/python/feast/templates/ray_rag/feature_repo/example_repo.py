"""
RAG Feature Repository - Movie Embeddings with Ray Native Processing

This template demonstrates distributed embedding generation using:
- Ray offline store for scalable data I/O
- Ray compute engine for parallel processing
- Milvus online store with vector search capabilities
"""

from datetime import timedelta
from pathlib import Path

import pandas as pd

from feast import BatchFeatureView, Entity, Field, FileSource, ValueType
from feast.types import Array, Float32, String

# Configuration
repo_path = Path(__file__).parent
data_path = repo_path / "data"
data_path.mkdir(exist_ok=True)

EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"

# Entity definition
document = Entity(
    name="document_id",
    join_keys=["document_id_pk"],
    value_type=ValueType.STRING,
    description="Document identifier for RAG retrieval",
)

# Data source
movies_source = FileSource(
    path=str(data_path / "raw_movies.parquet"),
    timestamp_field="DatePublished",
)


# Embedding processor for distributed Ray processing
class EmbeddingProcessor:
    """
    Generate embeddings using SentenceTransformer model.
    Model is loaded once per worker and reused for all batches.
    """

    def __init__(self):
        """Initialize model once per worker."""
        import torch
        from sentence_transformers import SentenceTransformer

        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(EMBED_MODEL_ID, device=device)

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        """Process batch and generate embeddings."""
        if "id" in batch.columns:
            batch["document_id"] = "movie_" + batch["id"].astype(str)
            batch["document_id_pk"] = batch["document_id"]

        # Generate embeddings from descriptions
        if "Description" in batch.columns:
            descriptions = batch["Description"].fillna("").tolist()
            model_batch_size = min(128, max(32, len(descriptions)))
            embeddings = self.model.encode(
                descriptions,
                show_progress_bar=False,
                batch_size=model_batch_size,
                normalize_embeddings=True,
                convert_to_numpy=True,
            )
            batch["embedding"] = embeddings.tolist()
            batch["embedding_model"] = EMBED_MODEL_ID

        # Standardize movie-related metadata
        if "Name" in batch.columns:
            batch["movie_name"] = batch["Name"].fillna("")
        if "Director" in batch.columns:
            batch["movie_director"] = batch["Director"].fillna("")
        if "Genres" in batch.columns:
            batch["movie_genres"] = batch["Genres"].fillna("")
        if "RatingValue" in batch.columns:
            batch["movie_rating"] = batch["RatingValue"].fillna(0.0)

        return batch


# Ray native UDF - Fully adaptive to cluster resources
def generate_embeddings_ray_native(ds):
    """
    Distributed embedding generation using Ray Data.
    """
    # Ray transformation mode providing control to user over the resources
    # Optimize the resources for the transformation
    num_blocks = ds.num_blocks()
    max_workers = 9
    batch_size = 2500
    try:
        sample = ds.take(1)
        has_data = len(sample) > 0
    except Exception:
        has_data = False
    if has_data and num_blocks < max_workers:
        ds = ds.repartition(max_workers)

    result = ds.map_batches(
        EmbeddingProcessor,
        batch_format="pandas",
        concurrency=max_workers,
        batch_size=batch_size,
    )
    return result


# Batch feature view for embeddings with metadata
document_embeddings_view = BatchFeatureView(
    name="document_embeddings",
    entities=[document],
    mode="ray",  # Native Ray Dataset mode for distributed processing
    ttl=timedelta(days=365 * 100),
    schema=[
        Field(name="document_id", dtype=String),
        Field(name="document_id_pk", dtype=String),
        Field(name="embedding", dtype=Array(Float32), vector_index=True),
        Field(name="embedding_model", dtype=String),
        Field(name="movie_name", dtype=String),
        Field(name="movie_director", dtype=String),
        Field(name="movie_genres", dtype=String),
        Field(name="movie_rating", dtype=Float32),
    ],
    source=movies_source,
    udf=generate_embeddings_ray_native,
    online=True,
    tags={
        "team": "ml_platform",
        "use_case": "rag",
        "transformation_mode": "ray_native",
    },
)
