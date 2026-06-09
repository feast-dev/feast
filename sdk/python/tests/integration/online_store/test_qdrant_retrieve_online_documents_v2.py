"""Integration tests for Qdrant retrieve_online_documents_v2 (local path store)."""

from __future__ import annotations

import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("qdrant_client")

from feast import Entity, FeatureStore, FeatureView, Field, FileSource
from feast.types import Array, Float32, Int64, String
from feast.value_type import ValueType


@pytest.mark.integration
def test_qdrant_retrieve_online_documents_v2_dense() -> None:
    """End-to-end: materialize to Qdrant and run dense v2 retrieval."""
    repo_dir = Path(tempfile.mkdtemp(prefix="feast_qdrant_v2_"))
    qdrant_path = repo_dir / "qdrant_data"
    parquet_path = repo_dir / "docs.parquet"
    registry_path = repo_dir / "registry.db"

    vector_dim = 3
    n_rows = 10
    rng = np.random.default_rng(42)

    df = pd.DataFrame(
        {
            "doc_id": list(range(n_rows)),
            "embedding": [rng.random(vector_dim).tolist() for _ in range(n_rows)],
            "text_field": [f"document {i} feast qdrant test" for i in range(n_rows)],
            "category": [f"cat-{i % 3}" for i in range(n_rows)],
            "event_timestamp": [datetime.now(UTC) for _ in range(n_rows)],
        }
    )
    df.to_parquet(parquet_path)

    feature_store_yaml = f"""
project: qdrant_v2_test
registry: {registry_path}
provider: local
entity_key_serialization_version: 3
online_store:
  type: qdrant
  path: {qdrant_path}
  similarity: cosine
  vector_enabled: true
  text_search_enabled: false
"""
    (repo_dir / "feature_store.yaml").write_text(feature_store_yaml.strip() + "\n")

    try:
        item = Entity(name="doc_id", join_keys=["doc_id"], value_type=ValueType.INT64)
        documents_fv = FeatureView(
            name="documents",
            entities=[item],
            schema=[
                Field(
                    name="embedding",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=vector_dim,
                ),
                Field(name="text_field", dtype=String),
                Field(name="category", dtype=String),
                Field(name="doc_id", dtype=Int64),
            ],
            source=FileSource(
                path=str(parquet_path),
                timestamp_field="event_timestamp",
            ),
        )

        store = FeatureStore(repo_path=str(repo_dir))
        store.apply([item, documents_fv])
        store.write_to_online_store("documents", df)

        query_embedding = rng.random(vector_dim).tolist()
        response = store.retrieve_online_documents_v2(
            features=[
                "documents:embedding",
                "documents:text_field",
                "documents:category",
                "documents:doc_id",
            ],
            query=query_embedding,
            top_k=3,
            distance_metric="cosine",
        )
        result = response.to_dict()

        assert len(result["embedding"]) == 3
        assert len(result["distance"]) == 3
        assert len(result["text_field"]) == 3
        assert len(result["category"]) == 3
        assert len(result["doc_id"]) == 3
        assert all(isinstance(d, float) for d in result["distance"])
        assert all(
            isinstance(t, str) and t.startswith("document")
            for t in result["text_field"]
        )
        assert all(
            isinstance(c, str) and c.startswith("cat-") for c in result["category"]
        )
        assert all(isinstance(i, int) for i in result["doc_id"])
        assert all(
            isinstance(e, list) and len(e) == vector_dim for e in result["embedding"]
        )
    finally:
        shutil.rmtree(repo_dir, ignore_errors=True)


@pytest.mark.integration
def test_qdrant_retrieve_online_documents_v2_query_string_requires_flag() -> None:
    """query_string without text_search_enabled should fail clearly."""
    repo_dir = Path(tempfile.mkdtemp(prefix="feast_qdrant_v2_flag_"))
    qdrant_path = repo_dir / "qdrant_data"
    registry_path = repo_dir / "registry.db"

    feature_store_yaml = f"""
project: qdrant_v2_flag_test
registry: {registry_path}
provider: local
entity_key_serialization_version: 3
online_store:
  type: qdrant
  path: {qdrant_path}
  similarity: cosine
  vector_enabled: true
  text_search_enabled: false
"""
    (repo_dir / "feature_store.yaml").write_text(feature_store_yaml.strip() + "\n")

    try:
        item = Entity(name="doc_id", join_keys=["doc_id"], value_type=ValueType.INT64)
        documents_fv = FeatureView(
            name="documents",
            entities=[item],
            schema=[
                Field(
                    name="embedding",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=3,
                ),
                Field(name="text_field", dtype=String),
            ],
            source=FileSource(path="dummy.parquet", timestamp_field="event_timestamp"),
        )
        store = FeatureStore(repo_path=str(repo_dir))
        store.apply([item, documents_fv])

        with pytest.raises(ValueError, match="text_search_enabled"):
            store.retrieve_online_documents_v2(
                features=["documents:text_field"],
                query_string="feast",
                top_k=1,
            )
    finally:
        shutil.rmtree(repo_dir, ignore_errors=True)
