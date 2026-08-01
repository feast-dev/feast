#!/usr/bin/env python3
"""Manual smoke test for Qdrant retrieve_online_documents_v2.

Run from repo root:
  uv run python sdk/python/tests/integration/online_store/manual_qdrant_v2.py
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import qdrant_client  # noqa: F401
except ImportError:
    print("Install qdrant: pip install 'feast[qdrant]'")
    sys.exit(1)

from feast import Entity, FeatureStore, FeatureView, Field, FileSource
from feast.types import Array, Float32, Int64, String
from feast.value_type import ValueType


def main() -> None:
    repo_dir = Path(tempfile.mkdtemp(prefix="feast_qdrant_manual_"))
    print(f"Repo dir: {repo_dir}")

    vector_dim = 3
    n_rows = 5
    rng = np.random.default_rng(0)

    parquet_path = repo_dir / "docs.parquet"
    df = pd.DataFrame(
        {
            "doc_id": list(range(n_rows)),
            "embedding": [rng.random(vector_dim).tolist() for _ in range(n_rows)],
            "text_field": [f"manual test document {i}" for i in range(n_rows)],
            "event_timestamp": [datetime.now(UTC) for _ in range(n_rows)],
        }
    )
    df.to_parquet(parquet_path)

    (repo_dir / "feature_store.yaml").write_text(
        f"""
project: manual_qdrant
registry: {repo_dir / "registry.db"}
provider: local
entity_key_serialization_version: 3
online_store:
  type: qdrant
  path: {repo_dir / "qdrant_data"}
  similarity: cosine
  vector_enabled: true
  text_search_enabled: false
""".strip()
        + "\n"
    )

    try:
        item = Entity(name="doc_id", join_keys=["doc_id"], value_type=ValueType.INT64)
        fv = FeatureView(
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
                Field(name="doc_id", dtype=Int64),
            ],
            source=FileSource(
                path=str(parquet_path), timestamp_field="event_timestamp"
            ),
        )

        store = FeatureStore(repo_path=str(repo_dir))
        print("Applying feature view...")
        store.apply([item, fv])

        print("Writing to online store...")
        store.write_to_online_store("documents", df)

        query = rng.random(vector_dim).tolist()
        print(f"Query embedding: {[round(x, 4) for x in query]}")

        print("retrieve_online_documents_v2 (top_k=3)...")
        response = store.retrieve_online_documents_v2(
            features=[
                "documents:embedding",
                "documents:text_field",
                "documents:doc_id",
            ],
            query=query,
            top_k=3,
            distance_metric="cosine",
        )
        result = response.to_dict()

        print("\n--- Results ---")
        for i in range(len(result["doc_id"])):
            print(
                f"  [{i}] doc_id={result['doc_id'][i]} "
                f"distance={result['distance'][i]:.4f} "
                f"text={result['text_field'][i]!r}"
            )

        assert len(result["doc_id"]) == 3
        assert all(isinstance(x, str) for x in result["text_field"])
        print("\nManual Qdrant v2 smoke test PASSED")
    finally:
        shutil.rmtree(repo_dir, ignore_errors=True)
        print(f"Cleaned up {repo_dir}")


if __name__ == "__main__":
    main()
