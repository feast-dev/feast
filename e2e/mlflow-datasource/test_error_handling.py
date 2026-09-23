#!/usr/bin/env python3
"""Error-handling E2E checks (local + MLflow when available)."""

from __future__ import annotations

import os
import sys

from feast import FileSource
from feast.infra.data_sources.mlflow import MlflowDatasetSource

from _common import ROOT, load_seed_state, require_mlflow_env

os.chdir(ROOT)

batch = FileSource(path=str(ROOT / "data" / "batch_sink.parquet"), timestamp_field="event_timestamp")

try:
    MlflowDatasetSource(
        name="bad_fmt",
        run_id="run",
        artifact_path="x.pkl",
        artifact_format="pkl",
        batch_source=batch,
    )
    print("ERROR: expected ValueError for pkl format", file=sys.stderr)
    sys.exit(1)
except ValueError as exc:
    assert "parquet" in str(exc).lower() or "csv" in str(exc).lower()

require_mlflow_env()
state = load_seed_state()
if not state:
    print("WARN: skipping MLflow server errors (no seed state)")
    sys.exit(0)

from feast import Entity, FeatureStore, FeatureView, Field
from feast.infra.data_sources.mlflow.auth import MlflowArtifactNotFoundError
from feast.types import Float64
from feast.value_type import ValueType
from datetime import timedelta

from _common import sample_frame

entity = Entity(name="record_id", join_keys=["record_id"], value_type=ValueType.STRING)
bad_src = MlflowDatasetSource(
    name="missing",
    run_id="00000000000000000000000000000000",
    artifact_path="outputs/nope.parquet",
    artifact_format="parquet",
    batch_source=batch,
    timestamp_field="event_timestamp",
)
fv = FeatureView(
    name="mlflow_missing",
    entities=[entity],
    schema=[Field(name="score", dtype=Float64)],
    source=bad_src,
    ttl=timedelta(days=1),
)
store = FeatureStore(repo_path=str(ROOT))
store.apply([fv])
entity_df = sample_frame()[["record_id", "event_timestamp"]]
try:
    store.get_historical_features(
        entity_df=entity_df,
        features=["mlflow_missing:score"],
    ).to_df()
    print("ERROR: expected not-found error", file=sys.stderr)
    sys.exit(1)
except (MlflowArtifactNotFoundError, Exception) as exc:
    if not isinstance(exc, MlflowArtifactNotFoundError) and "404" not in str(exc):
        raise

print("test_error_handling: OK")
