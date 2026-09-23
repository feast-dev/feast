#!/usr/bin/env python3
"""Happy-path E2E: retrieval, saved dataset, optional sync."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pyarrow.parquet as pq

from _common import ROOT, load_seed_state, require_mlflow_env, sample_frame
from definitions import (
    ENTITY,
    csv_feature_view,
    file_baseline_feature_view,
    genai_feature_view,
    parquet_feature_view,
)

os.chdir(ROOT)
require_mlflow_env()
state = load_seed_state()
if "parquet_run_id" not in state:
    print("ERROR: Run seed_mlflow.py first.", file=sys.stderr)
    sys.exit(1)

data_dir = ROOT / "data"
data_dir.mkdir(exist_ok=True)
pq.write_table(
    __import__("pyarrow").Table.from_pandas(sample_frame()),
    data_dir / "batch_sink.parquet",
)

from feast import FeatureStore
from feast.data_format import ParquetFormat
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

store = FeatureStore(repo_path=str(ROOT))
views = [parquet_feature_view(), csv_feature_view(), file_baseline_feature_view()]
genai_fv = genai_feature_view()
if genai_fv:
    views.append(genai_fv)
store.apply(views)

entity_df = sample_frame()[["record_id", "event_timestamp"]]

job = store.get_historical_features(
    entity_df=entity_df,
    features=["mlflow_parquet_features:score", "mlflow_parquet_features:category"],
)
result_df = job.to_df()
assert len(result_df) == 3, "Parquet artifact retrieval row count"

job_csv = store.get_historical_features(
    entity_df=entity_df,
    features=["mlflow_csv_features:score"],
)
assert len(job_csv.to_df()) == 3, "CSV artifact retrieval row count"

storage = SavedDatasetFileStorage(
    path=str(data_dir / "saved_roundtrip.parquet"),
    file_format=ParquetFormat(),
)
store.registry.apply_data_source(storage.to_data_source(), store.config.project)
saved = store.create_saved_dataset(
    from_=job,
    name="e2e_saved",
    storage=storage,
    allow_overwrite=True,
)
loaded = store.get_saved_dataset("e2e_saved")
assert loaded.to_df().shape[0] == saved.to_df().shape[0]

print("test_happy_path: OK")
