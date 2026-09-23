#!/usr/bin/env python3
"""Backwards compatibility: FileSource views unaffected by MLflow views."""

from __future__ import annotations

import os
import sys

import pyarrow.parquet as pq

from _common import ROOT, load_seed_state, require_mlflow_env, sample_frame
from definitions import csv_feature_view, file_baseline_feature_view, parquet_feature_view

os.chdir(ROOT)
require_mlflow_env()
if "parquet_run_id" not in load_seed_state():
    print("ERROR: Run seed_mlflow.py first.", file=sys.stderr)
    sys.exit(1)

data_dir = ROOT / "data"
data_dir.mkdir(exist_ok=True)
pq.write_table(
    __import__("pyarrow").Table.from_pandas(sample_frame()),
    data_dir / "batch_sink.parquet",
)

from feast import FeatureStore

store = FeatureStore(repo_path=str(ROOT))
entity_df = sample_frame()[["record_id", "event_timestamp"]]

store.apply([file_baseline_feature_view()])
baseline = store.get_historical_features(
    entity_df=entity_df,
    features=["file_baseline_features:score"],
).to_df()

store.apply(
    [file_baseline_feature_view(), parquet_feature_view(), csv_feature_view()]
)
baseline_after = store.get_historical_features(
    entity_df=entity_df,
    features=["file_baseline_features:score"],
).to_df()

assert baseline["score"].tolist() == baseline_after["score"].tolist()
print("test_backwards_compat: OK")
