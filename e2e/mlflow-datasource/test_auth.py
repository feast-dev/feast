#!/usr/bin/env python3
"""Auth E2E: valid token, invalid token, coexistence env vars."""

from __future__ import annotations

import os
import sys
from feast import FeatureStore
from feast.infra.data_sources.mlflow.auth import MlflowAuthError

from _common import ROOT, load_seed_state, require_mlflow_env, sample_frame
from definitions import parquet_feature_view

os.chdir(ROOT)
require_mlflow_env()
if "parquet_run_id" not in load_seed_state():
    print("ERROR: Run seed_mlflow.py first.", file=sys.stderr)
    sys.exit(1)

token = os.environ.get("MLFLOW_TRACKING_TOKEN")
if not token:
    print("WARN: MLFLOW_TRACKING_TOKEN not set; skipping auth E2E")
    sys.exit(0)

store = FeatureStore(repo_path=str(ROOT))
store.apply([parquet_feature_view()])
entity_df = sample_frame()[["record_id", "event_timestamp"]]

os.environ["MLFLOW_TRACKING_AUTH"] = "kubernetes-namespaced"
os.environ["MLFLOW_TRACKING_TOKEN"] = token
job = store.get_historical_features(
    entity_df=entity_df,
    features=["mlflow_parquet_features:score"],
)
assert len(job.to_df()) == 3

os.environ["MLFLOW_TRACKING_TOKEN"] = "invalid-token-for-e2e"
try:
    store.get_historical_features(
        entity_df=entity_df,
        features=["mlflow_parquet_features:score"],
    ).to_df()
    print("ERROR: expected auth failure with bad token", file=sys.stderr)
    sys.exit(1)
except (MlflowAuthError, Exception) as exc:
    msg = str(exc).lower()
    if not isinstance(exc, MlflowAuthError) and "401" not in msg and "403" not in msg:
        raise

os.environ["MLFLOW_TRACKING_TOKEN"] = token
print("test_auth: OK")
