#!/usr/bin/env python3
"""Seed cluster MLflow with Parquet/CSV artifacts and optional GenAI dataset."""

from __future__ import annotations

import os
import pickle
import tempfile
from pathlib import Path

import mlflow
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from _common import ROOT, require_mlflow_env, sample_frame, save_seed_state

EXPERIMENT = "feast_mlflow_datasource_e2e"


def main() -> None:
    require_mlflow_env()
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment(EXPERIMENT)

    df = sample_frame()
    state: dict = {}

    with mlflow.start_run(run_name="feast_e2e_parquet") as run:
        with tempfile.TemporaryDirectory() as tmp:
            parquet_path = Path(tmp) / "features.parquet"
            pq.write_table(pa.Table.from_pandas(df), parquet_path)
            mlflow.log_artifact(str(parquet_path), artifact_path="outputs")
        state["parquet_run_id"] = run.info.run_id
        state["parquet_artifact_path"] = "outputs/features.parquet"

    with mlflow.start_run(run_name="feast_e2e_csv") as run:
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "features.csv"
            pacsv.write_csv(pa.Table.from_pandas(df), csv_path)
            mlflow.log_artifact(str(csv_path), artifact_path="outputs")
        state["csv_run_id"] = run.info.run_id
        state["csv_artifact_path"] = "outputs/features.csv"

    with mlflow.start_run(run_name="feast_e2e_pickle") as run:
        with tempfile.TemporaryDirectory() as tmp:
            pkl_path = Path(tmp) / "model.pkl"
            pkl_path.write_bytes(pickle.dumps({"not": "tabular"}))
            mlflow.log_artifact(str(pkl_path), artifact_path="outputs")
        state["pickle_run_id"] = run.info.run_id
        state["pickle_artifact_path"] = "outputs/model.pkl"

    try:
        from mlflow.genai.datasets import create_dataset

        dataset = create_dataset(
            name="feast_e2e_genai",
            experiment_id=mlflow.get_experiment_by_name(EXPERIMENT).experiment_id,
            tags={"source": "feast-e2e"},
        )
        state["genai_dataset_name"] = dataset.name
    except Exception as exc:
        print(f"WARN: GenAI dataset not created ({exc})")

    save_seed_state(state)
    print(f"Seeded MLflow experiment '{EXPERIMENT}':")
    for key, value in state.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
