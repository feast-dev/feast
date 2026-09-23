"""FeatureView definitions for MLflow DataSource E2E (import from test scripts)."""

from __future__ import annotations

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource
from feast.infra.data_sources.mlflow import MlflowDatasetSource
from feast.types import Float64, String
from feast.value_type import ValueType

from _common import ROOT, load_seed_state

STATE = load_seed_state()

BATCH = FileSource(
    path=str(ROOT / "data" / "batch_sink.parquet"),
    timestamp_field="event_timestamp",
)

ENTITY = Entity(
    name="record_id",
    join_keys=["record_id"],
    value_type=ValueType.STRING,
)


def parquet_feature_view() -> FeatureView:
    return FeatureView(
        name="mlflow_parquet_features",
        entities=[ENTITY],
        schema=[
            Field(name="score", dtype=Float64),
            Field(name="category", dtype=String),
        ],
        source=MlflowDatasetSource(
            name="parquet_src",
            run_id=STATE["parquet_run_id"],
            artifact_path=STATE["parquet_artifact_path"],
            artifact_format="parquet",
            batch_source=BATCH,
            timestamp_field="event_timestamp",
        ),
        ttl=timedelta(days=1),
    )


def csv_feature_view() -> FeatureView:
    return FeatureView(
        name="mlflow_csv_features",
        entities=[ENTITY],
        schema=[
            Field(name="score", dtype=Float64),
            Field(name="category", dtype=String),
        ],
        source=MlflowDatasetSource(
            name="csv_src",
            run_id=STATE["csv_run_id"],
            artifact_path=STATE["csv_artifact_path"],
            artifact_format="csv",
            batch_source=BATCH,
            timestamp_field="event_timestamp",
        ),
        ttl=timedelta(days=1),
    )


def file_baseline_feature_view() -> FeatureView:
    return FeatureView(
        name="file_baseline_features",
        entities=[ENTITY],
        schema=[
            Field(name="score", dtype=Float64),
            Field(name="category", dtype=String),
        ],
        source=BATCH,
        ttl=timedelta(days=1),
    )


def genai_feature_view() -> FeatureView | None:
    name = STATE.get("genai_dataset_name")
    if not name:
        return None
    return FeatureView(
        name="mlflow_genai_features",
        entities=[ENTITY],
        schema=[Field(name="category", dtype=String)],
        source=MlflowDatasetSource(
            name="genai_src",
            dataset_name=name,
            batch_source=BATCH,
            timestamp_field="event_timestamp",
        ),
        ttl=timedelta(days=1),
    )
