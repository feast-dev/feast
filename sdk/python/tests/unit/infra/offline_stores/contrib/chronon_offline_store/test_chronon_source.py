from pathlib import Path

import pandas as pd
import pytest

from feast import FeatureStore, RepoConfig
from feast.infra.offline_stores.contrib.chronon_offline_store.chronon_source import (
    ChrononSource,
)


def test_chronon_source_proto_round_trip(tmp_path: Path):
    data_path = tmp_path / "chronon.parquet"
    pd.DataFrame(
        {
            "user_id": [1],
            "event_timestamp": [pd.Timestamp("2024-01-01T00:00:00Z")],
            "feature_a": [0.5],
        }
    ).to_parquet(data_path)

    source = ChrononSource(
        name="chronon_source",
        materialization_path=str(data_path),
        chronon_join="team/training_set.v1",
        online_endpoint="http://localhost:8080",
        timestamp_field="event_timestamp",
    )

    restored = ChrononSource.from_proto(source.to_proto())

    assert restored.name == source.name
    assert restored.materialization_path == source.materialization_path
    assert restored.chronon_join == source.chronon_join
    assert restored.online_endpoint == source.online_endpoint
    assert restored.timestamp_field == source.timestamp_field


def test_chronon_source_rejects_join_and_group_by(tmp_path: Path):
    data_path = tmp_path / "chronon.parquet"
    pd.DataFrame(
        {
            "user_id": [1],
            "event_timestamp": [pd.Timestamp("2024-01-01T00:00:00Z")],
            "feature_a": [0.5],
        }
    ).to_parquet(data_path)

    with pytest.raises(
        ValueError,
        match="exactly one of `chronon_join` or `chronon_group_by`",
    ):
        ChrononSource(
            materialization_path=str(data_path),
            chronon_join="team/training_set.v1",
            chronon_group_by="team/user_features.v1",
            timestamp_field="event_timestamp",
        )


def test_chronon_source_requires_chronon_object_for_online_endpoint(tmp_path: Path):
    data_path = tmp_path / "chronon.parquet"
    pd.DataFrame(
        {
            "user_id": [1],
            "event_timestamp": [pd.Timestamp("2024-01-01T00:00:00Z")],
            "feature_a": [0.5],
        }
    ).to_parquet(data_path)

    with pytest.raises(
        ValueError,
        match="`online_endpoint` requires `chronon_join` or `chronon_group_by`",
    ):
        ChrononSource(
            materialization_path=str(data_path),
            online_endpoint="http://localhost:8080",
            timestamp_field="event_timestamp",
        )


def test_chronon_source_requires_timestamp_field(tmp_path: Path):
    data_path = tmp_path / "chronon.parquet"
    pd.DataFrame(
        {
            "user_id": [1],
            "event_timestamp": [pd.Timestamp("2024-01-01T00:00:00Z")],
            "feature_a": [0.5],
        }
    ).to_parquet(data_path)

    with pytest.raises(ValueError, match="requires `timestamp_field`"):
        ChrononSource(
            materialization_path=str(data_path),
            chronon_join="team/training_set.v1",
        )


def test_chronon_source_requires_materialization_path():
    with pytest.raises(ValueError, match="requires `materialization_path`"):
        ChrononSource(
            name="chronon_source",
            materialization_path="",
            chronon_join="team/training_set.v1",
            timestamp_field="event_timestamp",
        )


@pytest.mark.parametrize(
    "option,initial,updated",
    [
        ("materialization_path", "old.parquet", "new.parquet"),
        ("chronon_join", "team/old.v1", "team/new.v2"),
        ("chronon_group_by", "team/old.v1", "team/new.v2"),
        ("online_endpoint", "http://old.test", "http://new.test"),
    ],
)
def test_apply_updates_chronon_source_options(
    tmp_path: Path, option: str, initial: str, updated: str
) -> None:
    config = RepoConfig(
        project="test",
        registry=str(tmp_path / "registry.db"),
        provider="chronon",
        offline_store={"type": "chronon"},
        online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
    )
    config.repo_path = tmp_path
    store = FeatureStore(config=config)
    for value in [initial, updated]:
        options = {
            "name": "source",
            "materialization_path": str(tmp_path / "source.parquet"),
            "timestamp_field": "event_timestamp",
            "chronon_join": "team/features.v1",
        }
        if option == "chronon_group_by":
            options.pop("chronon_join")
        options[option] = value
        source = ChrononSource(**options)
        store.apply([source])
    assert getattr(store.get_data_source("source"), option) == updated
    assert hash(source) == hash(store.get_data_source("source"))
