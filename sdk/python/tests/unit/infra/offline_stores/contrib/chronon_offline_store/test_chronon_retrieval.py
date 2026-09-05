from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

from feast import ChrononSource, Entity, FeatureStore, FeatureView, Field, RepoConfig
from feast.types import Float32


def _store(tmp_path: Path, ttl: timedelta, partitioned: bool = False) -> FeatureStore:
    data_path = tmp_path / "materialization"
    pd.DataFrame(
        {
            "user_id": [1, 2],
            "event_timestamp": pd.to_datetime(["2024-01-01", "2024-01-01"], utc=True),
            "feature_a": [2.0, 4.0],
        }
    ).to_parquet(data_path, partition_cols=["user_id"] if partitioned else None)
    config = RepoConfig(
        project="test",
        registry=str(tmp_path / "registry.db"),
        provider="chronon",
        offline_store={"type": "chronon"},
        online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
    )
    config.repo_path = tmp_path
    store = FeatureStore(config=config)
    user = Entity(name="user", join_keys=["user_id"])
    view = FeatureView(
        name="profile",
        entities=[user],
        ttl=ttl,
        schema=[Field(name="feature_a", dtype=Float32)],
        source=ChrononSource(
            materialization_path=str(data_path), timestamp_field="event_timestamp"
        ),
    )
    store.apply([user, view])
    return store


@pytest.mark.parametrize("ttl", [timedelta(days=1), timedelta(0)])
def test_historical_retrieval_honors_ttl(tmp_path: Path, ttl: timedelta) -> None:
    store = _store(tmp_path, ttl)
    entity_df = pd.DataFrame(
        {
            "user_id": [1, 1, 1],
            "event_timestamp": pd.to_datetime(
                ["2024-01-02", "2024-01-02 00:00:01", "2024-01-10"],
                format="mixed",
                utc=True,
            ),
        }
    )
    result = store.get_historical_features(
        entity_df=entity_df, features=["profile:feature_a"]
    ).to_df()
    assert result["feature_a"].iloc[0] == 2.0
    if ttl:
        assert result["feature_a"].iloc[1:].isna().all()
    else:
        assert result["feature_a"].tolist() == [2.0, 2.0, 2.0]


def test_historical_retrieval_infers_entity_timestamp(tmp_path: Path) -> None:
    store = _store(tmp_path, timedelta(days=1))
    entity_df = pd.DataFrame(
        {"user_id": [1], "request_time": pd.to_datetime(["2024-01-02"], utc=True)}
    )
    result = store.get_historical_features(
        entity_df=entity_df, features=["profile:feature_a"]
    ).to_df()
    assert result["feature_a"].tolist() == [2.0]
    pd.testing.assert_series_equal(result["request_time"], entity_df["request_time"])


def test_historical_retrieval_infers_partitioned_entity(tmp_path: Path) -> None:
    store = _store(tmp_path, timedelta(days=1), partitioned=True)
    assert store.get_feature_view("profile").join_keys == ["user_id"]
    result = store.get_historical_features(
        entity_df=pd.DataFrame(
            {
                "user_id": [2, 1],
                "event_timestamp": pd.to_datetime(
                    ["2024-01-02", "2024-01-02"], utc=True
                ),
            }
        ),
        features=["profile:feature_a"],
    ).to_df()
    assert result["feature_a"].tolist() == [4.0, 2.0]
