from pathlib import Path

import pandas as pd
import pytest

from feast import Entity, FeatureStore, FeatureView, Field, RepoConfig
from feast.infra.offline_stores.contrib.chronon_offline_store.chronon_source import (
    ChrononSource,
)
from feast.repo_config import RegistryConfig
from feast.types import Float32
from feast.value_type import ValueType


@pytest.mark.integration
def test_chronon_offline_store_historical_retrieval(tmp_path: Path):
    data_path = tmp_path / "chronon.parquet"
    pd.DataFrame(
        {
            "user_id": [1, 2],
            "event_timestamp": [
                pd.Timestamp("2024-01-01T00:00:00Z"),
                pd.Timestamp("2024-01-02T00:00:00Z"),
            ],
            "feature_a": [2.0, 4.0],
        }
    ).to_parquet(data_path)

    config = RepoConfig(
        project="test",
        registry=RegistryConfig(path=str(tmp_path / "registry.db")),
        provider="chronon",
        offline_store={"type": "chronon"},
        online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
    )
    config.repo_path = tmp_path

    store = FeatureStore(config=config)
    user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
    fv = FeatureView(
        name="fraud_profile",
        entities=[user],
        schema=[Field(name="feature_a", dtype=Float32)],
        source=ChrononSource(
            materialization_path=str(data_path),
            chronon_join="team/training_set.v1",
            timestamp_field="event_timestamp",
        ),
        offline=True,
    )
    store.apply([user, fv])

    entity_df = pd.DataFrame(
        {
            "user_id": [1, 2],
            "event_timestamp": [
                pd.Timestamp("2024-01-01T00:00:00Z"),
                pd.Timestamp("2024-01-02T00:00:00Z"),
            ],
        }
    )

    result = store.get_historical_features(
        entity_df=entity_df,
        features=["fraud_profile:feature_a"],
    ).to_df()

    assert result["feature_a"].tolist() == [2.0, 4.0]


@pytest.mark.integration
def test_chronon_offline_store_non_entity_retrieval(tmp_path: Path):
    data_path = tmp_path / "chronon.parquet"
    pd.DataFrame(
        {
            "user_id": [1, 2],
            "event_timestamp": [
                pd.Timestamp("2024-01-01T00:00:00Z"),
                pd.Timestamp("2024-01-02T00:00:00Z"),
            ],
            "feature_a": [2.0, 4.0],
        }
    ).to_parquet(data_path)

    config = RepoConfig(
        project="test",
        registry=RegistryConfig(path=str(tmp_path / "registry.db")),
        provider="chronon",
        offline_store={"type": "chronon"},
        online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
    )
    config.repo_path = tmp_path

    store = FeatureStore(config=config)
    user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
    fv = FeatureView(
        name="fraud_profile",
        entities=[user],
        schema=[Field(name="feature_a", dtype=Float32)],
        source=ChrononSource(
            materialization_path=str(data_path),
            chronon_join="team/training_set.v1",
            timestamp_field="event_timestamp",
        ),
        offline=True,
    )
    store.apply([user, fv])

    result = store.get_historical_features(
        features=["fraud_profile:feature_a"],
        start_date=pd.Timestamp("2024-01-01T00:00:00Z").to_pydatetime(),
        end_date=pd.Timestamp("2024-01-01T23:59:59Z").to_pydatetime(),
    ).to_df()

    assert result["feature_a"].tolist() == [2.0]
