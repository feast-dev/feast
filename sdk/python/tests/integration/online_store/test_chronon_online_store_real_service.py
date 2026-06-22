import os
from pathlib import Path

import pandas as pd
import pytest

from feast import Entity, FeatureStore, FeatureView, Field, RepoConfig
from feast.infra.offline_stores.contrib.chronon_offline_store.chronon_source import (
    ChrononSource,
)
from feast.repo_config import RegistryConfig
from feast.types import Int64
from feast.value_type import ValueType


@pytest.mark.integration
def test_chronon_online_store_real_service(tmp_path: Path):
    service_url = os.environ.get("CHRONON_SERVICE_URL")
    if not service_url:
        pytest.skip("Set CHRONON_SERVICE_URL to run against a live Chronon service.")

    data_path = tmp_path / "placeholder.parquet"
    pd.DataFrame(
        {
            "event_timestamp": [pd.Timestamp("2024-01-01T00:00:00Z")],
        }
    ).to_parquet(data_path)

    config = RepoConfig(
        project="test",
        registry=RegistryConfig(path=str(tmp_path / "registry.db")),
        provider="chronon",
        offline_store={"type": "chronon"},
        online_store={"type": "chronon", "path": service_url},
    )
    config.repo_path = tmp_path

    store = FeatureStore(config=config)
    user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.STRING)
    fv = FeatureView(
        name="training_set",
        entities=[user],
        schema=[
            Field(name="quickstart_purchases_v1_purchase_price_sum_30d", dtype=Int64),
            Field(name="quickstart_returns_v1_refund_amt_sum_30d", dtype=Int64),
        ],
        source=ChrononSource(
            materialization_path=str(data_path),
            chronon_join="quickstart/training_set.v2",
            timestamp_field="event_timestamp",
            online_endpoint=service_url,
        ),
        online=True,
        offline=False,
    )
    store.apply([user, fv])

    response = store.get_online_features(
        features=[
            "training_set:quickstart_purchases_v1_purchase_price_sum_30d",
            "training_set:quickstart_returns_v1_refund_amt_sum_30d",
        ],
        entity_rows=[{"user_id": "5"}],
    ).to_dict()

    assert response["quickstart_purchases_v1_purchase_price_sum_30d"] == [1253]
    assert response["quickstart_returns_v1_refund_amt_sum_30d"] == [1269]
