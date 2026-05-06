from pathlib import Path

import pandas as pd

from feast import Entity, FeatureView, Field
from feast.infra.offline_stores.contrib.chronon_offline_store.chronon_source import (
    ChrononSource,
)
from feast.infra.online_stores.chronon_online_store.chronon import (
    ChrononOnlineStore,
    ChrononOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig, RepoConfig
from feast.types import Float32, Int64
from feast.value_type import ValueType


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_chronon_online_store_maps_success_and_failure(monkeypatch, tmp_path: Path):
    data_path = tmp_path / "chronon.parquet"
    pd.DataFrame(
        {
            "user_id": [1],
            "event_timestamp": [pd.Timestamp("2024-01-01T00:00:00Z")],
            "feature_a": [0.5],
        }
    ).to_parquet(data_path)

    config = RepoConfig(
        project="test",
        registry=RegistryConfig(path=str(tmp_path / "registry.db")),
        provider="chronon",
        offline_store={"type": "chronon"},
        online_store=ChrononOnlineStoreConfig(path="http://chronon.test"),
    )
    config.repo_path = tmp_path

    source = ChrononSource(
        materialization_path=str(data_path),
        chronon_join="team/training_set.v1",
        timestamp_field="event_timestamp",
    )
    feature_view = FeatureView(
        name="fraud_profile",
        entities=[
            Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
        ],
        schema=[
            Field(name="user_id", dtype=Int64),
            Field(name="feature_a", dtype=Float32),
        ],
        source=source,
    )

    captured = {}

    class _Session:
        def post(self, url, json, timeout, verify):
            captured["url"] = url
            captured["json"] = json
            return _Response(
                {
                    "results": [
                        {"status": "Success", "features": {"feature_a": 0.5}},
                        {"status": "Failure", "error": "boom"},
                    ]
                }
            )

    monkeypatch.setattr(
        "feast.infra.online_stores.chronon_online_store.chronon.HttpSessionManager.get_session",
        lambda config, **kwargs: _Session(),
    )

    store = ChrononOnlineStore()
    entity_keys = [
        EntityKeyProto(
            join_keys=["user_id"],
            entity_values=[ValueProto(int64_val=1)],
        ),
        EntityKeyProto(
            join_keys=["user_id"],
            entity_values=[ValueProto(int64_val=2)],
        ),
    ]

    rows = store.online_read(
        config, feature_view, entity_keys, requested_features=["feature_a"]
    )

    assert captured["url"].endswith("/v1/features/join/team%2Ftraining_set.v1")
    assert captured["json"] == [{"user_id": 1}, {"user_id": 2}]
    assert rows[0][1] is not None
    assert abs(rows[0][1]["feature_a"].double_val - 0.5) < 1e-6
    assert rows[1] == (None, None)
