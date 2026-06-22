from pathlib import Path

import pandas as pd
import pytest

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


def _write_chronon_parquet(tmp_path: Path) -> Path:
    data_path = tmp_path / "chronon.parquet"
    pd.DataFrame(
        {
            "user_id": [1],
            "event_timestamp": [pd.Timestamp("2024-01-01T00:00:00Z")],
            "feature_a": [0.5],
        }
    ).to_parquet(data_path)
    return data_path


def _repo_config(tmp_path: Path) -> RepoConfig:
    config = RepoConfig(
        project="test",
        registry=RegistryConfig(path=str(tmp_path / "registry.db")),
        provider="chronon",
        offline_store={"type": "chronon"},
        online_store=ChrononOnlineStoreConfig(path="http://chronon.test"),
    )
    config.repo_path = tmp_path
    return config


def _feature_view(source: ChrononSource) -> FeatureView:
    return FeatureView(
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


def _entity_keys() -> list[EntityKeyProto]:
    return [
        EntityKeyProto(
            join_keys=["user_id"],
            entity_values=[ValueProto(int64_val=1)],
        ),
        EntityKeyProto(
            join_keys=["user_id"],
            entity_values=[ValueProto(int64_val=2)],
        ),
    ]


def test_chronon_online_store_maps_success_and_failure(monkeypatch, tmp_path: Path):
    data_path = _write_chronon_parquet(tmp_path)
    config = _repo_config(tmp_path)

    source = ChrononSource(
        materialization_path=str(data_path),
        chronon_join="team/training_set.v1",
        timestamp_field="event_timestamp",
    )
    feature_view = _feature_view(source)

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
    entity_keys = _entity_keys()

    rows = store.online_read(
        config, feature_view, entity_keys, requested_features=["feature_a"]
    )

    assert captured["url"].endswith("/v1/features/join/team%2Ftraining_set.v1")
    assert captured["json"] == [{"user_id": 1}, {"user_id": 2}]
    assert rows[0][1] is not None
    assert abs(rows[0][1]["feature_a"].double_val - 0.5) < 1e-6
    assert rows[1] == (None, None)


def test_chronon_online_store_builds_group_by_url(monkeypatch, tmp_path: Path):
    data_path = _write_chronon_parquet(tmp_path)
    config = _repo_config(tmp_path)
    source = ChrononSource(
        materialization_path=str(data_path),
        chronon_group_by="team/user_features.v1",
        timestamp_field="event_timestamp",
    )
    feature_view = _feature_view(source)
    captured = {}

    class _Session:
        def post(self, url, json, timeout, verify):
            captured["url"] = url
            return _Response(
                {
                    "results": [
                        {"status": "Success", "features": {"feature_a": 0.5}},
                        {"status": "Success", "features": {"feature_a": 1.0}},
                    ]
                }
            )

    monkeypatch.setattr(
        "feast.infra.online_stores.chronon_online_store.chronon.HttpSessionManager.get_session",
        lambda config, **kwargs: _Session(),
    )

    ChrononOnlineStore().online_read(
        config,
        feature_view,
        _entity_keys(),
        requested_features=["feature_a"],
    )

    assert captured["url"].endswith("/v1/features/groupby/team%2Fuser_features.v1")


def test_chronon_online_store_rejects_mismatched_result_count(
    monkeypatch, tmp_path: Path
):
    data_path = _write_chronon_parquet(tmp_path)
    config = _repo_config(tmp_path)
    source = ChrononSource(
        materialization_path=str(data_path),
        chronon_join="team/training_set.v1",
        timestamp_field="event_timestamp",
    )
    feature_view = _feature_view(source)

    class _Session:
        def post(self, url, json, timeout, verify):
            return _Response(
                {"results": [{"status": "Success", "features": {"feature_a": 0.5}}]}
            )

    monkeypatch.setattr(
        "feast.infra.online_stores.chronon_online_store.chronon.HttpSessionManager.get_session",
        lambda config, **kwargs: _Session(),
    )

    with pytest.raises(RuntimeError, match="returned 1 rows for 2 entity keys"):
        ChrononOnlineStore().online_read(
            config,
            feature_view,
            _entity_keys(),
            requested_features=["feature_a"],
        )


def test_chronon_online_store_rejects_invalid_response_shape(
    monkeypatch, tmp_path: Path
):
    data_path = _write_chronon_parquet(tmp_path)
    config = _repo_config(tmp_path)
    source = ChrononSource(
        materialization_path=str(data_path),
        chronon_join="team/training_set.v1",
        timestamp_field="event_timestamp",
    )
    feature_view = _feature_view(source)

    class _Session:
        def post(self, url, json, timeout, verify):
            return _Response({"results": {"status": "Success"}})

    monkeypatch.setattr(
        "feast.infra.online_stores.chronon_online_store.chronon.HttpSessionManager.get_session",
        lambda config, **kwargs: _Session(),
    )

    with pytest.raises(RuntimeError, match="expected `results` to be a list"):
        ChrononOnlineStore().online_read(
            config,
            feature_view,
            _entity_keys(),
            requested_features=["feature_a"],
        )


def test_chronon_online_store_rejects_invalid_result_row(monkeypatch, tmp_path: Path):
    data_path = _write_chronon_parquet(tmp_path)
    config = _repo_config(tmp_path)
    source = ChrononSource(
        materialization_path=str(data_path),
        chronon_join="team/training_set.v1",
        timestamp_field="event_timestamp",
    )
    feature_view = _feature_view(source)

    class _Session:
        def post(self, url, json, timeout, verify):
            return _Response({"results": ["not-a-row", "still-not-a-row"]})

    monkeypatch.setattr(
        "feast.infra.online_stores.chronon_online_store.chronon.HttpSessionManager.get_session",
        lambda config, **kwargs: _Session(),
    )

    with pytest.raises(RuntimeError, match="rows must be JSON objects"):
        ChrononOnlineStore().online_read(
            config,
            feature_view,
            _entity_keys(),
            requested_features=["feature_a"],
        )


def test_chronon_online_store_rejects_invalid_features_payload(
    monkeypatch, tmp_path: Path
):
    data_path = _write_chronon_parquet(tmp_path)
    config = _repo_config(tmp_path)
    source = ChrononSource(
        materialization_path=str(data_path),
        chronon_join="team/training_set.v1",
        timestamp_field="event_timestamp",
    )
    feature_view = _feature_view(source)

    class _Session:
        def post(self, url, json, timeout, verify):
            return _Response(
                {
                    "results": [
                        {"status": "Success", "features": ["not", "an", "object"]},
                        {"status": "Success", "features": {"feature_a": 1.0}},
                    ]
                }
            )

    monkeypatch.setattr(
        "feast.infra.online_stores.chronon_online_store.chronon.HttpSessionManager.get_session",
        lambda config, **kwargs: _Session(),
    )

    with pytest.raises(RuntimeError, match="`features` to be a JSON object"):
        ChrononOnlineStore().online_read(
            config,
            feature_view,
            _entity_keys(),
            requested_features=["feature_a"],
        )
