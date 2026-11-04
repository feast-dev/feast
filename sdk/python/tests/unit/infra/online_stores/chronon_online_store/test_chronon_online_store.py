from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from feast import Entity, FeatureStore, FeatureView, Field
from feast.infra.offline_stores.contrib.chronon_offline_store.chronon_source import (
    ChrononSource,
)
from feast.infra.online_stores.chronon_online_store.chronon import (
    ChrononOnlineStore,
    ChrononOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Int64List
from feast.protos.feast.types.Value_pb2 import Map as MapProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig, RepoConfig
from feast.types import Array, FeastType, Float32, Int32, Int64, Map
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


def test_chronon_online_store_maps_success_and_missing(monkeypatch, tmp_path: Path):
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
                        {"status": "Success", "features": {"feature_a": None}},
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
    assert rows[0][1]["feature_a"] == ValueProto(float_val=0.5)
    assert rows[1] == (None, {"feature_a": ValueProto()})


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


@pytest.mark.parametrize(
    "dtype,value,expected",
    [
        (Float32, 0.5, ValueProto(float_val=0.5)),
        (Int32, 3, ValueProto(int32_val=3)),
        (Array(Int64), [], ValueProto(int64_list_val=Int64List())),
        (Map, {}, ValueProto(map_val=MapProto())),
        (Float32, None, ValueProto()),
    ],
)
def test_online_read_preserves_schema(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    dtype: FeastType,
    value: Any,
    expected: ValueProto,
) -> None:
    source = ChrononSource(
        materialization_path=str(_write_chronon_parquet(tmp_path)),
        chronon_join="team/training_set.v1",
        timestamp_field="event_timestamp",
    )
    view = _feature_view(source)
    view.features = [Field(name="feature_a", dtype=dtype)]

    class Session:
        def post(self, *args: Any, **kwargs: Any) -> _Response:
            return _Response(
                {"results": [{"status": "Success", "features": {"feature_a": value}}]}
            )

    monkeypatch.setattr(
        "feast.infra.online_stores.chronon_online_store.chronon.HttpSessionManager.get_session",
        lambda *args, **kwargs: Session(),
    )
    rows = ChrononOnlineStore().online_read(
        _repo_config(tmp_path), view, _entity_keys()[:1], ["feature_a"]
    )
    assert rows == [(None, {"feature_a": expected})]


def test_online_read_applies_source_field_mapping(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = ChrononSource(
        materialization_path=str(_write_chronon_parquet(tmp_path)),
        chronon_join="team/training_set.v1",
        timestamp_field="event_timestamp",
        field_mapping={"user_id": "customer_id", "feature_a": "renamed"},
    )
    store = FeatureStore(config=_repo_config(tmp_path))
    customer = Entity(
        name="customer", join_keys=["customer_id"], value_type=ValueType.INT64
    )
    view = FeatureView(
        name="profile",
        entities=[customer],
        schema=[Field(name="renamed", dtype=Float32)],
        source=source,
    )
    store.apply([customer, view])

    class Session:
        def post(self, url: str, json: Any, **kwargs: Any) -> _Response:
            assert json == [{"user_id": 1}]
            return _Response(
                {"results": [{"status": "Success", "features": {"feature_a": 2.0}}]}
            )

    monkeypatch.setattr(
        "feast.infra.online_stores.chronon_online_store.chronon.HttpSessionManager.get_session",
        lambda *args, **kwargs: Session(),
    )
    result = store.get_online_features(
        features=["profile:renamed"], entity_rows=[{"customer_id": 1}]
    ).to_dict()
    assert result == {"customer_id": [1], "renamed": [2.0]}


@pytest.mark.parametrize("status", ["Failure", "Unexpected", None])
def test_online_read_raises_on_unsuccessful_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, status: Any
) -> None:
    source = ChrononSource(
        materialization_path=str(_write_chronon_parquet(tmp_path)),
        chronon_join="team/training_set.v1",
        timestamp_field="event_timestamp",
    )

    class Session:
        def post(self, *args: Any, **kwargs: Any) -> _Response:
            return _Response(
                {"results": [{"status": status, "error": "backend unavailable"}]}
            )

    monkeypatch.setattr(
        "feast.infra.online_stores.chronon_online_store.chronon.HttpSessionManager.get_session",
        lambda *args, **kwargs: Session(),
    )
    with pytest.raises(RuntimeError, match="Chronon.*row 0"):
        ChrononOnlineStore().online_read(
            _repo_config(tmp_path),
            _feature_view(source),
            _entity_keys()[:1],
            ["feature_a"],
        )


@pytest.mark.parametrize("retries", [-1, 6, True, 1.5])
def test_retry_count_is_bounded(retries: Any) -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ChrononOnlineStoreConfig(connection_retries=retries)


@pytest.mark.parametrize(
    "retries,status,expected_calls,success",
    [
        (0, 503, 1, False),
        (1, 503, 2, True),
        (1, 400, 1, False),
        (1, 429, 2, True),
        (1, 502, 2, True),
        (1, 504, 2, True),
        (1, 500, 2, True),
        (2, 503, 3, False),
    ],
)
def test_online_read_http_retry_policy(
    tmp_path: Path, retries: int, status: int, expected_calls: int, success: bool
) -> None:
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    import requests

    from feast.permissions.client.http_auth_requests_wrapper import HttpSessionManager

    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            calls.append(
                json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            )
            code = 200 if success and len(calls) > 1 else status
            body = json.dumps(
                {"results": [{"status": "Success", "features": {"feature_a": 2.0}}]}
            ).encode()
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        config = _repo_config(tmp_path)
        config.online_store.path = f"http://127.0.0.1:{server.server_port}"
        config.online_store.connection_retries = retries
        source = ChrononSource(
            materialization_path="unused.parquet",
            chronon_join="team/join",
            timestamp_field="event_timestamp",
        )
        if success:
            rows = ChrononOnlineStore().online_read(
                config, _feature_view(source), _entity_keys()[:1], ["feature_a"]
            )
            assert rows == [(None, {"feature_a": ValueProto(float_val=2.0)})]
        else:
            with pytest.raises(requests.RequestException):
                ChrononOnlineStore().online_read(
                    config, _feature_view(source), _entity_keys()[:1], ["feature_a"]
                )
        assert calls == [[{"user_id": 1}]] * expected_calls
    finally:
        HttpSessionManager.close_session()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
