import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
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
from tests.utils.http_server import free_port


class _ChrononHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        rows = json.loads(body.decode("utf8"))
        response = {
            "results": [
                {
                    "status": "Success",
                    "entityKeys": row,
                    "features": {"feature_a": float(row["user_id"]) * 2.0},
                }
                for row in rows
            ]
        }
        payload = json.dumps(response).encode("utf8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        return


@pytest.fixture
def chronon_server():
    port = free_port()
    server = HTTPServer(("127.0.0.1", port), _ChrononHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()
    thread.join(timeout=5)


@pytest.mark.integration
def test_chronon_online_store_feature_store_integration(
    tmp_path: Path, chronon_server: str
):
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
        online_store={"type": "chronon", "path": chronon_server},
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
        online=True,
    )
    store.apply([user, fv])

    response = store.get_online_features(
        features=["fraud_profile:feature_a"],
        entity_rows=[{"user_id": 1}, {"user_id": 2}],
    ).to_dict()

    assert response["feature_a"] == [2.0, 4.0]
