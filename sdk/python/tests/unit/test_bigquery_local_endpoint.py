"""Local BigQuery clients must not refresh cloud credentials."""

from __future__ import annotations

from unittest.mock import patch

from google.auth.credentials import AnonymousCredentials
from pytest import MonkeyPatch

from feast.infra.offline_stores.bigquery import _get_bigquery_client


def test_local_endpoint_uses_anonymous_credentials_with_connection_override(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("BIGQUERY_EMULATOR_HOST", "http://127.0.0.1:5388")
    with patch(
        "feast.infra.offline_stores.bigquery.get_connection_config_override",
        return_value={
            "project": "local-dev",
            "service_account_json": "not-read-in-local-mode",
        },
    ):
        client = _get_bigquery_client(data_source=object())
    try:
        assert isinstance(client._credentials, AnonymousCredentials)
        assert client.project == "local-dev"
        assert client._connection.API_BASE_URL == "http://127.0.0.1:5388"
    finally:
        client.close()
