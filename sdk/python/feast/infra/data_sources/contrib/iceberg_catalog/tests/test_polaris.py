"""Unit tests for PolarisSource, PolarisClient and PolarisProvider."""

import json
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_rest_client import (
    CatalogAuthError,
)
from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import (
    IcebergSource,
)
from feast.infra.data_sources.contrib.iceberg_catalog.polaris_client import (
    OAuthClientCredentialsTokenProvider,
    PolarisClient,
)
from feast.infra.data_sources.contrib.iceberg_catalog.polaris_provider import (
    PolarisProvider,
)
from feast.infra.data_sources.contrib.iceberg_catalog.polaris_source import (
    PolarisSource,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.types import Float32
from feast.value_type import ValueType

ENDPOINT = "http://polaris:8181/api/catalog"


def _source(**kwargs) -> PolarisSource:
    defaults: Dict[str, Any] = dict(
        endpoint=ENDPOINT,
        warehouse="my_catalog",
        namespace="ml",
        table="driver_stats",
        timestamp_field="event_timestamp",
    )
    defaults.update(kwargs)
    return PolarisSource(**defaults)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in (
        "POLARIS_HOST",
        "POLARIS_TOKEN",
        "POLARIS_CLIENT_ID",
        "POLARIS_CLIENT_SECRET",
    ):
        monkeypatch.delenv(var, raising=False)


class TestPolarisSource:
    def test_defaults(self):
        source = _source()
        assert source.catalog_type == "rest"
        assert source.token_env_var == "POLARIS_TOKEN"
        assert source.client_id_env_var == "POLARIS_CLIENT_ID"
        secret_var = source.client_secret_env_var
        assert secret_var == "POLARIS_CLIENT_SECRET"  # pragma: allowlist secret
        assert source.sync_metadata is True
        assert source.credential_vending is True
        assert source.fqn == "my_catalog.ml.driver_stats"

    def test_is_subclass_of_iceberg_source(self):
        assert isinstance(_source(), IcebergSource)

    def test_endpoint_from_polaris_host(self, monkeypatch):
        monkeypatch.setenv("POLARIS_HOST", "http://polaris:8181/")
        source = PolarisSource(warehouse="w", namespace="n", table="t")
        assert source.endpoint == "http://polaris:8181/api/catalog"

    def test_missing_endpoint_raises(self):
        with pytest.raises(ValueError, match="POLARIS_HOST"):
            PolarisSource(warehouse="w", namespace="n", table="t")

    def test_proto_roundtrip(self):
        source = _source(
            description="d",
            tags={"team": "ml"},
            owner="o@example.com",
            client_id_env_var="MY_ID",
            client_secret_env_var="MY_SECRET",  # pragma: allowlist secret
            oauth_scope="PRINCIPAL_ROLE:feast",
            oauth_server_uri="http://idp/token",
            credential_vending=False,
            sync_metadata=False,
        )
        proto = source.to_proto()
        assert proto.type == DataSourceProto.BATCH_ICEBERG
        assert proto.data_source_class_type.endswith("polaris_source.PolarisSource")
        options = json.loads(str(proto.custom_options.configuration, "utf8"))
        # Only env var names are persisted, never secrets.
        assert options["client_id_env_var"] == "MY_ID"
        assert "client_secret" not in options

        restored = PolarisSource.from_proto(proto)
        assert isinstance(restored, PolarisSource)
        assert restored == source
        assert restored.oauth_scope == "PRINCIPAL_ROLE:feast"
        assert restored.oauth_server_uri == "http://idp/token"
        assert restored.sync_metadata is False
        assert restored.credential_vending is False

    def test_iceberg_source_from_proto_dispatches_to_polaris(self):
        proto = _source().to_proto()
        restored = IcebergSource.from_proto(proto)
        assert isinstance(restored, PolarisSource)

    def test_equality_considers_polaris_fields(self):
        assert _source() == _source()
        assert _source(sync_metadata=False) != _source()
        assert _source(oauth_scope="PRINCIPAL_ROLE:x") != _source()

    def test_pyiceberg_config_static_token(self, monkeypatch):
        monkeypatch.setenv("POLARIS_TOKEN", "tok")
        config = _source().get_pyiceberg_catalog_config()
        assert config["type"] == "rest"
        assert config["uri"] == ENDPOINT
        assert config["warehouse"] == "my_catalog"
        assert config["token"] == "tok"
        assert "credential" not in config
        assert config["header.X-Iceberg-Access-Delegation"] == "vended-credentials"

    def test_pyiceberg_config_oauth_wins_over_token(self, monkeypatch):
        monkeypatch.setenv("POLARIS_TOKEN", "tok")
        monkeypatch.setenv("POLARIS_CLIENT_ID", "cid")
        monkeypatch.setenv("POLARIS_CLIENT_SECRET", "sec")
        config = _source(
            oauth_server_uri="http://idp/token"
        ).get_pyiceberg_catalog_config()
        assert config["credential"] == "cid:sec"
        assert config["scope"] == "PRINCIPAL_ROLE:ALL"
        assert config["oauth2-server-uri"] == "http://idp/token"
        assert "token" not in config

    def test_pyiceberg_config_without_credential_vending(self, monkeypatch):
        monkeypatch.setenv("POLARIS_TOKEN", "tok")
        config = _source(credential_vending=False).get_pyiceberg_catalog_config()
        assert "header.X-Iceberg-Access-Delegation" not in config

    @patch("pyiceberg.catalog.load_catalog")
    def test_load_pyiceberg_table_uses_polaris_config(self, mock_load, monkeypatch):
        monkeypatch.setenv("POLARIS_CLIENT_ID", "cid")
        monkeypatch.setenv("POLARIS_CLIENT_SECRET", "sec")
        _source().load_pyiceberg_table()
        name, kwargs = mock_load.call_args[0][0], mock_load.call_args[1]
        assert name == "feast_polaris"
        assert kwargs["credential"] == "cid:sec"
        mock_load.return_value.load_table.assert_called_once_with("ml.driver_stats")

    def test_catalog_client_is_polaris_client(self, monkeypatch):
        monkeypatch.setenv("POLARIS_CLIENT_ID", "cid")
        monkeypatch.setenv("POLARIS_CLIENT_SECRET", "sec")
        client = _source().get_catalog_client()
        assert isinstance(client, PolarisClient)
        assert client.token_provider is not None

    def test_base_iceberg_source_config_is_unchanged(self, monkeypatch):
        monkeypatch.setenv("T", "tok")
        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="n",
            table="t",
            token_env_var="T",
        )
        assert source.get_pyiceberg_catalog_config() == {
            "type": "rest",
            "uri": "http://host/iceberg",
            "warehouse": "w",
            "token": "tok",
        }


class TestDiscover:
    @patch.object(PolarisClient, "get_table_schema")
    @patch.object(PolarisClient, "list_tables")
    @patch.object(PolarisClient, "list_namespaces")
    def test_discover_builds_sources_and_skips_tables_without_timestamp(
        self, mock_ns, mock_tables, mock_schema
    ):
        mock_ns.return_value = ["ml"]
        mock_tables.return_value = ["with_ts", "without_ts"]
        mock_schema.side_effect = lambda ns, t: (
            [
                {"name": "id", "type": "long"},
                {"name": "event_timestamp", "type": "timestamptz"},
            ]
            if t == "with_ts"
            else [{"name": "id", "type": "long"}]
        )
        sources = PolarisSource.discover(
            warehouse="my_catalog", endpoint=ENDPOINT, tags={"team": "ml"}
        )
        assert [s.name for s in sources] == ["ml__with_ts"]
        assert sources[0].timestamp_field == "event_timestamp"
        assert sources[0].tags == {"team": "ml"}
        assert sources[0].endpoint == ENDPOINT

    @patch.object(PolarisClient, "list_tables")
    @patch.object(PolarisClient, "list_namespaces")
    def test_discover_respects_explicit_namespaces(self, mock_ns, mock_tables):
        mock_tables.return_value = []
        PolarisSource.discover(warehouse="w", endpoint=ENDPOINT, namespaces=["a", "b"])
        mock_ns.assert_not_called()
        assert [c.args[0] for c in mock_tables.call_args_list] == ["a", "b"]


class TestPolarisClientAuth:
    def test_static_token_header(self):
        client = PolarisClient(ENDPOINT, "w", token="tok")
        assert client._session.headers["Authorization"] == "Bearer tok"
        assert client.token_provider is None
        assert client.url_style == "prefix"

    def test_oauth_overrides_static_token(self):
        client = PolarisClient(
            ENDPOINT, "w", token="tok", client_id="cid", client_secret="sec"
        )
        assert "Authorization" not in client._session.headers
        assert client.token_provider is not None
        assert client.token_provider.token_url == f"{ENDPOINT}/v1/oauth/tokens"

    @patch("requests.post")
    def test_token_fetch_and_cache(self, mock_post):
        mock_post.return_value = Mock(
            ok=True, json=lambda: {"access_token": "abc", "expires_in": 3600}
        )
        provider = OAuthClientCredentialsTokenProvider(
            "http://idp/token", "cid", "sec", scope="PRINCIPAL_ROLE:ALL"
        )
        assert provider.get_token() == "abc"
        assert provider.get_token() == "abc"
        assert mock_post.call_count == 1
        data = mock_post.call_args.kwargs["data"]
        assert data["grant_type"] == "client_credentials"
        assert data["client_id"] == "cid"
        assert data["scope"] == "PRINCIPAL_ROLE:ALL"

    @patch("requests.post")
    def test_token_refreshes_before_expiry(self, mock_post):
        mock_post.side_effect = [
            Mock(ok=True, json=lambda: {"access_token": "t1", "expires_in": 100}),
            Mock(ok=True, json=lambda: {"access_token": "t2", "expires_in": 100}),
        ]
        provider = OAuthClientCredentialsTokenProvider(
            "http://idp/token", "cid", "sec", refresh_skew_seconds=60
        )
        with patch(
            "feast.infra.data_sources.contrib.iceberg_catalog.polaris_client.time.monotonic"
        ) as clock:
            clock.return_value = 1000.0
            assert provider.get_token() == "t1"
            clock.return_value = 1030.0  # 70s left, outside the 60s skew
            assert provider.get_token() == "t1"
            clock.return_value = 1050.0  # 50s left, inside the skew -> refresh
            assert provider.get_token() == "t2"
        assert mock_post.call_count == 2

    @patch("requests.post")
    def test_token_http_error_raises_auth_error(self, mock_post):
        mock_post.return_value = Mock(ok=False, status_code=401)
        provider = OAuthClientCredentialsTokenProvider("http://idp/token", "c", "s")
        with pytest.raises(CatalogAuthError):
            provider.get_token()

    @patch("requests.post")
    def test_token_connection_error_raises_auth_error(self, mock_post):
        mock_post.side_effect = requests.exceptions.ConnectionError("down")
        provider = OAuthClientCredentialsTokenProvider("http://idp/token", "c", "s")
        with pytest.raises(CatalogAuthError):
            provider.get_token()

    @patch("requests.post")
    def test_missing_access_token_raises(self, mock_post):
        mock_post.return_value = Mock(ok=True, json=lambda: {})
        provider = OAuthClientCredentialsTokenProvider("http://idp/token", "c", "s")
        with pytest.raises(CatalogAuthError, match="access_token"):
            provider.get_token()

    @patch("requests.post")
    def test_session_attaches_fresh_bearer_token(self, mock_post):
        mock_post.return_value = Mock(
            ok=True, json=lambda: {"access_token": "abc", "expires_in": 3600}
        )
        client = PolarisClient(ENDPOINT, "w", client_id="cid", client_secret="sec")
        prepared = client._session.prepare_request(
            requests.Request("GET", f"{ENDPOINT}/v1/w/namespaces")
        )
        assert prepared.headers["Authorization"] == "Bearer abc"


class TestPolarisClientProperties:
    @patch("requests.Session.post")
    def test_set_table_properties_request(self, mock_post):
        mock_post.return_value = Mock(ok=True)
        client = PolarisClient(ENDPOINT, "my_catalog", token="tok")
        assert client.set_table_properties("ml", "t", {"feast.project": "p"}) is True

        url = mock_post.call_args.args[0]
        assert url == f"{ENDPOINT}/v1/my_catalog/namespaces/ml/tables/t"
        assert mock_post.call_args.kwargs["json"] == {
            "requirements": [],
            "updates": [
                {"action": "set-properties", "updates": {"feast.project": "p"}}
            ],
        }

    @patch("requests.Session.post")
    def test_set_table_properties_noop_when_empty(self, mock_post):
        client = PolarisClient(ENDPOINT, "w", token="tok")
        assert client.set_table_properties("ml", "t", {}) is True
        mock_post.assert_not_called()

    @patch("requests.Session.post")
    def test_remove_table_properties_request(self, mock_post):
        mock_post.return_value = Mock(ok=True)
        client = PolarisClient(ENDPOINT, "w", token="tok")
        assert client.remove_table_properties("ml", "t", ["feast.deleted_at"])
        assert mock_post.call_args.kwargs["json"]["updates"] == [
            {"action": "remove-properties", "removals": ["feast.deleted_at"]}
        ]

    @patch("requests.Session.post")
    def test_http_failure_returns_false_without_raising(self, mock_post):
        mock_post.return_value = Mock(ok=False, status_code=403, text="forbidden")
        client = PolarisClient(ENDPOINT, "w", token="tok")
        assert client.set_table_properties("ml", "t", {"k": "v"}) is False

    @patch("requests.Session.post")
    def test_connection_failure_returns_false(self, mock_post):
        mock_post.side_effect = requests.exceptions.ConnectionError("down")
        client = PolarisClient(ENDPOINT, "w", token="tok")
        assert client.set_table_properties("ml", "t", {"k": "v"}) is False


def _feature_view(source, **kwargs) -> FeatureView:
    return FeatureView(
        name="driver_stats",
        entities=[
            Entity(
                name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
            )
        ],
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=source,
        **kwargs,
    )


class TestPolarisProvider:
    def test_build_properties(self):
        view = _feature_view(
            _source(), owner="o@example.com", description="stats", tags={"team": "ml"}
        )
        assert PolarisProvider.build_properties("proj", view) == {
            "feast.is_feature_table": "true",
            "feast.project": "proj",
            "feast.feature_view": "driver_stats",
            "feast.owner": "o@example.com",
            "feast.description": "stats",
            "feast.tag.team": "ml",
        }

    def test_sync_sets_properties_for_polaris_views_only(self):
        polaris_source = _source()
        client = MagicMock()
        view = _feature_view(polaris_source)
        other = FeatureView(
            name="other",
            entities=[
                Entity(
                    name="driver_id",
                    join_keys=["driver_id"],
                    value_type=ValueType.INT64,
                )
            ],
            schema=[Field(name="x", dtype=Float32)],
            source=IcebergSource(
                endpoint="http://h",
                warehouse="w",
                namespace="n",
                table="t",
                timestamp_field="ts",
            ),
        )
        provider = object.__new__(PolarisProvider)
        with patch.object(PolarisSource, "get_catalog_client", return_value=client):
            provider._sync_polaris_properties("proj", [view, other])

        client.set_table_properties.assert_called_once()
        args = client.set_table_properties.call_args.args
        assert args[:2] == ("ml", "driver_stats")
        assert args[2]["feast.project"] == "proj"

    def test_sync_respects_sync_metadata_flag(self):
        client = MagicMock()
        view = _feature_view(_source(sync_metadata=False))
        provider = object.__new__(PolarisProvider)
        with patch.object(PolarisSource, "get_catalog_client", return_value=client):
            provider._sync_polaris_properties("proj", [view])
        client.set_table_properties.assert_not_called()

    def test_deleted_view_marks_table_without_dropping(self):
        client = MagicMock()
        view = _feature_view(_source())
        provider = object.__new__(PolarisProvider)
        with patch.object(PolarisSource, "get_catalog_client", return_value=client):
            provider._mark_polaris_tables_removed([view])

        props = client.set_table_properties.call_args.args[2]
        assert props["feast.is_feature_table"] == "false"
        assert "feast.deleted_at" in props
        assert not client.method_calls or all(
            "drop" not in c[0] for c in client.method_calls
        )

    def test_registered_in_provider_map(self):
        from feast.infra.provider import PROVIDERS_CLASS_FOR_TYPE

        assert PROVIDERS_CLASS_FOR_TYPE["polaris"].endswith(
            "polaris_provider.PolarisProvider"
        )
