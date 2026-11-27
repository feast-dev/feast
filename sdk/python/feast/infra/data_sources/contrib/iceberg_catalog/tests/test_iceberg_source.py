"""Unit tests for IcebergSource, UnityCatalogSource, and IcebergRestClient."""

import json
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_rest_client import (
    CatalogAuthError,
    IcebergCatalogError,
    IcebergRestClient,
    TableNotFoundError,
)
from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import (
    IcebergSource,
    _iceberg_type_to_feast_value_type,
)
from feast.infra.data_sources.contrib.iceberg_catalog.unity_catalog_source import (
    UnityCatalogSource,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.value_type import ValueType


class TestIcebergSource:
    def test_basic_creation(self):
        source = IcebergSource(
            endpoint="http://localhost:8080/api/2.1/unity-catalog/iceberg",
            warehouse="unity",
            namespace="default",
            table="my_table",
            timestamp_field="event_ts",
            token_env_var="MY_TOKEN",
        )
        assert source.fqn == "unity.default.my_table"
        assert source.endpoint == "http://localhost:8080/api/2.1/unity-catalog/iceberg"
        assert source.warehouse == "unity"
        assert source.namespace == "default"
        assert source.iceberg_table == "my_table"
        assert source.token_env_var == "MY_TOKEN"
        assert source.credential_vending is True
        assert source.name == "unity.default.my_table"

    def test_custom_name(self):
        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
            name="custom_name",
        )
        assert source.name == "custom_name"
        assert source.fqn == "w.ns.t"

    def test_source_type(self):
        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        assert source.source_type() == DataSourceProto.BATCH_ICEBERG

    def test_catalog_type_default(self):
        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        assert source.catalog_type == "rest"

    def test_catalog_type_explicit(self):
        source = IcebergSource(
            catalog_type="glue",
            warehouse="my_account",
            namespace="my_db",
            table="t",
            catalog_properties={"region_name": "us-east-1"},
        )
        assert source.catalog_type == "glue"
        assert source.catalog_properties == {"region_name": "us-east-1"}

    def test_proto_roundtrip(self):
        source = IcebergSource(
            endpoint="http://localhost:8080/iceberg",
            warehouse="prod",
            namespace="ml",
            table="features",
            timestamp_field="ts",
            created_timestamp_column="created",
            field_mapping={"src_col": "dst_col"},
            description="Test source",
            tags={"team": "ml"},
            owner="owner@example.com",
            token_env_var="TOKEN",
            credential_vending=False,
        )
        proto = source.to_proto()
        assert proto.type == DataSourceProto.BATCH_ICEBERG
        assert (
            "iceberg_catalog.iceberg_source.IcebergSource"
            in proto.data_source_class_type
        )

        config = json.loads(str(proto.custom_options.configuration, encoding="utf8"))
        assert config["endpoint"] == "http://localhost:8080/iceberg"
        assert config["warehouse"] == "prod"
        assert config["namespace"] == "ml"
        assert config["table"] == "features"
        assert config["token_env_var"] == "TOKEN"
        assert config["credential_vending"] is False
        assert config["catalog_type"] == "rest"

        restored = IcebergSource.from_proto(proto)
        assert restored == source
        assert restored.fqn == source.fqn
        assert restored.credential_vending == source.credential_vending

    def test_proto_roundtrip_with_catalog_properties(self):
        source = IcebergSource(
            catalog_type="hive",
            warehouse="wh",
            namespace="ns",
            table="t",
            catalog_properties={"uri": "thrift://metastore:9083"},
        )
        proto = source.to_proto()
        config = json.loads(str(proto.custom_options.configuration, encoding="utf8"))
        assert config["catalog_type"] == "hive"
        assert config["catalog_properties"] == {"uri": "thrift://metastore:9083"}

        restored = IcebergSource.from_proto(proto)
        assert restored == source
        assert restored.catalog_type == "hive"
        assert restored.catalog_properties == {"uri": "thrift://metastore:9083"}

    def test_get_table_query_string(self):
        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="catalog",
            namespace="schema",
            table="tbl",
        )
        assert source.get_table_query_string() == "iceberg_tmp_tbl"

    @patch(
        "feast.infra.data_sources.contrib.iceberg_catalog"
        ".iceberg_source.IcebergSource.get_catalog_client"
    )
    def test_get_table_column_names_and_types(self, mock_client):
        mock_instance = MagicMock()
        mock_instance.load_table.return_value = MagicMock(
            schema_fields=[
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "score", "type": "double"},
            ]
        )
        mock_client.return_value = mock_instance

        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        columns = list(source.get_table_column_names_and_types(config=MagicMock()))
        assert columns == [
            ("id", "long"),
            ("name", "string"),
            ("score", "double"),
        ]

    @patch(
        "feast.infra.data_sources.contrib.iceberg_catalog"
        ".iceberg_source.IcebergSource.get_catalog_client"
    )
    def test_validate_success(self, mock_client):
        mock_instance = MagicMock()
        mock_instance.load_table.return_value = MagicMock(location="s3://bucket/path")
        mock_client.return_value = mock_instance

        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        source.validate(config=MagicMock())

    @patch(
        "feast.infra.data_sources.contrib.iceberg_catalog"
        ".iceberg_source.IcebergSource.get_catalog_client"
    )
    def test_validate_failure_not_found(self, mock_client):
        mock_instance = MagicMock()
        mock_instance.load_table.side_effect = TableNotFoundError(
            "Table not found", status_code=404
        )
        mock_client.return_value = mock_instance

        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        with pytest.raises(ValueError, match="not found"):
            source.validate(config=MagicMock())

    @patch(
        "feast.infra.data_sources.contrib.iceberg_catalog"
        ".iceberg_source.IcebergSource.get_catalog_client"
    )
    def test_validate_failure_auth(self, mock_client):
        mock_instance = MagicMock()
        mock_instance.load_table.side_effect = CatalogAuthError(
            "Unauthorized", status_code=401
        )
        mock_client.return_value = mock_instance

        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        with pytest.raises(ValueError, match="Authentication failed"):
            source.validate(config=MagicMock())

    def test_equality(self):
        s1 = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        s2 = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        assert s1 == s2

        s3 = IcebergSource(
            endpoint="http://other/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        assert s1 != s3

    def test_equality_different_catalog_type(self):
        s1 = IcebergSource(
            catalog_type="rest",
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        s2 = IcebergSource(
            catalog_type="hive",
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        assert s1 != s2


class TestUnityCatalogSource:
    @patch.dict("os.environ", {"DATABRICKS_HOST": "http://my-workspace"})
    def test_default_endpoint(self):
        source = UnityCatalogSource(
            warehouse="prod",
            namespace="ml",
            table="features",
        )
        assert source.endpoint == "http://my-workspace/api/2.1/unity-catalog/iceberg"
        assert source.token_env_var == "DATABRICKS_TOKEN"

    def test_explicit_endpoint(self):
        source = UnityCatalogSource(
            endpoint="http://custom:8080/iceberg",
            warehouse="prod",
            namespace="ml",
            table="features",
            token_env_var="CUSTOM_TOKEN",
        )
        assert source.endpoint == "http://custom:8080/iceberg"
        assert source.token_env_var == "CUSTOM_TOKEN"

    def test_governance_flags(self):
        source = UnityCatalogSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
            register_as_feature_table=False,
            sync_lineage=False,
        )
        assert source.register_as_feature_table is False
        assert source.sync_lineage is False

    def test_is_subclass_of_iceberg_source(self):
        source = UnityCatalogSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        assert isinstance(source, IcebergSource)

    def test_proto_roundtrip(self):
        source = UnityCatalogSource(
            endpoint="http://host:8080/iceberg",
            warehouse="catalog",
            namespace="schema",
            table="my_table",
            timestamp_field="ts",
            register_as_feature_table=True,
            sync_lineage=False,
        )
        proto = source.to_proto()
        assert (
            "iceberg_catalog.unity_catalog_source.UnityCatalogSource"
            in proto.data_source_class_type
        )

        config = json.loads(str(proto.custom_options.configuration, encoding="utf8"))
        assert config["register_as_feature_table"] is True
        assert config["sync_lineage"] is False

        restored = UnityCatalogSource.from_proto(proto)
        assert restored == source
        assert restored.register_as_feature_table is True
        assert restored.sync_lineage is False


class TestIcebergTypeMapping:
    @pytest.mark.parametrize(
        "iceberg_type,expected",
        [
            ("boolean", ValueType.BOOL),
            ("int", ValueType.INT32),
            ("long", ValueType.INT64),
            ("float", ValueType.FLOAT),
            ("double", ValueType.DOUBLE),
            ("string", ValueType.STRING),
            ("binary", ValueType.BYTES),
            ("timestamp", ValueType.INT64),
            ("timestamptz", ValueType.INT64),
            ("decimal(10,2)", ValueType.DOUBLE),
            ("uuid", ValueType.STRING),
            ("unknown_type", ValueType.UNKNOWN),
        ],
    )
    def test_type_mapping(self, iceberg_type, expected):
        assert _iceberg_type_to_feast_value_type(iceberg_type) == expected


class TestIcebergRestClient:
    """Tests for IcebergRestClient: URL construction, error handling, pagination, schema resolution."""

    def test_url_style_auto_unity(self):
        client = IcebergRestClient(
            endpoint="https://host/api/2.1/unity-catalog/iceberg",
            warehouse="my_catalog",
        )
        assert client.url_style == "unity"
        assert client._base_url == (
            "https://host/api/2.1/unity-catalog/iceberg/v1/catalogs/my_catalog"
        )

    def test_url_style_auto_standard(self):
        client = IcebergRestClient(
            endpoint="https://polaris.example.com",
            warehouse="my_catalog",
        )
        assert client.url_style == "prefix"
        assert client._base_url == ("https://polaris.example.com/v1/my_catalog")

    def test_url_style_no_warehouse(self):
        client = IcebergRestClient(endpoint="https://nessie.example.com")
        assert client._base_url == "https://nessie.example.com/v1"

    def test_url_style_explicit_unity(self):
        client = IcebergRestClient(
            endpoint="https://custom.host",
            warehouse="w",
            url_style="unity",
        )
        assert client._base_url == "https://custom.host/v1/catalogs/w"

    def test_url_style_explicit_prefix(self):
        client = IcebergRestClient(
            endpoint="https://custom.host",
            warehouse="w",
            url_style="prefix",
        )
        assert client._base_url == "https://custom.host/v1/w"

    def test_session_headers_with_token(self):
        client = IcebergRestClient(
            endpoint="https://host",
            token="my-token",
        )
        assert client._session.headers["Authorization"] == "Bearer my-token"
        assert "Content-Type" in client._session.headers

    def test_session_headers_without_token(self):
        client = IcebergRestClient(
            endpoint="https://host",
            credential_vending=False,
        )
        assert "Authorization" not in client._session.headers
        assert "X-Iceberg-Access-Delegation" not in client._session.headers

    @patch("requests.Session.get")
    def test_load_table_404_raises_not_found(self, mock_get):
        resp = Mock()
        resp.ok = False
        resp.status_code = 404
        resp.json.return_value = {"message": "Table not found"}
        resp.text = "Table not found"
        mock_get.return_value = resp

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        with pytest.raises(TableNotFoundError) as exc_info:
            client.load_table("ns", "missing_table")
        assert exc_info.value.status_code == 404

    @patch("requests.Session.get")
    def test_load_table_401_raises_auth_error(self, mock_get):
        resp = Mock()
        resp.ok = False
        resp.status_code = 401
        resp.json.return_value = {"error": "Invalid token"}
        resp.text = "Unauthorized"
        mock_get.return_value = resp

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        with pytest.raises(CatalogAuthError) as exc_info:
            client.load_table("ns", "t")
        assert exc_info.value.status_code == 401

    @patch("requests.Session.get")
    def test_load_table_403_raises_auth_error(self, mock_get):
        resp = Mock()
        resp.ok = False
        resp.status_code = 403
        resp.json.return_value = {"message": "Forbidden"}
        resp.text = "Forbidden"
        mock_get.return_value = resp

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        with pytest.raises(CatalogAuthError) as exc_info:
            client.load_table("ns", "t")
        assert exc_info.value.status_code == 403

    @patch("requests.Session.get")
    def test_load_table_500_raises_catalog_error(self, mock_get):
        resp = Mock()
        resp.ok = False
        resp.status_code = 500
        resp.json.return_value = {"message": "Internal error"}
        resp.text = "Internal Server Error"
        mock_get.return_value = resp

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        with pytest.raises(IcebergCatalogError) as exc_info:
            client.load_table("ns", "t")
        assert exc_info.value.status_code == 500

    @patch("requests.Session.get")
    def test_load_table_connection_error(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError("refused")

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        with pytest.raises(IcebergCatalogError, match="Connection failed"):
            client.load_table("ns", "t")

    @patch("requests.Session.get")
    def test_load_table_timeout(self, mock_get):
        mock_get.side_effect = requests.exceptions.Timeout("timed out")

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        with pytest.raises(IcebergCatalogError, match="timed out"):
            client.load_table("ns", "t")

    @patch("requests.Session.get")
    def test_load_table_success(self, mock_get):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = {
            "metadata": {
                "table-uuid": "abc-123",
                "location": "s3://bucket/path",
                "format-version": 2,
                "schemas": [
                    {
                        "schema-id": 0,
                        "fields": [
                            {"name": "id", "type": "long"},
                            {"name": "name", "type": "string"},
                        ],
                    }
                ],
                "current-schema-id": 0,
                "properties": {"owner": "test"},
            }
        }
        mock_get.return_value = resp

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        metadata = client.load_table("ns", "t")
        assert metadata.table_id == "abc-123"
        assert metadata.location == "s3://bucket/path"
        assert len(metadata.schema_fields) == 2
        assert metadata.schema_fields[0] == {"name": "id", "type": "long"}

    @patch("requests.Session.get")
    def test_schema_resolution_uses_current_schema_id(self, mock_get):
        """Verifies that current-schema-id is used, not schemas[-1]."""
        resp = Mock()
        resp.ok = True
        resp.json.return_value = {
            "metadata": {
                "table-uuid": "abc",
                "location": "s3://b/p",
                "schemas": [
                    {
                        "schema-id": 0,
                        "fields": [{"name": "old_col", "type": "string"}],
                    },
                    {
                        "schema-id": 1,
                        "fields": [{"name": "new_col", "type": "long"}],
                    },
                    {
                        "schema-id": 2,
                        "fields": [{"name": "newest_col", "type": "double"}],
                    },
                ],
                "current-schema-id": 1,
            }
        }
        mock_get.return_value = resp

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        metadata = client.load_table("ns", "t")
        assert len(metadata.schema_fields) == 1
        assert metadata.schema_fields[0]["name"] == "new_col"

    @patch("requests.Session.get")
    def test_list_tables_with_pagination(self, mock_get):
        resp1 = Mock()
        resp1.ok = True
        resp1.json.return_value = {
            "identifiers": [{"namespace": ["ns"], "name": "t1"}],
            "next-page-token": "page2",
        }
        resp2 = Mock()
        resp2.ok = True
        resp2.json.return_value = {
            "identifiers": [{"namespace": ["ns"], "name": "t2"}],
        }
        mock_get.side_effect = [resp1, resp2]

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        tables = client.list_tables("ns")
        assert tables == ["t1", "t2"]
        assert mock_get.call_count == 2

    @patch("requests.Session.get")
    def test_list_tables_auth_error(self, mock_get):
        resp = Mock()
        resp.ok = False
        resp.status_code = 403
        resp.json.return_value = {"message": "Forbidden"}
        resp.text = "Forbidden"
        mock_get.return_value = resp

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        with pytest.raises(CatalogAuthError):
            client.list_tables("ns")

    @patch("requests.Session.get")
    def test_list_namespaces_with_pagination(self, mock_get):
        resp1 = Mock()
        resp1.ok = True
        resp1.json.return_value = {
            "namespaces": [["ns1"]],
            "next-page-token": "page2",
        }
        resp2 = Mock()
        resp2.ok = True
        resp2.json.return_value = {
            "namespaces": [["ns2"], ["ns3"]],
        }
        mock_get.side_effect = [resp1, resp2]

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        namespaces = client.list_namespaces()
        assert namespaces == ["ns1", "ns2", "ns3"]

    @patch("requests.Session.get")
    def test_credential_vending_s3(self, mock_get):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = {
            "config": {
                "s3.access-key-id": "AKID",
                "s3.secret-access-key": "secret",
                "s3.session-token": "tok",
                "s3.endpoint": "https://s3.amazonaws.com",
            }
        }
        mock_get.return_value = resp

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        cred = client.vend_credentials("ns", "t")
        assert cred is not None
        assert cred.access_key == "AKID"
        assert cred.credential_type == "s3"

    @patch("requests.Session.get")
    def test_credential_vending_gcs(self, mock_get):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = {
            "config": {
                "gcs.oauth2.token": "gcs-token",
                "gcs.oauth2.token-expires-at": 1700000000,
            }
        }
        mock_get.return_value = resp

        client = IcebergRestClient(endpoint="https://host", warehouse="w")
        cred = client.vend_credentials("ns", "t")
        assert cred is not None
        assert cred.access_key == "gcs-token"
        assert cred.credential_type == "gcs"

    def test_credential_vending_disabled(self):
        client = IcebergRestClient(
            endpoint="https://host",
            warehouse="w",
            credential_vending=False,
        )
        result = client.vend_credentials("ns", "t")
        assert result is None
