from unittest.mock import MagicMock, patch

from feast.infra.offline_stores.iceberg.iceberg_source import (
    IcebergRestCatalogSource,
    UnityCatalogSource,
    load_catalog_from_source,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto


def _make_iceberg_source(**overrides):
    defaults = {
        "name": "test_source",
        "endpoint": "https://test.databricks.net/api/2.1/unity-catalog/iceberg",
        "warehouse": "test_cat",
        "namespace": "test_sch",
        "table": "test_table",
        "timestamp_field": "event_ts",
    }
    return IcebergRestCatalogSource(**{**defaults, **overrides})


def test_iceberg_source_init():
    src = _make_iceberg_source()
    assert src.name == "test_source"
    assert src.endpoint == "https://test.databricks.net/api/2.1/unity-catalog/iceberg"
    assert src.warehouse == "test_cat"
    assert src.namespace == "test_sch"
    assert src._table == "test_table"
    assert src.timestamp_field == "event_ts"
    assert src.token_env_var == "ICEBERG_REST_TOKEN"
    assert src.credential_vending is True


def test_iceberg_source_full_table_name():
    src = _make_iceberg_source()
    assert src.full_table_name == "`test_cat`.`test_sch`.`test_table`"


def test_iceberg_source_get_table_query_string():
    src = _make_iceberg_source()
    assert src.get_table_query_string() == "`test_cat`.`test_sch`.`test_table`"


def test_iceberg_source_source_type():
    src = _make_iceberg_source()
    assert src.source_type() == DataSourceProto.CUSTOM_SOURCE


def test_iceberg_source_proto_roundtrip():
    src = _make_iceberg_source(
        name="proto_test",
        timestamp_field="ts",
        description="a test source",
        owner="user@test.com",
        tags={"env": "test"},
    )
    proto = src.to_proto()
    assert proto.name == "proto_test"
    assert proto.type == DataSourceProto.CUSTOM_SOURCE
    assert proto.owner == "user@test.com"
    assert proto.description == "a test source"

    restored = IcebergRestCatalogSource.from_proto(proto)
    assert restored == src
    assert restored.name == src.name
    assert restored.endpoint == src.endpoint
    assert restored.warehouse == src.warehouse
    assert restored.namespace == src.namespace
    assert restored._table == src._table


def test_iceberg_source_equality():
    src1 = _make_iceberg_source(name="src1", table="t1")
    src2 = _make_iceberg_source(name="src1", table="t1")
    assert src1 == src2

    src3 = _make_iceberg_source(name="diff", table="t2")
    assert src1 != src3


def test_iceberg_source_path_file_format_query():
    src = _make_iceberg_source()
    assert src.path is None
    assert src.file_format is None
    assert src.query is None


def test_iceberg_source_proto_class_type():
    src = _make_iceberg_source()
    proto = src.to_proto()
    assert (
        proto.data_source_class_type
        == "feast.infra.offline_stores.iceberg.iceberg_source.IcebergRestCatalogSource"
    )


def test_unity_catalog_source_init():
    src = UnityCatalogSource(
        name="uc_source",
        endpoint="https://test.databricks.net/api/2.1/unity-catalog/iceberg",
        warehouse="uc_cat",
        namespace="uc_sch",
        table="uc_table",
        timestamp_field="ts",
        register_as_feature_table=True,
        sync_lineage=True,
    )
    assert src.register_as_feature_table is True
    assert src.sync_lineage is True
    assert src.token_env_var == "DATABRICKS_TOKEN"


def test_unity_catalog_source_proto_roundtrip():
    src = UnityCatalogSource(
        name="uc_test",
        endpoint="https://test.databricks.net/api/2.1/unity-catalog/iceberg",
        warehouse="uc_cat",
        namespace="uc_sch",
        table="uc_table",
        timestamp_field="ts",
        cluster_id="0123-4567-abcde",
        register_as_feature_table=True,
        sync_lineage=True,
    )
    proto = src.to_proto()
    restored = UnityCatalogSource.from_proto(proto)
    assert restored == src
    assert restored.cluster_id == "0123-4567-abcde"
    assert restored.register_as_feature_table is True
    assert restored.sync_lineage is True


def test_unity_catalog_source_equality():
    src1 = UnityCatalogSource(
        name="u1",
        endpoint="https://test.databricks.net",
        warehouse="cat",
        namespace="sch",
        table="t1",
        cluster_id="c1",
    )
    src2 = UnityCatalogSource(
        name="u1",
        endpoint="https://test.databricks.net",
        warehouse="cat",
        namespace="sch",
        table="t1",
        cluster_id="c1",
    )
    src3 = UnityCatalogSource(
        name="u1",
        endpoint="https://test.databricks.net",
        warehouse="cat",
        namespace="sch",
        table="t1",
        cluster_id="c2",
    )
    assert src1 == src2
    assert src1 != src3


def test_unity_catalog_source_proto_class_type():
    src = UnityCatalogSource(
        name="uc_test",
        endpoint="https://test.databricks.net/api/2.1/unity-catalog/iceberg",
        warehouse="cat",
        namespace="sch",
        table="t",
    )
    proto = src.to_proto()
    assert (
        proto.data_source_class_type
        == "feast.infra.offline_stores.iceberg.iceberg_source.UnityCatalogSource"
    )


@patch("feast.infra.offline_stores.iceberg.iceberg_source.load_catalog")
def test_load_catalog_from_source_iceberg(mock_load_catalog):
    mock_catalog = MagicMock()
    mock_load_catalog.return_value = mock_catalog

    src = _make_iceberg_source()
    catalog = load_catalog_from_source(src)

    assert catalog == mock_catalog
    mock_load_catalog.assert_called_once()
    call_kwargs = mock_load_catalog.call_args[1]
    assert call_kwargs["type"] == "rest"
    assert src.endpoint in call_kwargs.get("uri", "")


@patch("feast.infra.offline_stores.iceberg.iceberg_source.load_catalog")
def test_load_catalog_from_source_unity(mock_load_catalog):
    mock_catalog = MagicMock()
    mock_load_catalog.return_value = mock_catalog

    src = UnityCatalogSource(
        name="uc",
        endpoint="https://test.databricks.net/api/2.1/unity-catalog/iceberg",
        warehouse="cat",
        namespace="sch",
        table="t",
    )
    catalog = load_catalog_from_source(src)

    assert catalog == mock_catalog
    mock_load_catalog.assert_called_once()
    call_kwargs = mock_load_catalog.call_args[1]
    assert call_kwargs["type"] == "databricks"
