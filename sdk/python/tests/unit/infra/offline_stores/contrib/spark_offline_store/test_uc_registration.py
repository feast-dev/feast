from typing import Dict, Optional
from unittest.mock import MagicMock, patch

from feast import FeatureView
from feast.infra.offline_stores.iceberg.catalog_config import IcebergCatalogConfig
from feast.infra.offline_stores.iceberg.catalog_manager import _feast_to_iceberg_type
from feast.infra.offline_stores.iceberg.registration import (
    _build_tags,
    _get_primary_keys,
    _resolve_uc_path,
    _should_register,
    register_uc_feature_tables,
    write_uc_materialized_data,
)


def make_mock_field(name: str, dtype_str: str = "", nullable: bool = True):
    field = MagicMock()
    field.name = name
    if dtype_str:

        class FakeDtype:
            def __str__(self):
                return dtype_str

        field.dtype = FakeDtype()
    else:
        field.dtype = None
    return field


def test_feast_to_iceberg_type_none():
    assert _feast_to_iceberg_type(None) == "string"


def test_feast_to_iceberg_type_int32():
    assert _feast_to_iceberg_type(make_mock_field("col", "Int32").dtype) == "int"


def test_feast_to_iceberg_type_int64():
    assert _feast_to_iceberg_type(make_mock_field("col", "Int64").dtype) == "long"


def test_feast_to_iceberg_type_float32():
    assert _feast_to_iceberg_type(make_mock_field("col", "Float32").dtype) == "float"


def test_feast_to_iceberg_type_float64():
    assert _feast_to_iceberg_type(make_mock_field("col", "Float64").dtype) == "double"


def test_feast_to_iceberg_type_string():
    assert _feast_to_iceberg_type(make_mock_field("col", "String").dtype) == "string"


def test_feast_to_iceberg_type_bool():
    assert _feast_to_iceberg_type(make_mock_field("col", "Bool").dtype) == "boolean"


def test_feast_to_iceberg_type_bytes():
    assert _feast_to_iceberg_type(make_mock_field("col", "Bytes").dtype) == "binary"


def test_feast_to_iceberg_type_unix_timestamp():
    t = _feast_to_iceberg_type(make_mock_field("col", "UnixTimestamp").dtype)
    assert t == "timestamp"


def test_feast_to_iceberg_type_list():
    t = _feast_to_iceberg_type(make_mock_field("col", "List<String>").dtype)
    assert t == "list<string>"


def test_feast_to_iceberg_type_array():
    t = _feast_to_iceberg_type(make_mock_field("col", "Array<Int32>").dtype)
    assert t == "list<string>"


def test_feast_to_iceberg_type_unknown():
    t = _feast_to_iceberg_type(make_mock_field("col", "UnknownType").dtype)
    assert t == "string"


def test_should_register_default():
    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    assert _should_register(fv) is True


def test_should_register_true():
    fv = MagicMock(spec=FeatureView)
    fv.tags = {"uc.register_as_feature_table": "true"}
    assert _should_register(fv) is True


def test_should_register_false():
    fv = MagicMock(spec=FeatureView)
    fv.tags = {"uc.register_as_feature_table": "false"}
    assert _should_register(fv) is False


def test_should_register_case_insensitive():
    fv = MagicMock(spec=FeatureView)
    fv.tags = {"uc.register_as_feature_table": "FALSE"}
    assert _should_register(fv) is False


def test_resolve_uc_path_uses_defaults():
    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    fv.name = "my_feature_view"
    catalog, schema, table = _resolve_uc_path(fv, "default_cat", "default_sch")
    assert catalog == "default_cat"
    assert schema == "default_sch"
    assert table == "my_feature_view"


def test_resolve_uc_path_uses_tags():
    fv = MagicMock(spec=FeatureView)
    fv.tags = {
        "uc.catalog": "tag_cat",
        "uc.schema": "tag_sch",
        "uc.table": "tag_table",
    }
    fv.name = "ignored"
    catalog, schema, table = _resolve_uc_path(fv, "default_cat", "default_sch")
    assert catalog == "tag_cat"
    assert schema == "tag_sch"
    assert table == "tag_table"


def test_resolve_uc_path_sanitizes_table_name():
    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    fv.name = "my-feature.view"
    catalog, schema, table = _resolve_uc_path(fv, "cat", "sch")
    assert table == "my_feature_view"


def test_resolve_uc_path_tag_overrides_none_default():
    fv = MagicMock(spec=FeatureView)
    fv.tags = {"uc.catalog": "tag_cat"}
    fv.name = "fv_name"
    catalog, schema, table = _resolve_uc_path(fv, None, None)
    assert catalog == "tag_cat"
    assert schema is None
    assert table == "fv_name"


def test_build_tags():
    fv = MagicMock(spec=FeatureView)
    fv.tags = {
        "env": "prod",
        "uc.register_as_feature_table": "false",
        "owner": "team_a",
    }
    tags = _build_tags(fv, "my_project")
    assert tags["env"] == "prod"
    assert tags["owner"] == "team_a"
    assert "uc.register_as_feature_table" not in tags
    assert tags["feast_managed"] == "feast"
    assert tags["feast_project"] == "my_project"


def test_build_tags_empty_tags():
    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    tags = _build_tags(fv, "proj")
    assert tags["feast_managed"] == "feast"
    assert tags["feast_project"] == "proj"


def test_get_primary_keys():
    fv = MagicMock(spec=FeatureView)
    col1 = make_mock_field("id", "Int32")
    col2 = make_mock_field("date", "String")
    fv.entity_columns = [col1, col2]
    assert _get_primary_keys(fv) == ["id", "date"]


def test_get_primary_keys_empty():
    fv = MagicMock(spec=FeatureView)
    fv.entity_columns = []
    assert _get_primary_keys(fv) == []


def _make_catalog_config(**overrides) -> IcebergCatalogConfig:
    defaults = {
        "type": "rest",
        "endpoint": "https://test.databricks.net/api/2.1/unity-catalog/iceberg",
        "warehouse": "test_cat",
        "namespace": "test_sch",
    }
    merged = {**defaults, **overrides}
    return IcebergCatalogConfig(**merged)


def test_register_uc_feature_tables_skips_when_missing_pyiceberg():
    config = _make_catalog_config()
    with patch.dict("sys.modules", {"pyiceberg": None}):
        register_uc_feature_tables(config, [], "test_project")
    # No error means skip silently


@patch("feast.infra.offline_stores.iceberg.registration.load_catalog")
def test_register_uc_feature_tables_creates_new_table(mock_load_catalog):
    mock_catalog = MagicMock()
    mock_load_catalog.return_value = mock_catalog
    mock_catalog.load_table.side_effect = Exception("table not found")

    config = _make_catalog_config()

    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    fv.name = "test_fv"
    fv.entity_columns = []
    fv.features = []
    fv.batch_source = MagicMock()
    fv.batch_source.timestamp_field = None
    fv.description = "test description"
    fv.owner = "user1"

    with patch(
        "feast.infra.offline_stores.iceberg.registration.table_exists",
        return_value=False,
    ):
        with patch(
            "feast.infra.offline_stores.iceberg.registration.create_feature_table"
        ) as mock_create:
            register_uc_feature_tables(config, [fv], "proj")

    mock_create.assert_called_once()
    call_kwargs = mock_create.call_args[1]
    assert call_kwargs["namespace"] == "test_sch"
    assert call_kwargs["table_name"] == "test_fv"
    assert call_kwargs["description"] == "test description"


@patch("feast.infra.offline_stores.iceberg.registration.load_catalog")
def test_register_uc_feature_tables_updates_existing(mock_load_catalog):
    mock_catalog = MagicMock()
    mock_load_catalog.return_value = mock_catalog

    config = _make_catalog_config()

    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    fv.name = "test_fv"
    fv.entity_columns = []
    fv.features = []
    fv.batch_source = MagicMock()
    fv.batch_source.timestamp_field = None
    fv.description = "test description"
    fv.owner = "user1"

    with patch(
        "feast.infra.offline_stores.iceberg.registration.table_exists",
        return_value=True,
    ):
        with patch(
            "feast.infra.offline_stores.iceberg.registration.update_table_properties"
        ) as mock_update:
            register_uc_feature_tables(config, [fv], "proj")

    mock_update.assert_called_once()
    call_kwargs = mock_update.call_args[1]
    assert call_kwargs["namespace"] == "test_sch"
    assert call_kwargs["table_name"] == "test_fv"
    assert call_kwargs["description"] == "test description"


@patch("feast.infra.offline_stores.iceberg.registration.load_catalog")
def test_register_uc_feature_tables_skips_opt_out(mock_load_catalog):
    mock_catalog = MagicMock()
    mock_load_catalog.return_value = mock_catalog

    config = _make_catalog_config()

    fv = MagicMock(spec=FeatureView)
    fv.tags = {"uc.register_as_feature_table": "false"}
    fv.name = "opt_out_fv"
    fv.entity_columns = []
    fv.features = []
    fv.batch_source = MagicMock()
    fv.batch_source.timestamp_field = None
    fv.description = ""
    fv.owner = None

    with patch(
        "feast.infra.offline_stores.iceberg.registration.create_feature_table"
    ) as mock_create:
        register_uc_feature_tables(config, [fv], "proj")

    mock_create.assert_not_called()


@patch("feast.infra.offline_stores.iceberg.registration.load_catalog")
def test_register_uc_feature_tables_skips_when_missing_catalog(
    mock_load_catalog,
):
    mock_catalog = MagicMock()
    mock_load_catalog.return_value = mock_catalog

    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    fv.name = "no_cat_fv"
    fv.entity_columns = []
    fv.features = []
    fv.batch_source = MagicMock()
    fv.batch_source.timestamp_field = None
    fv.description = ""
    fv.owner = None

    # Use a config with empty warehouse/namespace to simulate missing catalog info
    config_no_warehouse = _make_catalog_config(warehouse="", namespace="")

    with patch(
        "feast.infra.offline_stores.iceberg.registration.create_feature_table"
    ) as mock_create:
        with patch(
            "feast.infra.offline_stores.iceberg.registration.update_table_properties"
        ) as mock_update:
            register_uc_feature_tables(config_no_warehouse, [fv], "proj")

    mock_create.assert_not_called()
    mock_update.assert_not_called()


# ──────────────────────────────────────────────
#  Tests for write_uc_materialized_data (L3)
# ──────────────────────────────────────────────


def _make_catalog_config_for_mat(
    warehouse: str = "cat",
    namespace: str = "sch",
) -> IcebergCatalogConfig:
    return IcebergCatalogConfig(
        type="rest",
        endpoint="https://example.com/api/iceberg",
        warehouse=warehouse,
        namespace=namespace,
    )


def _make_mock_fv_for_mat(
    name: str = "test_fv",
    tags: Optional[Dict] = None,
) -> FeatureView:
    fv = MagicMock(spec=FeatureView)
    fv.name = name
    fv.tags = tags or {}
    fv.entity_columns = []
    fv.features = []
    fv.batch_source = MagicMock()
    fv.batch_source.timestamp_field = None
    fv.description = ""
    fv.owner = None
    return fv


def test_write_uc_materialized_data_skips_opt_out():
    """Tag uc.register_as_feature_table=false → skip."""
    catalog_config = _make_catalog_config_for_mat()
    fv = _make_mock_fv_for_mat(tags={"uc.register_as_feature_table": "false"})

    with patch(
        "feast.infra.offline_stores.iceberg.registration.load_catalog",
    ) as mock_load:
        write_uc_materialized_data(catalog_config, fv, MagicMock(), "proj")

    mock_load.assert_not_called()


def test_write_uc_materialized_data_skips_materialize_offline_false():
    """Tag uc.materialize_offline=false → skip."""
    catalog_config = _make_catalog_config_for_mat()
    fv = _make_mock_fv_for_mat(tags={"uc.materialize_offline": "false"})

    with patch(
        "feast.infra.offline_stores.iceberg.registration.load_catalog",
    ) as mock_load:
        write_uc_materialized_data(catalog_config, fv, MagicMock(), "proj")

    mock_load.assert_not_called()


def test_write_uc_materialized_data_skips_missing_catalog():
    """Missing catalog/schema → skip (log warning)."""
    catalog_config = _make_catalog_config_for_mat(warehouse="", namespace="")
    fv = _make_mock_fv_for_mat()

    with patch(
        "feast.infra.offline_stores.iceberg.registration.load_catalog",
    ) as mock_load:
        write_uc_materialized_data(catalog_config, fv, MagicMock(), "proj")

    mock_load.assert_not_called()


def test_write_uc_materialized_data_skips_catalog_load_failure():
    """load_catalog raises → skip."""
    catalog_config = _make_catalog_config_for_mat()
    fv = _make_mock_fv_for_mat()

    with patch(
        "feast.infra.offline_stores.iceberg.registration.load_catalog",
        side_effect=Exception("connection error"),
    ) as mock_load:
        write_uc_materialized_data(catalog_config, fv, MagicMock(), "proj")

    mock_load.assert_called_once()


def test_write_uc_materialized_data_writes_merge_existing_table():
    """Existing table → write_materialized_data (merge) is called."""
    catalog_config = _make_catalog_config_for_mat()
    fv = _make_mock_fv_for_mat()

    with patch(
        "feast.infra.offline_stores.iceberg.registration.load_catalog",
    ) as mock_load:
        with patch(
            "feast.infra.offline_stores.iceberg.registration.table_exists",
            return_value=True,
        ) as mock_exists:
            with patch(
                "feast.infra.offline_stores.iceberg.registration.write_materialized_data",
            ) as mock_write:
                mock_catalog = MagicMock()
                mock_load.return_value = mock_catalog

                write_uc_materialized_data(catalog_config, fv, MagicMock(), "proj")

    mock_exists.assert_called_once()
    mock_write.assert_called_once()
    _, kwargs = mock_write.call_args
    assert kwargs["namespace"] == "sch"
    assert kwargs["table_name"] == "test_fv"
    assert kwargs["mode"] == "merge"


def test_write_uc_materialized_data_creates_then_writes():
    """New table → create_feature_table then write_materialized_data."""
    catalog_config = _make_catalog_config_for_mat()
    fv = _make_mock_fv_for_mat()

    with patch(
        "feast.infra.offline_stores.iceberg.registration.load_catalog",
    ) as mock_load:
        with patch(
            "feast.infra.offline_stores.iceberg.registration.table_exists",
            return_value=False,
        ):
            with patch(
                "feast.infra.offline_stores.iceberg.registration.create_feature_table",
            ) as mock_create:
                with patch(
                    "feast.infra.offline_stores.iceberg.registration.write_materialized_data",
                ) as mock_write:
                    mock_catalog = MagicMock()
                    mock_load.return_value = mock_catalog

                    write_uc_materialized_data(catalog_config, fv, MagicMock(), "proj")

    mock_create.assert_called_once()
    mock_write.assert_called_once()
    assert mock_write.call_args[1]["mode"] == "merge"
