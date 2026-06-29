from unittest.mock import MagicMock, patch

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

from feast import FeatureView
from feast.infra.offline_stores.contrib.spark_offline_store.databricks_uc import (
    DatabricksUCOfflineStoreConfig,
    UCRegistrationConfig,
)
from feast.infra.offline_stores.contrib.spark_offline_store.uc_registration import (
    _build_full_table_name,
    _build_spark_schema,
    _build_uc_tags,
    _feast_to_spark_type_simple,
    _get_primary_keys,
    _resolve_uc_path,
    _should_register,
    register_uc_feature_tables,
    write_uc_materialized_data,
)
from feast.repo_config import RepoConfig


def make_mock_field(name: str, dtype_str: str = "", nullable: bool = True):
    field = MagicMock()
    field.name = name
    # _feast_to_spark_type_simple does str(dtype).upper()
    # When dtype_str is empty, dtype will be None -> StringType fallback
    if dtype_str:

        class FakeDtype:
            def __str__(self):
                return dtype_str

        field.dtype = FakeDtype()
    else:
        field.dtype = None
    return field


def test_feast_to_spark_type_simple_none():
    field = make_mock_field("col", "")
    result = _feast_to_spark_type_simple(field)
    assert isinstance(result, StringType)


def test_feast_to_spark_type_simple_int32():
    field = make_mock_field("col", "Int32")
    result = _feast_to_spark_type_simple(field)
    assert isinstance(result, IntegerType)


def test_feast_to_spark_type_simple_int64():
    field = make_mock_field("col", "Int64")
    result = _feast_to_spark_type_simple(field)
    assert isinstance(result, LongType)


def test_feast_to_spark_type_simple_float32():
    field = make_mock_field("col", "Float32")
    result = _feast_to_spark_type_simple(field)
    assert isinstance(result, FloatType)


def test_feast_to_spark_type_simple_float64():
    field = make_mock_field("col", "Float64")
    result = _feast_to_spark_type_simple(field)
    assert isinstance(result, DoubleType)


def test_feast_to_spark_type_simple_string():
    field = make_mock_field("col", "String")
    result = _feast_to_spark_type_simple(field)
    assert isinstance(result, StringType)


def test_feast_to_spark_type_simple_bool():
    field = make_mock_field("col", "Bool")
    result = _feast_to_spark_type_simple(field)
    assert isinstance(result, BooleanType)


def test_feast_to_spark_type_simple_bytes():
    field = make_mock_field("col", "Bytes")
    result = _feast_to_spark_type_simple(field)
    assert isinstance(result, BinaryType)


def test_feast_to_spark_type_simple_unix_timestamp():
    field = make_mock_field("col", "UnixTimestamp")
    result = _feast_to_spark_type_simple(field)
    assert isinstance(result, TimestampType)


def test_feast_to_spark_type_simple_list():
    field = make_mock_field("col", "List<String>")
    result = _feast_to_spark_type_simple(field)
    assert isinstance(result, ArrayType)


def test_feast_to_spark_type_simple_array():
    field = make_mock_field("col", "Array<Int32>")
    result = _feast_to_spark_type_simple(field)
    assert isinstance(result, ArrayType)


def test_feast_to_spark_type_simple_unknown():
    field = make_mock_field("col", "UnknownType")
    result = _feast_to_spark_type_simple(field)
    assert isinstance(result, StringType)


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


def test_build_uc_tags():
    fv = MagicMock(spec=FeatureView)
    fv.tags = {
        "env": "prod",
        "uc.register_as_feature_table": "false",
        "owner": "team_a",
    }
    tags = _build_uc_tags(fv, "my_project")
    assert tags["env"] == "prod"
    assert tags["owner"] == "team_a"
    assert "uc.register_as_feature_table" not in tags
    assert tags["feast_managed"] == "feast"
    assert tags["feast_project"] == "my_project"


def test_build_uc_tags_empty_tags():
    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    tags = _build_uc_tags(fv, "proj")
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


def test_build_full_table_name_all_parts():
    assert _build_full_table_name("cat", "sch", "table") == "`cat`.`sch`.`table`"


def test_build_full_table_name_no_catalog():
    assert _build_full_table_name(None, "sch", "table") == "`sch`.`table`"


def test_build_full_table_name_no_schema():
    assert _build_full_table_name("cat", None, "table") == "`cat`.`table`"


def test_build_full_table_name_only_table():
    assert _build_full_table_name(None, None, "table") == "`table`"


def test_build_spark_schema():
    fv = MagicMock(spec=FeatureView)
    id_field = make_mock_field("entity_id", "Int32")
    feat_field = make_mock_field("feature_val", "Float64")
    fv.entity_columns = [id_field]
    fv.features = [feat_field]
    fv.batch_source = MagicMock()
    fv.batch_source.timestamp_field = "event_ts"

    schema = _build_spark_schema(fv)

    assert isinstance(schema, StructType)
    fields = schema.fields
    assert fields[0].name == "entity_id"
    assert isinstance(fields[0].dataType, IntegerType)
    assert fields[0].nullable is False

    assert fields[1].name == "feature_val"
    assert isinstance(fields[1].dataType, DoubleType)
    assert fields[1].nullable is True

    assert fields[2].name == "event_ts"
    assert isinstance(fields[2].dataType, TimestampType)
    assert fields[2].nullable is True


def test_build_spark_schema_no_timestamp():
    fv = MagicMock(spec=FeatureView)
    id_field = make_mock_field("id", "Int64")
    fv.entity_columns = [id_field]
    fv.features = []
    fv.batch_source = MagicMock()
    fv.batch_source.timestamp_field = None

    schema = _build_spark_schema(fv)
    assert len(schema.fields) == 1
    assert schema.fields[0].name == "id"


def test_register_uc_feature_tables_skips_when_disabled():
    config = DatabricksUCOfflineStoreConfig(type="databricks_uc")
    register_uc_feature_tables(config, [], "test_project")
    # No error means success (skip silently)


def test_register_uc_feature_tables_skips_when_no_uc_config():
    config = DatabricksUCOfflineStoreConfig(
        type="databricks_uc",
        uc_registration=UCRegistrationConfig(enabled=False),
    )
    register_uc_feature_tables(config, [], "test_project")
    # No error means success


def _run_with_mock_fe(config, fvs, project, table_exists: bool = False):
    """Helper to run register_uc_feature_tables with mocked external deps."""
    fe_module = MagicMock()
    fe_module.FeatureEngineeringClient = MagicMock()
    with patch.dict("sys.modules", {"databricks.feature_engineering": fe_module}):
        with patch(
            "feast.infra.offline_stores.contrib.spark_offline_store.uc_registration.get_databricks_session"
        ) as mock_get_session:
            fe_client = MagicMock()
            fe_module.FeatureEngineeringClient.return_value = fe_client
            mock_session = MagicMock()
            mock_session.catalog.tableExists.return_value = table_exists
            mock_get_session.return_value = mock_session
            register_uc_feature_tables(config, fvs, project)
    return fe_client, mock_session


def test_register_uc_feature_tables_creates_new_table():
    config = DatabricksUCOfflineStoreConfig(
        type="databricks_uc",
        default_catalog="cat",
        default_schema="sch",
        uc_registration=UCRegistrationConfig(enabled=True),
    )

    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    fv.name = "test_fv"
    fv.entity_columns = []
    fv.features = []
    fv.batch_source = MagicMock()
    fv.batch_source.timestamp_field = None
    fv.description = "test description"
    fv.owner = "user1"

    fe_client, mock_session = _run_with_mock_fe(
        config, [fv], "proj", table_exists=False
    )

    fe_client.create_table.assert_called_once()
    call_kwargs = fe_client.create_table.call_args[1]
    assert call_kwargs["name"] == "`cat`.`sch`.`test_fv`"
    assert call_kwargs["primary_keys"] == []
    assert call_kwargs["description"] == "test description"


@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.uc_registration.get_databricks_session"
)
def test_register_uc_feature_tables_updates_existing(
    mock_get_session,
):
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_session.catalog.tableExists.return_value = True

    fe_module = MagicMock()
    fe_client = MagicMock()
    fe_module.FeatureEngineeringClient.return_value = fe_client

    config = DatabricksUCOfflineStoreConfig(
        type="databricks_uc",
        default_catalog="cat",
        default_schema="sch",
        uc_registration=UCRegistrationConfig(enabled=True),
    )

    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    fv.name = "test_fv"
    fv.entity_columns = []
    fv.features = []
    fv.batch_source = MagicMock()
    fv.batch_source.timestamp_field = None
    fv.description = "test description"
    fv.owner = "user1"

    with patch.dict("sys.modules", {"databricks.feature_engineering": fe_module}):
        register_uc_feature_tables(config, [fv], "proj")

    fe_client.create_table.assert_not_called()
    mock_session.sql.assert_any_call(
        "COMMENT ON TABLE `cat`.`sch`.`test_fv` IS 'test description'"
    )


@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.uc_registration.get_databricks_session"
)
def test_register_uc_feature_tables_skips_opt_out(
    mock_get_session,
):
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_session.catalog.tableExists.return_value = False

    config = DatabricksUCOfflineStoreConfig(
        type="databricks_uc",
        default_catalog="cat",
        default_schema="sch",
        uc_registration=UCRegistrationConfig(enabled=True),
    )

    fv = MagicMock(spec=FeatureView)
    fv.tags = {"uc.register_as_feature_table": "false"}
    fv.name = "opt_out_fv"
    fv.entity_columns = []
    fv.features = []
    fv.batch_source = MagicMock()
    fv.batch_source.timestamp_field = None
    fv.description = ""
    fv.owner = None

    fe_module = MagicMock()
    fe_client = MagicMock()
    fe_module.FeatureEngineeringClient.return_value = fe_client
    with patch.dict("sys.modules", {"databricks.feature_engineering": fe_module}):
        register_uc_feature_tables(config, [fv], "proj")

    # Per-view opt-out means create_table should NOT be called
    fe_client.create_table.assert_not_called()


def test_register_uc_feature_tables_skips_when_missing_catalog():
    fe_module = MagicMock()
    fe_client = MagicMock()
    fe_module.FeatureEngineeringClient.return_value = fe_client
    with patch.dict("sys.modules", {"databricks.feature_engineering": fe_module}):
        with patch(
            "feast.infra.offline_stores.contrib.spark_offline_store.uc_registration.get_databricks_session"
        ) as mock_get_session:
            mock_session = MagicMock()
            mock_get_session.return_value = mock_session

            config = DatabricksUCOfflineStoreConfig(
                type="databricks_uc",
                default_catalog=None,
                default_schema=None,
                uc_registration=UCRegistrationConfig(enabled=True),
            )

            fv = MagicMock(spec=FeatureView)
            fv.tags = {}
            fv.name = "no_cat_fv"
            fv.entity_columns = []
            fv.features = []
            fv.batch_source = MagicMock()
            fv.batch_source.timestamp_field = None
            fv.description = ""
            fv.owner = None

            register_uc_feature_tables(config, [fv], "proj")

    fe_client.create_table.assert_not_called()


# ──────────────────────────────────────────────
#  Tests for write_uc_materialized_data (L3)
# ──────────────────────────────────────────────


def _run_write_with_mock(
    config,
    fv,
    df,
    project,
    table_exists: bool = False,
    mock_spark_session=None,
):
    """Helper to run write_uc_materialized_data with mocked external deps."""
    fe_module = MagicMock()
    fe_module.FeatureEngineeringClient = MagicMock()
    with patch.dict("sys.modules", {"databricks.feature_engineering": fe_module}):
        with patch(
            "feast.infra.offline_stores.contrib.spark_offline_store.uc_registration.get_databricks_session"
        ) as mock_get_session:
            fe_client = MagicMock()
            fe_module.FeatureEngineeringClient.return_value = fe_client
            if mock_spark_session is None:
                mock_session = MagicMock()
            else:
                mock_session = mock_spark_session
            mock_session.catalog.tableExists.return_value = table_exists
            mock_get_session.return_value = mock_session
            write_uc_materialized_data(config, fv, df, project)
    return fe_client, mock_session


def test_write_uc_materialized_data_skips_wrong_offline_store():
    config = RepoConfig(
        registry="file:///tmp/reg.db",
        project="test",
        provider="local",
        offline_store=MagicMock(),
    )
    fv = MagicMock(spec=FeatureView)
    write_uc_materialized_data(config, fv, MagicMock(), "test")


def test_write_uc_materialized_data_skips_when_disabled():
    config = RepoConfig(
        registry="file:///tmp/reg.db",
        project="test",
        provider="local",
        offline_store=DatabricksUCOfflineStoreConfig(
            type="databricks_uc",
            uc_registration=UCRegistrationConfig(enabled=False),
        ),
    )
    fv = MagicMock(spec=FeatureView)
    write_uc_materialized_data(config, fv, MagicMock(), "test")


def test_write_uc_materialized_data_skips_opt_out():
    config = RepoConfig(
        registry="file:///tmp/reg.db",
        project="test",
        provider="local",
        offline_store=DatabricksUCOfflineStoreConfig(
            type="databricks_uc",
            default_catalog="cat",
            default_schema="sch",
            uc_registration=UCRegistrationConfig(enabled=True),
        ),
    )
    fv = MagicMock(spec=FeatureView)
    fv.tags = {"uc.register_as_feature_table": "false"}
    fv.name = "opt_out_fv"

    fe_module = MagicMock()
    with patch.dict("sys.modules", {"databricks.feature_engineering": fe_module}):
        write_uc_materialized_data(config, fv, MagicMock(), "test")

    fe_module.FeatureEngineeringClient.assert_not_called()


def test_write_uc_materialized_data_skips_materialize_offline_false():
    config = RepoConfig(
        registry="file:///tmp/reg.db",
        project="test",
        provider="local",
        offline_store=DatabricksUCOfflineStoreConfig(
            type="databricks_uc",
            default_catalog="cat",
            default_schema="sch",
            uc_registration=UCRegistrationConfig(enabled=True),
        ),
    )
    fv = MagicMock(spec=FeatureView)
    fv.tags = {"uc.materialize_offline": "false"}
    fv.name = "skip_mat_fv"

    fe_module = MagicMock()
    with patch.dict("sys.modules", {"databricks.feature_engineering": fe_module}):
        write_uc_materialized_data(config, fv, MagicMock(), "test")

    fe_module.FeatureEngineeringClient.assert_not_called()


def test_write_uc_materialized_data_skips_missing_catalog():
    config = RepoConfig(
        registry="file:///tmp/reg.db",
        project="test",
        provider="local",
        offline_store=DatabricksUCOfflineStoreConfig(
            type="databricks_uc",
            default_catalog=None,
            default_schema="sch",
            uc_registration=UCRegistrationConfig(enabled=True),
        ),
    )
    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    fv.name = "test_fv"

    fe_module = MagicMock()
    with patch.dict("sys.modules", {"databricks.feature_engineering": fe_module}):
        write_uc_materialized_data(config, fv, MagicMock(), "test")

    fe_module.FeatureEngineeringClient.assert_not_called()


def test_write_uc_materialized_data_writes_merge():
    config = RepoConfig(
        registry="file:///tmp/reg.db",
        project="test",
        provider="local",
        offline_store=DatabricksUCOfflineStoreConfig(
            type="databricks_uc",
            default_catalog="cat",
            default_schema="sch",
            uc_registration=UCRegistrationConfig(enabled=True),
        ),
    )
    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    fv.name = "test_fv"
    fv.entity_columns = []
    fv.features = []
    fv.batch_source = MagicMock()
    fv.batch_source.timestamp_field = None
    fv.description = ""
    fv.owner = None

    fe_client, _ = _run_write_with_mock(
        config, fv, MagicMock(), "test", table_exists=True
    )

    fe_client.write_table.assert_called_once()
    call_args = fe_client.write_table.call_args[1]
    assert call_args["name"] == "`cat`.`sch`.`test_fv`"
    assert call_args["mode"] == "merge"


def test_write_uc_materialized_data_creates_then_writes():
    config = RepoConfig(
        registry="file:///tmp/reg.db",
        project="test",
        provider="local",
        offline_store=DatabricksUCOfflineStoreConfig(
            type="databricks_uc",
            default_catalog="cat",
            default_schema="sch",
            uc_registration=UCRegistrationConfig(enabled=True),
        ),
    )
    fv = MagicMock(spec=FeatureView)
    fv.tags = {}
    fv.name = "new_fv"
    fv.entity_columns = []
    fv.features = []
    fv.batch_source = MagicMock()
    fv.batch_source.timestamp_field = None
    fv.description = ""
    fv.owner = None

    fe_client, _ = _run_write_with_mock(
        config, fv, MagicMock(), "test", table_exists=False
    )

    fe_client.create_table.assert_called_once()
    fe_client.write_table.assert_called_once()
    assert fe_client.write_table.call_args[1]["mode"] == "merge"
