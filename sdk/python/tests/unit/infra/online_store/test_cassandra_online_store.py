import pytest

from feast import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.contrib.cassandra_online_store.cassandra_online_store import (
    CassandraOnlineStore,
)


@pytest.fixture
def file_source():
    file_source = FileSource(name="my_file_source", path="test.parquet")
    return file_source


def test_fq_table_name_v1_within_limit(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(name="test_feature_view", source=file_source)

    expected_table_name = f'"{keyspace}"."{project}_{table.name}"'
    actual_table_name = CassandraOnlineStore._fq_table_name(keyspace, project, table, 1)

    assert expected_table_name == actual_table_name


def test_fq_table_name_v1_exceeds_limit(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(
        name="test_feature_view_with_a_very_long_name_exceeding_limit",
        source=file_source,
    )
    expected_table_name = f'"{keyspace}"."{project}_{table.name}"'
    actual_table_name = CassandraOnlineStore._fq_table_name(keyspace, project, table, 1)

    assert expected_table_name == actual_table_name


def test_fq_table_name_v2_within_limit(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(name="test_feature_view", source=file_source)

    expected_table_name = f'"{keyspace}"."{project}_{table.name}"'
    actual_table_name = CassandraOnlineStore._fq_table_name(keyspace, project, table, 2)

    assert expected_table_name == actual_table_name


def test_fq_table_name_v2_exceeds_limit(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(
        name="test_feature_view_with_a_very_long_name_exceeding_limit",
        source=file_source,
    )
    expected_table_name = (
        f'"{keyspace}"."test__29UZUpJQRijDZsYzl_test__5Ur8Mv5QutEG23Cp2C"'
    )
    actual_table_name = CassandraOnlineStore._fq_table_name(keyspace, project, table, 2)

    assert expected_table_name == actual_table_name


def test_fq_table_name_invalid_version(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(name="test_feature_view", source=file_source)

    with pytest.raises(ValueError) as excinfo:
        CassandraOnlineStore._fq_table_name(keyspace, project, table, 3)
    assert "Unknown table name format version: 3" in str(excinfo.value)
