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


def test_fq_table_name_within_limit(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(name="test_feature_view", source=file_source)

    expected_table_name = f'"{keyspace}"."{project}_{table.name}"'
    actual_table_name = CassandraOnlineStore._fq_table_name(keyspace, project, table)

    assert expected_table_name == actual_table_name


def test_fq_table_name_exceeds_limit(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(
        name="test_feature_view_with_a_very_long_name_exceeding_limit",
        source=file_source,
    )
    expected_table_name = (
        f'"{keyspace}"."p6e72a69_test_feature_view_with_a_very__4d479508"'
    )
    actual_table_name = CassandraOnlineStore._fq_table_name(keyspace, project, table)

    assert expected_table_name == actual_table_name


def test_fq_table_name_edge_case(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(name="a" * 30, source=file_source)

    expected_table_name = f'"{keyspace}"."{project}_{table.name}"'
    actual_table_name = CassandraOnlineStore._fq_table_name(keyspace, project, table)

    assert expected_table_name == actual_table_name


def test_fq_table_name_edge_case_exceeds_limit(file_source):
    keyspace = "test_keyspace"
    project = "test_project_edge"
    table = FeatureView(name="a" * 31, source=file_source)

    expected_table_name = (
        f'"{keyspace}"."pfd98066_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_625ed0fd"'
    )
    actual_table_name = CassandraOnlineStore._fq_table_name(keyspace, project, table)

    assert expected_table_name == actual_table_name
