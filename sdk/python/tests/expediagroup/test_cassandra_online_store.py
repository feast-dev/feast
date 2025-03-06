import textwrap

import pytest
from cassandra.cluster import Cluster

from feast import Entity, Field, FileSource, RepoConfig, ValueType
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.online_stores.contrib.cassandra_online_store.cassandra_online_store import (
    CassandraOnlineStore,
    CassandraOnlineStoreConfig,
)
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.sorted_feature_view import SortedFeatureView, SortKey
from feast.types import (
    Array,
    Int64,
    String,
)
from tests.expediagroup.cassandra_online_store_creator import (
    EGCassandraOnlineStoreCreator,
)

REGISTRY = "s3://test_registry/registry.db"
PROJECT = "cassandra_project"
PROVIDER = "aws"
ENTITY_KEY_SERIALIZATION_VERSION = 2


@pytest.fixture(scope="session")
def cassandra_online_store_config():
    creator = EGCassandraOnlineStoreCreator("cassandra_project")
    config = creator.create_online_store()
    yield config
    creator.teardown()


@pytest.fixture(scope="session")
def setup_keyspace(cassandra_online_store_config):
    hosts = cassandra_online_store_config["hosts"]
    port = cassandra_online_store_config["port"]
    cluster = Cluster(hosts, port=port)
    session = cluster.connect()
    keyspace = "feast_keyspace"
    create_keyspace_cql = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
    """
    session.execute(create_keyspace_cql)
    yield keyspace
    drop_keyspace_cql = f"DROP KEYSPACE {keyspace};"
    session.execute(drop_keyspace_cql)
    session.shutdown()
    cluster.shutdown()


@pytest.fixture(scope="session")
def repo_config(cassandra_online_store_config, setup_keyspace) -> RepoConfig:
    return RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=CassandraOnlineStoreConfig(
            hosts=cassandra_online_store_config["hosts"],
            port=cassandra_online_store_config["port"],
            keyspace=setup_keyspace,
            table_name_format_version=cassandra_online_store_config.get(
                "table_name_format_version", 1
            ),
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=ENTITY_KEY_SERIALIZATION_VERSION,
    )


@pytest.fixture(scope="session")
def online_store(repo_config) -> CassandraOnlineStore:
    store = CassandraOnlineStore()
    yield store


@pytest.fixture
def cassandra_session(online_store, repo_config):
    session = online_store._get_session(repo_config)
    keyspace = online_store._keyspace
    return session, keyspace


class TestCassandraConnectionManager:
    def test_connection_manager(self, repo_config, caplog, mocker):
        mocker.patch("cassandra.cluster.Cluster.connect")
        store = CassandraOnlineStore()
        with store._get_session(repo_config):
            assert f"{repo_config.online_store.hosts}" in caplog.text

        Cluster.connect.assert_called_once_with(
            repo_config.online_store.keyspace,
        )

    def test_session_shutdown(self, repo_config, mocker):
        store = CassandraOnlineStore()
        session = store._get_session(repo_config)

        def fake_shutdown():
            session.is_shutdown = True

        mocker.patch.object(session, "shutdown", fake_shutdown)
        store.__del__()
        assert session.is_shutdown, "Session was not shutdown after __del__()"


class TestCassandraOnlineStore:
    def test_create_table_from_sorted_feature_view(
        self,
        cassandra_session,
        repo_config: RepoConfig,
        online_store: CassandraOnlineStore,
    ):
        """
        Test the _create_table method of CassandraOnlineStore for sorted feature view
        """
        session, keyspace = cassandra_session

        sorted_feature_view = SortedFeatureView(
            name="test_sorted_feature_view",
            entities=[Entity(name="entity1", join_keys=["entity1_id"])],
            source=FileSource(name="my_file_source", path="test.parquet"),
            schema=[
                Field(name="feature1", dtype=Int64),
                Field(name="feature2", dtype=Array(String)),
                Field(name="sort_key1", dtype=Int64),
                Field(name="sort_key2", dtype=String),
            ],
            sort_keys=[
                SortKey(
                    name="sort_key1",
                    value_type=ValueType.INT64,
                    default_sort_order=SortOrder.Enum.ASC,
                ),
                SortKey(
                    name="sort_key2",
                    value_type=ValueType.STRING,
                    default_sort_order=SortOrder.Enum.DESC,
                ),
            ],
        )

        constructed_table_name_in_cassandra = online_store._fq_table_name(
            keyspace,
            repo_config.project,
            sorted_feature_view,
            repo_config.online_store.table_name_format_version,
        )

        constructed_table_name = constructed_table_name_in_cassandra.split(".")[
            1
        ].strip('"')

        online_store._create_table(
            repo_config, repo_config.project, sorted_feature_view
        )

        # Verify that the table now exists by querying system_schema.tables.
        result = session.execute(
            f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace}' AND table_name='{constructed_table_name}';"
        )

        tables = [row.table_name for row in result]
        assert constructed_table_name in tables, "Table was not created successfully"

        # Verify that the schema (columns and types) matches.
        columns_query = textwrap.dedent(f"""\
            SELECT column_name, type
            FROM system_schema.columns
            WHERE keyspace_name='{keyspace}' AND table_name='{constructed_table_name}';
        """)

        rows = session.execute(columns_query)
        actual_columns = {row.column_name: row.type for row in rows}

        expected_columns = {
            "entity_key": "text",
            "feature1": "bigint",
            "feature2": "list<text>",
            "sort_key1": "bigint",
            "sort_key2": "text",
            "event_ts": "timestamp",
            "created_ts": "timestamp",
        }

        for col, expected_type in expected_columns.items():
            assert col in actual_columns, f"Missing column: {col}"
            assert actual_columns[col] == expected_type, (
                f"Column '{col}' has type '{actual_columns[col]}' but expected '{expected_type}'"
            )
