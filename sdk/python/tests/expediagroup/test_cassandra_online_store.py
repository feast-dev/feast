import textwrap
import time
from datetime import datetime, timedelta

import pytest
from cassandra.cluster import Cluster

from feast import Entity, Field, FileSource, RepoConfig, ValueType
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.online_stores.contrib.cassandra_online_store.cassandra_online_store import (
    CassandraOnlineStore,
    CassandraOnlineStoreConfig,
)
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import (
    BoolList,
    BytesList,
    DoubleList,
    FloatList,
    Int32List,
    Int64List,
    StringList,
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.sorted_feature_view import SortedFeatureView, SortKey
from feast.types import (
    Array,
    Bool,
    Bytes,
    Float32,
    Float64,
    Int32,
    Int64,
    String,
    UnixTimestamp,
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

    def test_cassandra_online_write_batch_with_timestamp_as_sortkey(
        self,
        cassandra_session,
        repo_config: RepoConfig,
        online_store: CassandraOnlineStore,
    ):
        session, keyspace = cassandra_session
        (
            feature_view,
            data,
        ) = self._create_n_test_sample_features_with_timestamp_as_sortkey()
        constructed_table_name_in_cassandra = online_store._fq_table_name(
            keyspace,
            repo_config.project,
            feature_view,
            repo_config.online_store.table_name_format_version,
        )

        constructed_table_name = constructed_table_name_in_cassandra.split(".")[
            1
        ].strip('"')

        online_store._create_table(repo_config, repo_config.project, feature_view)
        online_store.online_write_batch(
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )
        result = session.execute(
            f"SELECT COUNT(*) from {keyspace}.{constructed_table_name};"
        )
        count = [row.count for row in result]
        assert count[0] == 10

    def test_cassandra_online_write_batch(
        self,
        cassandra_session,
        repo_config: RepoConfig,
        online_store: CassandraOnlineStore,
    ):
        session, keyspace = cassandra_session
        (
            feature_view,
            data,
        ) = self._create_n_test_sample_features()
        constructed_table_name_in_cassandra = online_store._fq_table_name(
            keyspace,
            repo_config.project,
            feature_view,
            repo_config.online_store.table_name_format_version,
        )

        constructed_table_name = constructed_table_name_in_cassandra.split(".")[
            1
        ].strip('"')

        online_store._create_table(repo_config, repo_config.project, feature_view)
        online_store.online_write_batch(
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )
        result = session.execute(
            f"SELECT COUNT(*) from {keyspace}.{constructed_table_name};"
        )
        count = [row.count for row in result]
        assert count[0] == 10

    def test_cassandra_online_write_batch_ttl(
        self,
        cassandra_session,
        repo_config: RepoConfig,
        online_store: CassandraOnlineStore,
    ):
        session, keyspace = cassandra_session
        (
            feature_view,
            data,
        ) = self._create_n_test_sample_features()
        constructed_table_name_in_cassandra = online_store._fq_table_name(
            keyspace,
            repo_config.project,
            feature_view,
            repo_config.online_store.table_name_format_version,
        )

        constructed_table_name = constructed_table_name_in_cassandra.split(".")[
            1
        ].strip('"')

        online_store._create_table(repo_config, repo_config.project, feature_view)
        online_store.online_write_batch(
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )
        # Wait until the records expire before querying
        time.sleep(15)
        result = session.execute(
            f"SELECT COUNT(*) from {keyspace}.{constructed_table_name};"
        )
        count = [row.count for row in result]
        # Number of records should be 0 as they were expired
        assert count[0] == 0

    def test_ttl_when_apply_ttl_on_write_true(
        self,
        online_store: CassandraOnlineStore,
    ):
        ttl = online_store._get_ttl(
            True,
            timedelta(seconds=10),
            15,
            datetime.utcnow() - timedelta(seconds=300),
        )
        assert ttl == 10

    def test_only_ttl_online_store_config_is_configured(
        self,
        online_store: CassandraOnlineStore,
    ):
        ttl = online_store._get_ttl(
            False,
            timedelta(0),
            30,
            datetime.utcnow() - timedelta(seconds=15),
        )
        assert ttl == 15

    def test_cassandra_online_write_batch_all_datatypes(
        self,
        cassandra_session,
        repo_config: RepoConfig,
        online_store: CassandraOnlineStore,
    ):
        session, keyspace = cassandra_session
        (
            feature_view,
            data,
        ) = self._create_n_test_sample_features_all_datatypes()
        constructed_table_name_in_cassandra = online_store._fq_table_name(
            keyspace,
            repo_config.project,
            feature_view,
            repo_config.online_store.table_name_format_version,
        )

        constructed_table_name = constructed_table_name_in_cassandra.split(".")[
            1
        ].strip('"')

        online_store._create_table(repo_config, repo_config.project, feature_view)
        online_store.online_write_batch(
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )
        result = session.execute(
            f"SELECT COUNT(*) from {keyspace}.{constructed_table_name};"
        )
        count = [row.count for row in result]
        assert count[0] == 1

    def _create_n_test_sample_features_with_timestamp_as_sortkey(self, n=10):
        fv = SortedFeatureView(
            name="sortedfeatureview",
            source=FileSource(
                name="my_file_source",
                path="test.parquet",
                timestamp_field="event_timestamp",
            ),
            entities=[Entity(name="id")],
            sort_keys=[
                SortKey(
                    name="event_timestamp",
                    value_type=ValueType.UNIX_TIMESTAMP,
                    default_sort_order=SortOrder.DESC,
                )
            ],
            schema=[
                Field(
                    name="id",
                    dtype=String,
                ),
                Field(
                    name="text",
                    dtype=String,
                ),
                Field(
                    name="int",
                    dtype=Int32,
                ),
                Field(
                    name="event_timestamp",
                    dtype=UnixTimestamp,
                ),
            ],
        )
        return fv, [
            (
                EntityKeyProto(
                    join_keys=["id"],
                    entity_values=[ValueProto(string_val=str(i))],
                ),
                {
                    "text": ValueProto(string_val="text"),
                    "int": ValueProto(int32_val=n),
                    "event_timestamp": ValueProto(
                        unix_timestamp_val=int(datetime.utcnow().timestamp())
                    ),
                },
                datetime.utcnow(),
                None,
            )
            for i in range(n)
        ]

    def _create_n_test_sample_features(self, n=10):
        fv = SortedFeatureView(
            name="sortedfv",
            source=FileSource(
                name="my_file_source",
                path="test.parquet",
                timestamp_field="event_timestamp",
            ),
            entities=[Entity(name="id")],
            ttl=timedelta(seconds=10),
            sort_keys=[
                SortKey(
                    name="int",
                    value_type=ValueType.INT32,
                    default_sort_order=SortOrder.DESC,
                )
            ],
            schema=[
                Field(
                    name="id",
                    dtype=String,
                ),
                Field(
                    name="text",
                    dtype=String,
                ),
                Field(
                    name="int",
                    dtype=Int32,
                ),
            ],
        )
        return fv, [
            (
                EntityKeyProto(
                    join_keys=["id"],
                    entity_values=[ValueProto(string_val=str(i))],
                ),
                {
                    "text": ValueProto(string_val="text"),
                    "int": ValueProto(int32_val=n),
                },
                datetime.utcnow(),
                None,
            )
            for i in range(n)
        ]

    def _create_n_test_sample_features_all_datatypes(self, n=10):
        mlpfs_test_all_dtypes_sorted_fv = SortedFeatureView(
            name="all_dtypes_sorted_fv",
            entities=[
                Entity(
                    name="index_sfv",
                    description="Index for the SortedFeatureView",
                    join_keys=["index_id"],
                    value_type=ValueType.STRING,
                )
            ],
            sort_keys=[
                SortKey(
                    name="event_timestamp",
                    value_type=ValueType.UNIX_TIMESTAMP,
                    default_sort_order=SortOrder.DESC,
                ),
                SortKey(
                    name="int_val",
                    value_type=ValueType.INT32,
                    default_sort_order=SortOrder.DESC,
                ),
                SortKey(
                    name="double_val",
                    value_type=ValueType.DOUBLE,
                    default_sort_order=SortOrder.DESC,
                ),
            ],
            source=FileSource(
                name="my_file_source",
                path="test.parquet",
                timestamp_field="event_timestamp",
            ),
            description="Sorted Feature View with all supported feast datatypes",
            online=True,
            schema=[
                Field(name="index_id", dtype=String),
                Field(name="int_val", dtype=Int32),
                Field(name="long_val", dtype=Int64),
                Field(name="float_val", dtype=Float32),
                Field(name="double_val", dtype=Float64),
                Field(name="byte_val", dtype=Bytes),
                Field(name="string_val", dtype=String),
                Field(name="timestamp_val", dtype=UnixTimestamp),
                Field(name="boolean_val", dtype=Bool),
                Field(name="array_int_val", dtype=Array(Int32)),
                Field(name="array_long_val", dtype=Array(Int64)),
                Field(name="array_float_val", dtype=Array(Float32)),
                Field(name="array_double_val", dtype=Array(Float64)),
                Field(name="array_byte_val", dtype=Array(Bytes)),
                Field(name="array_string_val", dtype=Array(String)),
                Field(name="array_timestamp_val", dtype=Array(UnixTimestamp)),
                Field(name="array_boolean_val", dtype=Array(Bool)),
                Field(name="null_int_val", dtype=Int32),
                Field(name="null_long_val", dtype=Int64),
                Field(name="null_float_val", dtype=Float32),
                Field(name="null_double_val", dtype=Float64),
                Field(name="null_byte_val", dtype=Bytes),
                Field(name="null_string_val", dtype=String),
                Field(name="null_timestamp_val", dtype=UnixTimestamp),
                Field(name="null_boolean_val", dtype=Bool),
                Field(name="null_array_int_val", dtype=Array(Int32)),
                Field(name="null_array_long_val", dtype=Array(Int64)),
                Field(name="null_array_float_val", dtype=Array(Float32)),
                Field(name="null_array_double_val", dtype=Array(Float64)),
                Field(name="null_array_byte_val", dtype=Array(Bytes)),
                Field(name="null_array_string_val", dtype=Array(String)),
                Field(name="null_array_timestamp_val", dtype=Array(UnixTimestamp)),
                Field(name="null_array_boolean_val", dtype=Array(Bool)),
                Field(name="event_timestamp", dtype=UnixTimestamp),
            ],
        )

        return mlpfs_test_all_dtypes_sorted_fv, [
            (
                EntityKeyProto(
                    join_keys=["index_id"],
                    entity_values=[ValueProto(string_val=str(1))],
                ),
                {
                    "int_val": ValueProto(int32_val=826),
                    "long_val": ValueProto(int64_val=79856),
                    "float_val": ValueProto(float_val=12.433371),
                    "double_val": ValueProto(double_val=8.295230388348628),
                    "byte_val": ValueProto(
                        bytes_val=bytes("some random byte", "utf-8")
                    ),
                    "string_val": ValueProto(string_val="text"),
                    "timestamp_val": ValueProto(unix_timestamp_val=n),
                    "boolean_val": ValueProto(bool_val=True),
                    "array_int_val": ValueProto(
                        int32_list_val=Int32List(val=[712, 317])
                    ),
                    "array_long_val": ValueProto(
                        int64_list_val=Int64List(val=[34949, 51284])
                    ),
                    "array_float_val": ValueProto(
                        float_list_val=FloatList(val=[25.404484, 48.07086])
                    ),
                    "array_double_val": ValueProto(
                        double_list_val=DoubleList(
                            val=[98.94745193632949, 98.94745193632949]
                        )
                    ),
                    "array_byte_val": ValueProto(
                        bytes_list_val=BytesList(
                            val=[
                                bytes("some random byte", "utf-8"),
                                bytes("some random byte", "utf-8"),
                            ]
                        )
                    ),
                    "array_string_val": ValueProto(
                        string_list_val=StringList(val=["6J0T8", "9EN4B"])
                    ),
                    "array_timestamp_val": ValueProto(
                        unix_timestamp_list_val=Int64List(val=[34949, 51284])
                    ),
                    "array_boolean_val": ValueProto(
                        bool_list_val=BoolList(val=[True, True])
                    ),
                    "null_int_val": ValueProto(null_val=None),
                    "null_long_val": ValueProto(null_val=None),
                    "null_float_val": ValueProto(null_val=None),
                    "null_double_val": ValueProto(null_val=None),
                    "null_byte_val": ValueProto(null_val=None),
                    "null_string_val": ValueProto(null_val=None),
                    "null_timestamp_val": ValueProto(null_val=None),
                    "null_boolean_val": ValueProto(null_val=None),
                    "null_array_int_val": ValueProto(null_val=None),
                    "null_array_long_val": ValueProto(null_val=None),
                    "null_array_float_val": ValueProto(null_val=None),
                    "null_array_double_val": ValueProto(null_val=None),
                    "null_array_byte_val": ValueProto(null_val=None),
                    "null_array_string_val": ValueProto(null_val=None),
                    "null_array_timestamp_val": ValueProto(null_val=None),
                    "null_array_boolean_val": ValueProto(null_val=None),
                    "event_timestamp": ValueProto(int32_val=n),
                },
                datetime.utcnow(),
                None,
            )
        ]
