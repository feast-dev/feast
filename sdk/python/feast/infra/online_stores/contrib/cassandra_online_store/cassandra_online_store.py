#
#  Copyright 2019 The Feast Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""
Cassandra/Astra DB online store for Feast.
"""

import hashlib
import logging
import string
import time
from datetime import datetime
from functools import partial
from queue import Queue
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Union

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    ResultSet,
    Session,
)
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import BatchStatement, BatchType, PreparedStatement
from pydantic import StrictFloat, StrictInt, StrictStr

from feast import Entity, FeatureView, RepoConfig, SortedFeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.rate_limiter import SlidingWindowRateLimiter
from feast.repo_config import FeastConfigBaseModel
from feast.types import (
    Array,
    Bool,
    Bytes,
    ComplexFeastType,
    Float32,
    Float64,
    Int32,
    Int64,
    PrimitiveFeastType,
    String,
    UnixTimestamp,
)

# Error messages
E_CASSANDRA_UNEXPECTED_CONFIGURATION_CLASS = (
    "Unexpected configuration object (not a CassandraOnlineStoreConfig instance)"
)
E_CASSANDRA_NOT_CONFIGURED = (
    "Inconsistent Cassandra configuration: provide exactly one between "
    "'hosts' and 'secure_bundle_path' and a 'keyspace'"
)
E_CASSANDRA_MISCONFIGURED = (
    "Inconsistent Cassandra configuration: provide either 'hosts' or "
    "'secure_bundle_path', not both"
)
E_CASSANDRA_INCONSISTENT_AUTH = (
    "Username and password for Cassandra must be provided either both or none"
)
E_CASSANDRA_UNKNOWN_LB_POLICY = (
    "Unknown/unsupported Load Balancing Policy name in Cassandra configuration"
)

# CQL command templates (that is, before replacing schema names)
INSERT_CQL_4_TEMPLATE = (
    "INSERT INTO {fqtable} (feature_name,"
    " value, entity_key, event_ts) VALUES"
    " (?, ?, ?, ?) USING TTL {ttl};"
)

SELECT_CQL_TEMPLATE = "SELECT {columns} FROM {fqtable} WHERE entity_key = ?;"

CREATE_TABLE_CQL_TEMPLATE = """
    CREATE TABLE IF NOT EXISTS {fqtable} (
        entity_key      TEXT,
        feature_name    TEXT,
        value           BLOB,
        event_ts        TIMESTAMP,
        created_ts      TIMESTAMP,
        PRIMARY KEY ((entity_key), feature_name)
    ) WITH CLUSTERING ORDER BY (feature_name ASC)
    AND COMMENT='project={project}, feature_view={feature_view}';
"""

DROP_TABLE_CQL_TEMPLATE = "DROP TABLE IF EXISTS {fqtable};"

# op_name -> (cql template string, prepare boolean)
CQL_TEMPLATE_MAP = {
    # Queries/DML, statements to be prepared
    "insert4": (INSERT_CQL_4_TEMPLATE, True),
    "select": (SELECT_CQL_TEMPLATE, True),
    # DDL, do not prepare these
    "drop": (DROP_TABLE_CQL_TEMPLATE, False),
    "create": (CREATE_TABLE_CQL_TEMPLATE, False),
}

V2_TABLE_NAME_FORMAT_MAX_LENGTH = 48

# Logger
logger = logging.getLogger(__name__)


class CassandraInvalidConfig(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)


class CassandraOnlineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the Cassandra/Astra DB online store.

    Exactly one of `hosts` and `secure_bundle_path` must be provided;
    depending on which one, the connection will be to a regular Cassandra
    or an Astra DB instance (respectively).

    If connecting to Astra DB, authentication must be provided with username
    and password being the Client ID and Client Secret of the database token.
    """

    type: Literal["cassandra", "scylladb"] = "cassandra"
    """Online store type selector."""

    # settings for connection to Cassandra / Astra DB

    hosts: Optional[List[StrictStr]] = None
    """List of host addresses to reach the cluster."""

    secure_bundle_path: Optional[StrictStr] = None
    """Path to the secure connect bundle (for Astra DB; replaces hosts)."""

    port: Optional[StrictInt] = None
    """Port number for connecting to the cluster (optional)."""

    keyspace: StrictStr = "feast_keyspace"
    """Target Cassandra keyspace where all tables will be."""

    username: Optional[StrictStr] = None
    """Username for DB auth, possibly Astra DB token Client ID."""

    password: Optional[StrictStr] = None
    """Password for DB auth, possibly Astra DB token Client Secret."""

    protocol_version: Optional[StrictInt] = None
    """Explicit specification of the CQL protocol version used."""

    request_timeout: Optional[StrictFloat] = None
    """Request timeout in seconds. Defaults to no operation timeout."""

    lazy_table_creation: Optional[bool] = False
    """
    If True, tables will be created on during materialization, rather than registration.
    Table deletion is not currently supported in this mode.
    """

    key_ttl_seconds: Optional[StrictInt] = None
    """
    Default TTL (in seconds) to apply to all tables if not specified in FeatureView. Value 0 or None means No TTL.
    """

    key_batch_size: Optional[StrictInt] = 10
    """In Go Feature Server, this configuration is used to query tables with multiple keys at a time using IN clause based on the size specified. Value 1 means key batching is disabled. Valid values are 1 to 100."""

    class CassandraLoadBalancingPolicy(FeastConfigBaseModel):
        """
        Configuration block related to the Cluster's load-balancing policy.
        """

        load_balancing_policy: StrictStr
        """
        A stringy description of the load balancing policy to instantiate
        the cluster with. Supported values:
            "DCAwareRoundRobinPolicy"
            "TokenAwarePolicy(DCAwareRoundRobinPolicy)"
        """

        local_dc: StrictStr = "datacenter1"
        """The local datacenter, usually necessary to create the policy."""

    load_balancing: Optional[CassandraLoadBalancingPolicy] = None
    """
    Details on the load-balancing policy: it will be
    wrapped into an execution profile if present.
    """

    read_concurrency: Optional[StrictInt] = 100
    """
    Value of the `concurrency` parameter internally passed to Cassandra driver's
    `execute_concurrent_with_args` call when reading rows from tables.
    See https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/concurrent/#module-cassandra.concurrent .
    Default: 100.
    """

    write_concurrency: Optional[StrictInt] = 100
    """
    Controls the number of concurrent writes to the database.
    Default: 100.
    """

    write_rate_limit: Optional[StrictInt] = 0
    """
    The maximum number of write batches per second. Value 0 means no rate limiting.
    For spark materialization engine, this configuration is per executor task.
    """

    table_name_format_version: Optional[StrictInt] = 1
    """
    Version of the table name format. This is used to determine the format of the table name.
    Version 1: <project>_<feature_view_name>
    Version 2: Limits the length of the table name to 48 characters.
    Table names should be quoted to make them case sensitive.
    """


class CassandraOnlineStore(OnlineStore):
    """
    Cassandra/Astra DB online store implementation for Feast.

    Attributes:
        _cluster:   Cassandra cluster to connect to.
        _session:   (DataStax Cassandra drivers) session object
                    to issue commands.
        _keyspace:  Cassandra keyspace all tables live in.
        _prepared_statements: cache of statements prepared by the driver.
    """

    _cluster: Cluster = None
    _session: Session = None
    _keyspace: str = "feast_keyspace"
    _prepared_statements: Dict[str, PreparedStatement] = {}

    def _get_session(self, config: RepoConfig):
        """
        Establish the database connection, if not yet created,
        and return it.

        Also perform basic config validation checks.
        """

        online_store_config = config.online_store
        if not isinstance(online_store_config, CassandraOnlineStoreConfig):
            raise CassandraInvalidConfig(E_CASSANDRA_UNEXPECTED_CONFIGURATION_CLASS)

        if self._session:
            if not self._session.is_shutdown:
                return self._session
            else:
                self._session = None
        if not self._session:
            # configuration consistency checks
            hosts = online_store_config.hosts
            secure_bundle_path = online_store_config.secure_bundle_path
            port = online_store_config.port or 9042
            keyspace = online_store_config.keyspace
            username = online_store_config.username
            password = online_store_config.password
            protocol_version = online_store_config.protocol_version

            db_directions = hosts or secure_bundle_path
            if not db_directions or not keyspace:
                raise CassandraInvalidConfig(E_CASSANDRA_NOT_CONFIGURED)
            if hosts and secure_bundle_path:
                raise CassandraInvalidConfig(E_CASSANDRA_MISCONFIGURED)
            if (username is None) ^ (password is None):
                raise CassandraInvalidConfig(E_CASSANDRA_INCONSISTENT_AUTH)

            if username is not None:
                auth_provider = PlainTextAuthProvider(
                    username=username,
                    password=password,
                )
            else:
                auth_provider = None

            # handling of load-balancing policy (optional)
            if online_store_config.load_balancing:
                # construct a proper execution profile embedding
                # the configured LB policy
                _lbp_name = online_store_config.load_balancing.load_balancing_policy
                if _lbp_name == "DCAwareRoundRobinPolicy":
                    lb_policy = DCAwareRoundRobinPolicy(
                        local_dc=online_store_config.load_balancing.local_dc,
                    )
                elif _lbp_name == "TokenAwarePolicy(DCAwareRoundRobinPolicy)":
                    lb_policy = TokenAwarePolicy(
                        DCAwareRoundRobinPolicy(
                            local_dc=online_store_config.load_balancing.local_dc,
                        )
                    )
                else:
                    raise CassandraInvalidConfig(E_CASSANDRA_UNKNOWN_LB_POLICY)

                # wrap it up in a map of ex.profiles with a default
                exe_profile = ExecutionProfile(
                    request_timeout=online_store_config.request_timeout,
                    load_balancing_policy=lb_policy,
                )
                execution_profiles = {EXEC_PROFILE_DEFAULT: exe_profile}
            else:
                execution_profiles = None

            # additional optional keyword args to Cluster
            cluster_kwargs = {
                k: v
                for k, v in {
                    "protocol_version": protocol_version,
                    "execution_profiles": execution_profiles,
                }.items()
                if v is not None
            }

            # creation of Cluster (Cassandra vs. Astra)
            if hosts:
                self._cluster = Cluster(
                    hosts,
                    port=port,
                    auth_provider=auth_provider,
                    **cluster_kwargs,
                )
            else:
                # we use 'secure_bundle_path'
                self._cluster = Cluster(
                    cloud={"secure_connect_bundle": secure_bundle_path},
                    auth_provider=auth_provider,
                    **cluster_kwargs,
                )

            # creation of Session
            self._keyspace = keyspace
            self._session = self._cluster.connect(self._keyspace)

        return self._session

    def __del__(self):
        """
        Shutting down the session and cluster objects. If you don't do this,
        you would notice increase in connection spikes on the cluster. Once shutdown,
        you can't use the session object anymore.
        You'd get a RuntimeError "cannot schedule new futures after shutdown".
        """
        if self._session:
            if not self._session.is_shutdown:
                self._session.shutdown()

        if self._cluster:
            if not self._cluster.is_shutdown:
                self._cluster.shutdown()

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Write a batch of features of several entities to the database.

        Args:
            config: The RepoConfig for the current FeatureStore.
            table: Feast FeatureView.
            data: a list of quadruplets containing Feature data. Each
                  quadruplet contains an Entity Key, a dict containing feature
                  values, an event timestamp for the row, and
                  the created timestamp for the row if it exists.
            progress: Optional function to be called once every mini-batch of
                      rows is written to the online store. Can be used to
                      display progress.
        """

        def on_success(result, concurrent_queue):
            concurrent_queue.get_nowait()

        def on_failure(exc, concurrent_queue):
            concurrent_queue.get_nowait()
            logger.exception(f"Error writing a batch: {exc}")
            raise Exception("Exception raised while writing a batch") from exc

        online_store_config = config.online_store

        project = config.project

        ttl = online_store_config.key_ttl_seconds or 0
        write_concurrency = online_store_config.write_concurrency
        write_rate_limit = online_store_config.write_rate_limit
        concurrent_queue: Queue = Queue(maxsize=write_concurrency)
        rate_limiter = SlidingWindowRateLimiter(write_rate_limit, 1)

        session: Session = self._get_session(config)
        keyspace: str = self._keyspace
        table_name_version = online_store_config.table_name_format_version
        fqtable = CassandraOnlineStore._fq_table_name(
            keyspace, project, table, table_name_version
        )

        insert_cql = self._get_cql_statement(
            config,
            "insert4",
            fqtable=fqtable,
            ttl=ttl,
            session=session,
        )

        for entity_key, values, timestamp, created_ts in data:
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            for feature_name, val in values.items():
                params: Tuple[str, bytes, str, datetime] = (
                    feature_name,
                    val.SerializeToString(),
                    entity_key_bin,
                    timestamp,
                )
                batch.add(insert_cql, params)

            # Wait until the rate limiter allows
            if not rate_limiter.acquire():
                logger.warning(
                    f"Rate limit {write_rate_limit} exceeded. Waiting for reset."
                )
                while not rate_limiter.acquire():
                    time.sleep(0.001)

            future = session.execute_async(batch)
            concurrent_queue.put(future)
            future.add_callbacks(
                partial(
                    on_success,
                    concurrent_queue=concurrent_queue,
                ),
                partial(
                    on_failure,
                    concurrent_queue=concurrent_queue,
                ),
            )

            # this happens N-1 times, will be corrected outside:
            if progress:
                progress(1)
        # Wait for all futures to complete
        if not concurrent_queue.empty():
            logger.warning(
                f"Waiting for futures. Pending are {concurrent_queue.qsize()}"
            )
            while not concurrent_queue.empty():
                time.sleep(0.001)
            # Spark materialization engine doesn't log info messages
            # so we print the message to stdout
            print("Completed writing all futures.")

        # correction for the last missing call to `progress`:
        if progress:
            progress(1)

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Read feature values pertaining to the requested entities from
        the online store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            table: Feast FeatureView.
            entity_keys: a list of entity keys that should be read
                         from the FeatureStore.
        """
        project = config.project

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        entity_key_bins = [
            serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            for entity_key in entity_keys
        ]

        feature_rows_sequence = self._read_rows_by_entity_keys(
            config,
            project,
            table,
            entity_key_bins,
            columns=["feature_name", "value", "event_ts"],
        )

        for entity_key_bin, feature_rows in zip(entity_key_bins, feature_rows_sequence):
            res = {}
            res_ts = None
            if feature_rows:
                for feature_row in feature_rows:
                    if (
                        requested_features is None
                        or feature_row.feature_name in requested_features
                    ):
                        val = ValueProto()
                        val.ParseFromString(feature_row.value)
                        res[feature_row.feature_name] = val
                        res_ts = feature_row.event_ts
            if not res:
                result.append((None, None))
            else:
                result.append((res_ts, res))
        return result

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        Update schema on DB, by creating and destroying tables accordingly.

        Args:
            config: The RepoConfig for the current FeatureStore.
            tables_to_delete: Tables to delete from the Online Store.
            tables_to_keep: Tables to keep in the Online Store.
        """
        project = config.project

        for table in tables_to_keep:
            self._create_table(config, project, table)
        for table in tables_to_delete:
            self._drop_table(config, project, table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Delete tables from the database.

        Args:
            config: The RepoConfig for the current FeatureStore.
            tables: Tables to delete from the feature repo.
        """
        project = config.project

        for table in tables:
            self._drop_table(config, project, table)

    @staticmethod
    def _fq_table_name_v2(keyspace: str, project: str, feature_view_name: str) -> str:
        """
        Generate a fully-qualified table name,
        including quotes and keyspace.
        Limits table name to 48 characters.
        """
        db_table_name = f"{project}_{feature_view_name}"

        def base62encode(input: bytes) -> str:
            """Convert bytes to a base62 string."""

            def to_base62(num):
                """
                Converts a number to a base62 string.
                """
                chars = string.digits + string.ascii_lowercase + string.ascii_uppercase
                if num == 0:
                    return "0"

                result = []
                while num:
                    num, remainder = divmod(num, 62)
                    result.append(chars[remainder])

                return "".join(reversed(result))

            byte_to_num = int.from_bytes(input, byteorder="big")
            return to_base62(byte_to_num)

        if len(db_table_name) <= V2_TABLE_NAME_FORMAT_MAX_LENGTH:
            return f'"{keyspace}"."{db_table_name}"'
        else:
            # we are using quotes for table name to make it case sensitive.
            # So we can pack more bytes into the string by including capital letters.
            # So using base62(0-9a-zA-Z) encoding instead of base36 (0-9a-z) encoding.
            prj_prefix_maxlen = 5
            fv_prefix_maxlen = 5
            truncated_project = project[:prj_prefix_maxlen]
            truncated_fv = feature_view_name[:fv_prefix_maxlen]

            project_to_hash = project[len(truncated_project) :]
            fv_to_hash = feature_view_name[len(truncated_fv) :]

            project_hash = base62encode(hashlib.md5(project_to_hash.encode()).digest())
            fv_hash = base62encode(hashlib.md5(fv_to_hash.encode()).digest())

            # (48 - 3 underscores - 5 prj prefix - 5 fv prefix) / 2 = 17.5
            db_table_name = (
                f"{truncated_project}_{project_hash[:17]}_{truncated_fv}_{fv_hash[:18]}"
            )
            return f'"{keyspace}"."{db_table_name}"'

    @staticmethod
    def _fq_table_name(
        keyspace: str, project: str, table: FeatureView, table_name_version: int
    ) -> str:
        """
        Generate a fully-qualified table name,
        including quotes and keyspace.
        """
        feature_view_name = table.name
        db_table_name = f"{project}_{feature_view_name}"
        if table_name_version == 1:
            return f'"{keyspace}"."{db_table_name}"'

        elif table_name_version == 2:
            return CassandraOnlineStore._fq_table_name_v2(
                keyspace, project, feature_view_name
            )
        else:
            raise ValueError(f"Unknown table name format version: {table_name_version}")

    def _read_rows_by_entity_keys(
        self,
        config: RepoConfig,
        project: str,
        table: FeatureView,
        entity_key_bins: List[str],
        columns: Optional[List[str]] = None,
    ) -> ResultSet:
        """
        Handle the CQL (low-level) reading of feature values from a table.
        """
        session: Session = self._get_session(config)
        keyspace: str = self._keyspace
        table_name_version = config.online_store.table_name_format_version
        fqtable = CassandraOnlineStore._fq_table_name(
            keyspace, project, table, table_name_version
        )
        projection_columns = "*" if columns is None else ", ".join(columns)
        select_cql = self._get_cql_statement(
            config,
            "select",
            fqtable=fqtable,
            columns=projection_columns,
        )
        retrieval_results = execute_concurrent_with_args(
            session,
            select_cql,
            ((entity_key_bin,) for entity_key_bin in entity_key_bins),
            concurrency=config.online_store.read_concurrency,
        )
        # execute_concurrent_with_args return a sequence
        # of (success, result_or_exception) pairs:
        returned_sequence = []
        for success, result_or_exception in retrieval_results:
            if success:
                returned_sequence.append(result_or_exception)
            else:
                # an exception
                logger.error(
                    f"Cassandra online store exception during concurrent fetching: {str(result_or_exception)}"
                )
                returned_sequence.append(None)
        return returned_sequence

    def _drop_table(
        self,
        config: RepoConfig,
        project: str,
        table: FeatureView,
    ):
        """Handle the CQL (low-level) deletion of a table."""
        session: Session = self._get_session(config)
        keyspace: str = self._keyspace
        table_name_version = config.online_store.table_name_format_version
        fqtable = CassandraOnlineStore._fq_table_name(
            keyspace, project, table, table_name_version
        )
        drop_cql = self._get_cql_statement(config, "drop", fqtable)
        logger.info(f"Deleting table {fqtable}.")
        session.execute(drop_cql)

    def _create_table(
        self,
        config: RepoConfig,
        project: str,
        table: Union[FeatureView, SortedFeatureView],
    ):
        """Handle the CQL (low-level) creation of a table."""
        session: Session = self._get_session(config)
        keyspace: str = self._keyspace
        table_name_version = config.online_store.table_name_format_version
        fqtable = CassandraOnlineStore._fq_table_name(
            keyspace, project, table, table_name_version
        )
        if isinstance(table, SortedFeatureView):
            create_cql = self._build_sorted_table_cql(project, table, fqtable)
        else:
            create_cql = self._get_cql_statement(
                config,
                "create",
                fqtable,
                project=project,
                feature_view=table.name,
            )
        logger.info(
            f"Creating table {fqtable} in keyspace {keyspace} if not exists using {create_cql}."
        )
        session.execute(create_cql)

    def _build_sorted_table_cql(
        self, project: str, table: SortedFeatureView, fqtable: str
    ) -> str:
        """
        Build the CQL statement for creating a SortedFeatureView table with custom
        entity and sort key columns.
        """

        feature_columns = [
            f"{feature.name} {self._get_cql_type(feature.dtype)}"
            for feature in table.features
        ]
        feature_columns_str = ",".join(feature_columns)

        ts_field = getattr(table.batch_source, "timestamp_field", None)

        sorted_keys = [
            (
                "event_ts" if sk.name == ts_field else sk.name,
                "ASC" if sk.default_sort_order == SortOrder.Enum.ASC else "DESC",
            )
            for sk in table.sort_keys
        ]

        sort_key_names = ", ".join(name for name, _ in sorted_keys)
        clustering_order = ", ".join(f"{name} {order}" for name, order in sorted_keys)

        create_cql = (
            f"CREATE TABLE IF NOT EXISTS {fqtable} (\n"
            f"    entity_key TEXT,\n"
            f"    {feature_columns_str},\n"
            f"    event_ts TIMESTAMP,\n"
            f"    created_ts TIMESTAMP,\n"
            f"    PRIMARY KEY ((entity_key), {sort_key_names})\n"
            f") WITH CLUSTERING ORDER BY ({clustering_order})\n"
            f"AND COMMENT='project={project}, feature_view={table.name}';"
        )
        return create_cql.strip()

    def _get_cql_statement(
        self, config: RepoConfig, op_name: str, fqtable: str, **kwargs
    ):
        """
        Resolve an 'op_name' (create, insert4, etc) into a CQL statement
        ready to be bound to parameters when executing.

        If the statement is defined to be 'prepared', use an instance-specific
        cache of prepared statements.

        This additional layer makes it easy to control whether to use prepared
        statements and, if so, on which database operations.
        """
        session: Session = None
        if "session" in kwargs:
            session = kwargs["session"]
        else:
            session = self._get_session(config)

        template, prepare = CQL_TEMPLATE_MAP[op_name]
        statement = template.format(
            fqtable=fqtable,
            **kwargs,
        )
        if prepare:
            # using the statement itself as key (no problem with that)
            cache_key = statement
            if cache_key not in self._prepared_statements:
                logger.info(f"Preparing a {op_name} statement on {fqtable}.")
                self._prepared_statements[cache_key] = session.prepare(statement)
            return self._prepared_statements[cache_key]
        else:
            return statement

    def _get_cql_type(
        self, value_type: Union[ComplexFeastType, PrimitiveFeastType]
    ) -> str:
        """Map Feast value types to Cassandra CQL data types."""
        scalar_mapping = {
            Bytes: "BLOB",
            String: "TEXT",
            Int32: "INT",
            Int64: "BIGINT",
            Float32: "FLOAT",
            Float64: "DOUBLE",
            Bool: "BOOLEAN",
            UnixTimestamp: "TIMESTAMP",
            Array(Bytes): "LIST<BLOB>",
            Array(String): "LIST<TEXT>",
            Array(Int32): "LIST<INT>",
            Array(Int64): "LIST<BIGINT>",
            Array(Float32): "LIST<FLOAT>",
            Array(Float64): "LIST<DOUBLE>",
            Array(Bool): "LIST<BOOLEAN>",
            Array(UnixTimestamp): "LIST<TIMESTAMP>",
        }

        if value_type in scalar_mapping:
            return scalar_mapping[value_type]
        else:
            raise ValueError(f"Unsupported type: {value_type}")
