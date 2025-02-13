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
import time
from datetime import datetime
from functools import partial
from queue import Queue
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

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

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.rate_limiter import SlidingWindowRateLimiter
from feast.repo_config import FeastConfigBaseModel

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

SCYLLADB_MAX_TABLE_NAME_LENGTH = 48

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
    """Request timeout in seconds."""

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
        fqtable = CassandraOnlineStore._fq_table_name(keyspace, project, table)

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
        # Wait for all tasks to complete
        while not concurrent_queue.empty():
            time.sleep(0.001)

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
    def _fq_table_name(keyspace: str, project: str, table: FeatureView) -> str:
        """
        Generate a fully-qualified table name,
        including quotes and keyspace.
        Scylladb has a limit of 48 characters for table names.
        """
        feature_view_name = table.name
        db_table_name = f"{project}_{feature_view_name}"
        if len(db_table_name) <= SCYLLADB_MAX_TABLE_NAME_LENGTH:
            return f'"{keyspace}"."{db_table_name}"'

        project_hash = hashlib.md5(project.encode()).hexdigest()[:7]
        table_name_hash = hashlib.md5(db_table_name.encode()).hexdigest()[:8]

        # Shorten table name if it exceeds 30 characters
        if len(feature_view_name) > 30:
            feature_view_name = feature_view_name[:30]

        # Table name should start with a character so we are adding 'p' as prefix.
        # p represents project.
        return f'"{keyspace}"."p{project_hash}_{feature_view_name}_{table_name_hash}"'

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
        fqtable = CassandraOnlineStore._fq_table_name(keyspace, project, table)
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
        fqtable = CassandraOnlineStore._fq_table_name(keyspace, project, table)
        drop_cql = self._get_cql_statement(config, "drop", fqtable)
        logger.info(f"Deleting table {fqtable}.")
        session.execute(drop_cql)

    def _create_table(self, config: RepoConfig, project: str, table: FeatureView):
        """Handle the CQL (low-level) creation of a table."""
        session: Session = self._get_session(config)
        keyspace: str = self._keyspace
        fqtable = CassandraOnlineStore._fq_table_name(keyspace, project, table)
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
