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

import logging
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
)

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
from cassandra.query import PreparedStatement
from pydantic import StrictFloat, StrictInt, StrictStr

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
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
    " (?, ?, ?, ?);"
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
    ) WITH CLUSTERING ORDER BY (feature_name ASC);
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

    type: Literal["cassandra"] = "cassandra"
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
    Value of the `concurrency` parameter internally passed to Cassandra driver's
    `execute_concurrent_with_args` call when writing rows to tables.
    See https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/concurrent/#module-cassandra.concurrent .
    Default: 100.
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
            return self._session
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
                    hosts, port=port, auth_provider=auth_provider, **cluster_kwargs
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
        One may be tempted to reclaim resources and do, here:
            if self._session:
                self._session.shutdown()
        But *beware*, DON'T DO THIS.
        Indeed this could destroy the session object before some internal
        tasks runs in other threads (this is handled internally in the
        Cassandra driver).
        You'd get a RuntimeError "cannot schedule new futures after shutdown".
        """
        pass

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
        project = config.project

        def unroll_insertion_tuples() -> Iterable[Tuple[str, bytes, str, datetime]]:
            """
            We craft an iterable over all rows to be inserted (entities->features),
            but this way we can call `progress` after each entity is done.
            """
            for entity_key, values, timestamp, created_ts in data:
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
                    yield params
                # this happens N-1 times, will be corrected outside:
                if progress:
                    progress(1)

        self._write_rows_concurrently(
            config,
            project,
            table,
            unroll_insertion_tuples(),
        )
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
        """
        return f'"{keyspace}"."{project}_{table.name}"'

    def _write_rows_concurrently(
        self,
        config: RepoConfig,
        project: str,
        table: FeatureView,
        rows: Iterable[Tuple[str, bytes, str, datetime]],
    ):
        session: Session = self._get_session(config)
        keyspace: str = self._keyspace
        fqtable = CassandraOnlineStore._fq_table_name(keyspace, project, table)
        insert_cql = self._get_cql_statement(config, "insert4", fqtable=fqtable)
        #
        execute_concurrent_with_args(
            session,
            insert_cql,
            rows,
            concurrency=config.online_store.write_concurrency,
        )

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
        create_cql = self._get_cql_statement(config, "create", fqtable)
        logger.info(f"Creating table {fqtable}.")
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
        session: Session = self._get_session(config)
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
