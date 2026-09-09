import logging
import warnings
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
    Union,
)

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    Session,
)
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import ConsistencyLevel, PreparedStatement
from pydantic import StrictFloat, StrictInt, StrictStr

from feast import Entity, FeatureView, RepoConfig
from feast.filter_models import ComparisonFilter, CompoundFilter
from feast.infra.key_encoding_utils import deserialize_entity_key, serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Error messages
# ---------------------------------------------------------------------------

E_SCYLLA_UNEXPECTED_CONFIG = (
    "Unexpected configuration object (not a ScyllaDBOnlineStoreConfig instance)"
)
E_SCYLLA_NOT_CONFIGURED = (
    "Inconsistent ScyllaDB configuration: 'hosts' and 'keyspace' are required"
)
E_SCYLLA_INCONSISTENT_AUTH = (
    "ScyllaDB username and password must be provided together or not at all"
)

# ---------------------------------------------------------------------------
# CQL templates
# ---------------------------------------------------------------------------


def _build_create_table_cql(fqtable: str, dim: Optional[int]) -> str:
    """Return a CREATE TABLE CQL string, optionally including the vector column.

    When *dim* is given a ``vector_value vector<float, dim>`` column is inserted
    between the BLOB ``value`` column and ``event_ts``.  When *dim* is ``None``
    the column is omitted entirely and the table schema is fully static.
    """
    vec_col = f"vector_value vector<float, {dim}>,\n" if dim else ""
    return (
        f"CREATE TABLE IF NOT EXISTS {fqtable} (\n"
        f"    entity_key   TEXT,\n"
        f"    feature_name TEXT,\n"
        f"    value        BLOB,\n"
        f"    {vec_col}"
        f"    event_ts     TIMESTAMP,\n"
        f"    created_ts   TIMESTAMP,\n"
        f"    PRIMARY KEY ((entity_key), feature_name)\n"
        f");"
    )


DROP_TABLE_CQL = "DROP TABLE IF EXISTS {fqtable};"

INSERT_CQL = (
    "INSERT INTO {fqtable} (feature_name, value, entity_key, event_ts, created_ts)"
    " VALUES (?, ?, ?, ?, ?);"
)

INSERT_CQL_TTL = (
    "INSERT INTO {fqtable} (feature_name, value, entity_key, event_ts, created_ts)"
    " VALUES (?, ?, ?, ?, ?) USING TTL ?;"
)

# INSERT for vector feature rows: populates both the BLOB value (for online_read
# compatibility) and the native vector_value column (for ANN search).
# No TTL variant, ScyllaDB ignores TTL on vector-indexed columns,
# a future release will fix this,
# combining TTL with vector features is rejected at feast apply time.
INSERT_VEC_CQL = (
    "INSERT INTO {fqtable}"
    " (feature_name, value, entity_key, event_ts, created_ts, vector_value)"
    " VALUES (?, ?, ?, ?, ?, ?);"
)

SELECT_CQL = "SELECT {columns} FROM {fqtable} WHERE entity_key = ?;"

# Global vector index on the main feature table.
CREATE_VECTOR_INDEX_CQL = (
    "CREATE CUSTOM INDEX IF NOT EXISTS {index_name}"
    " ON {fqtable} (vector_value)"
    " USING 'vector_index'"
    " WITH OPTIONS = {{'similarity_function': '{sim_func}'}};"
)

# ANN query on the main feature table.
# ALLOW FILTERING is required because feature_name is a clustering key.
ANN_SELECT_CQL = (
    "SELECT entity_key, {sim_func_call} AS score, event_ts"
    " FROM {fqtable}"
    " WHERE feature_name = ?"
    " ORDER BY vector_value ANN OF ?"
    " LIMIT ?"
    " ALLOW FILTERING;"
)

# op_name -> (template, prepare?)
_CQL_TEMPLATES: Dict[str, Tuple[str, bool]] = {
    "drop": (DROP_TABLE_CQL, False),
    "insert": (INSERT_CQL, True),
    "insert_ttl": (INSERT_CQL_TTL, True),
    "select": (SELECT_CQL, True),
}

# Similarity function CQL expression helpers (vector_value is the fixed column name)
_SIM_FUNC_EXPR = {
    "COSINE": "similarity_cosine(vector_value, ?)",
    "DOT_PRODUCT": "similarity_dot_product(vector_value, ?)",
    "EUCLIDEAN": "similarity_euclidean(vector_value, ?)",
}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class ScyllaDBOnlineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the native ScyllaDB online store.

    Requires ``scylla-driver`` (``pip install scylla-driver``).

    Example ``feature_store.yaml``::

        online_store:
          type: feast_scylladb.ScyllaDBOnlineStore
          hosts:
            - node-0.aws_us_east_1.xxxxxxxx.clusters.scylla.cloud
          keyspace: feast
          username: scylla
          password: pass
          local_dc: AWS_US_EAST_1
    """

    type: Literal[
        "scylladb",
        "feast.infra.online_stores.scylladb_online_store.scylladb.ScyllaDBOnlineStore",
    ] = "scylladb"

    # Connection
    hosts: List[StrictStr]
    """Contact-point host addresses."""

    port: Optional[StrictInt] = 9042
    """CQL port (default 9042)."""

    keyspace: StrictStr = "feast_keyspace"
    """Target ScyllaDB keyspace."""

    username: Optional[StrictStr] = None
    """Auth username."""

    password: Optional[StrictStr] = None
    """Auth password."""

    local_dc: Optional[StrictStr] = None
    """
    Local datacenter name.
    For ScyllaDB Cloud this is the region string, e.g. ``AWS_US_EAST_1``.
    """

    request_timeout: Optional[StrictFloat] = None
    """Driver request timeout in seconds."""

    read_concurrency: Optional[StrictInt] = 100
    """Concurrency level passed to ``execute_concurrent_with_args`` for reads."""

    write_concurrency: Optional[StrictInt] = 100
    """Concurrency level passed to ``execute_concurrent_with_args`` for writes."""

    vector_similarity_function: StrictStr = "COSINE"
    """
    Default similarity function used when creating vector indexes.
    Can be overridden per-feature via the ``similarity_function`` Field tag.
    Supported values: ``COSINE``, ``DOT_PRODUCT``, ``EUCLIDEAN``.
    """


# ---------------------------------------------------------------------------
# Vector feature helpers
# ---------------------------------------------------------------------------


def _get_vector_features(
    table: FeatureView, default_sim_func: str
) -> List[Tuple[str, int, str]]:
    """
    Return ``(feature_name, dimension, similarity_function)`` for every feature
    in *table* tagged as a vector feature.

    A feature is treated as a vector feature when its tags include::

        "vector_index": "true"
        "dimensions":   "<int>"

    The similarity function defaults to *default_sim_func* but can be
    overridden per-feature with a ``"similarity_function"`` tag.
    """
    result = []
    for field in table.schema:
        tags = field.tags or {}
        if tags.get("vector_index", "").lower() != "true":
            continue
        dim_str = tags.get("dimensions", "")
        if not dim_str:
            warnings.warn(
                f"Feature '{field.name}' in FeatureView '{table.name}' is tagged "
                "vector_index=true but is missing a 'dimensions' tag. "
                "Skipping vector table creation for this feature.",
                UserWarning,
                stacklevel=2,
            )
            continue
        sim_func = tags.get("similarity_function", default_sim_func).upper()
        result.append((field.name, int(dim_str), sim_func))
    return result


# ---------------------------------------------------------------------------
# Store implementation
# ---------------------------------------------------------------------------


class ScyllaDBInvalidConfig(Exception):
    pass


class ScyllaDBOnlineStore(OnlineStore):
    """
    Native ScyllaDB online store for Feast.

    Supports both regular feature materialisation and vector similarity search
    via ScyllaDB's ANN / ``vector_index`` functionality.

    **Vector features** — tag your ``Field`` definitions like this::

        from feast import Field
        from feast.types import Array, Float32

        Field(
            name="embedding",
            dtype=Array(Float32),
            tags={
                "vector_index":       "true",
                "dimensions":         "768",
                "similarity_function": "COSINE",   # optional, default COSINE
            },
        )

    Then call ``FeatureStore.retrieve_online_documents_v2(...)`` to perform an
    approximate nearest-neighbour search.
    """

    _cluster: Optional[Cluster] = None
    _session: Optional[Session] = None
    _keyspace: str = "feast_keyspace"
    _prepared_statements: Dict[str, PreparedStatement] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_session(self, config: RepoConfig) -> Session:
        """Return the active session, creating the cluster connection if needed."""
        online_store_config = config.online_store
        if not isinstance(online_store_config, ScyllaDBOnlineStoreConfig):
            raise ScyllaDBInvalidConfig(E_SCYLLA_UNEXPECTED_CONFIG)

        if self._session:
            return self._session

        hosts = online_store_config.hosts
        port = online_store_config.port or 9042
        keyspace = online_store_config.keyspace
        username = online_store_config.username
        password = online_store_config.password
        local_dc = online_store_config.local_dc

        if not hosts or not keyspace:
            raise ScyllaDBInvalidConfig(E_SCYLLA_NOT_CONFIGURED)
        if (username is None) ^ (password is None):
            raise ScyllaDBInvalidConfig(E_SCYLLA_INCONSISTENT_AUTH)

        auth_provider = (
            PlainTextAuthProvider(username=username, password=password)
            if username is not None
            else None
        )

        # Build execution profile
        if local_dc:
            lb_policy: Any = TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=local_dc)
            )
            exe_profile = ExecutionProfile(
                request_timeout=online_store_config.request_timeout,
                load_balancing_policy=lb_policy,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            )
            execution_profiles: Optional[Dict] = {EXEC_PROFILE_DEFAULT: exe_profile}
        elif online_store_config.request_timeout is not None:
            exe_profile = ExecutionProfile(
                request_timeout=online_store_config.request_timeout,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            )
            execution_profiles = {EXEC_PROFILE_DEFAULT: exe_profile}
        else:
            exe_profile = ExecutionProfile(
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            )
            execution_profiles = {EXEC_PROFILE_DEFAULT: exe_profile}

        cluster_kwargs: Dict[str, Any] = {
            k: v
            for k, v in {
                "execution_profiles": execution_profiles,
            }.items()
            if v is not None
        }

        self._cluster = Cluster(
            hosts,
            port=port,
            auth_provider=auth_provider,
            **cluster_kwargs,
        )
        self._keyspace = keyspace
        # Connect without a keyspace first so we can create it if needed.
        session = self._cluster.connect()
        session.execute(
            f'CREATE KEYSPACE IF NOT EXISTS "{keyspace}"'
            " WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '3'};"
        )
        session.set_keyspace(keyspace)
        self._session = session
        return self._session

    @staticmethod
    def _fq_table_name(keyspace: str, project: str, table: FeatureView) -> str:
        return f'"{keyspace}"."{project}_{table.name}"'

    def _get_statement(
        self, config: RepoConfig, op_name: str, fqtable: str, **kwargs: Any
    ) -> Any:
        """
        Resolve *op_name* to a CQL statement, preparing and caching it when the
        template is marked as prepareable.
        """
        session = self._get_session(config)
        template, do_prepare = _CQL_TEMPLATES[op_name]
        cql = template.format(fqtable=fqtable, **kwargs)
        if do_prepare:
            if cql not in self._prepared_statements:
                logger.info("Preparing %s statement on %s.", op_name, fqtable)
                self._prepared_statements[cql] = session.prepare(cql)
            return self._prepared_statements[cql]
        return cql

    # ------------------------------------------------------------------
    # Table lifecycle helpers
    # ------------------------------------------------------------------

    def _create_table(
        self,
        config: RepoConfig,
        project: str,
        table: FeatureView,
        vec_features: Optional[List[Tuple[str, int, str]]] = None,
    ) -> None:
        session = self._get_session(config)
        fqtable = self._fq_table_name(self._keyspace, project, table)
        dim = vec_features[0][1] if vec_features else None
        logger.info("Creating table %s.", fqtable)
        session.execute(_build_create_table_cql(fqtable, dim))

        for _feat_name, _dim, sim_func in vec_features or []:
            index_name = f"{project}_{table.name}_vec_idx"
            logger.info("Creating vector index %s on %s.", index_name, fqtable)
            session.execute(
                CREATE_VECTOR_INDEX_CQL.format(
                    index_name=index_name,
                    fqtable=fqtable,
                    sim_func=sim_func,
                )
            )

    def _drop_table(
        self,
        config: RepoConfig,
        project: str,
        table: FeatureView,
        vec_features: Optional[List[Tuple[str, int, str]]] = None,
    ) -> None:
        session = self._get_session(config)
        fqtable = self._fq_table_name(self._keyspace, project, table)
        logger.info("Dropping table %s.", fqtable)
        session.execute(DROP_TABLE_CQL.format(fqtable=fqtable))

    # ------------------------------------------------------------------
    # OnlineStore interface — infrastructure
    # ------------------------------------------------------------------

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ) -> None:
        """Create and drop feature-store tables as required by the registry."""
        project = config.project
        online_store_config = config.online_store
        assert isinstance(online_store_config, ScyllaDBOnlineStoreConfig)
        default_sim = online_store_config.vector_similarity_function

        for table in tables_to_keep:
            vec_features = _get_vector_features(table, default_sim)
            if vec_features and table.ttl:
                raise ValueError(
                    f"FeatureView '{table.name}' has both a TTL and vector features. "
                    "ScyllaDB does not support TTL on vector-indexed columns. "
                    "Remove the TTL or the vector_index tag."
                )
            self._create_table(config, project, table, vec_features=vec_features)

        for table in tables_to_delete:
            vec_features = _get_vector_features(table, default_sim)
            self._drop_table(config, project, table, vec_features=vec_features)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        """Drop all tables for the given feature views."""
        project = config.project
        online_store_config = config.online_store
        assert isinstance(online_store_config, ScyllaDBOnlineStoreConfig)

        for table in tables:
            vec_features = _get_vector_features(
                table, online_store_config.vector_similarity_function
            )
            self._drop_table(config, project, table, vec_features=vec_features)

    # ------------------------------------------------------------------
    # OnlineStore interface — write
    # ------------------------------------------------------------------

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
        Write a batch of feature rows to the online store.

        All features are written to the main table.  For vector features,
        the float-list value is additionally written to the dedicated vector
        table to support ANN search.
        """
        project = config.project
        online_store_config = config.online_store
        assert isinstance(online_store_config, ScyllaDBOnlineStoreConfig)
        default_sim = online_store_config.vector_similarity_function
        vec_features = _get_vector_features(table, default_sim)

        # Compute TTL in seconds from the FeatureView's ttl field (timedelta or None).
        # ScyllaDB will automatically expire rows after this many seconds, covering
        # both "support for ttl at retrieval" and "support for deleting expired data".
        ttl_seconds: Optional[int] = (
            int(table.ttl.total_seconds()) if table.ttl else None
        )

        vec_feature_set = {fn for fn, _, _ in vec_features}
        fqtable = self._fq_table_name(self._keyspace, project, table)
        insert_op = "insert_ttl" if ttl_seconds is not None else "insert"
        insert_stmt = self._get_statement(config, insert_op, fqtable)

        session = self._get_session(config)

        # --- non-vector rows (all features except vector features) ---
        def _main_rows() -> Iterable[Tuple]:
            for entity_key, values, timestamp, created_ts in data:
                entity_key_bin = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                ).hex()
                for feature_name, val in values.items():
                    if feature_name in vec_feature_set:
                        continue  # written separately via INSERT_VEC_CQL
                    row: Tuple = (
                        feature_name,
                        val.SerializeToString(),
                        entity_key_bin,
                        timestamp,
                        created_ts,
                    )
                    if ttl_seconds is not None:
                        row = row + (ttl_seconds,)
                    yield row
                if progress:
                    progress(1)

        execute_concurrent_with_args(
            session,
            insert_stmt,
            _main_rows(),
            concurrency=online_store_config.write_concurrency,
        )
        # correction for the last missing call to `progress`:
        if progress:
            progress(1)

        # --- vector rows ---
        # TTL is not used here: TTL + vector features is rejected at feast apply time,
        # so this path is only reached for TTL-free feature views.
        if vec_features:
            vec_insert_cql = INSERT_VEC_CQL.format(fqtable=fqtable)
            if vec_insert_cql not in self._prepared_statements:
                self._prepared_statements[vec_insert_cql] = session.prepare(
                    vec_insert_cql
                )
            vec_insert_stmt = self._prepared_statements[vec_insert_cql]

            def _vec_rows() -> Iterable[Tuple]:
                for entity_key, values, timestamp, created_ts in data:
                    entity_key_bin = serialize_entity_key(
                        entity_key,
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    ).hex()
                    for feat_name in vec_feature_set:
                        if feat_name not in values:
                            continue
                        val = values[feat_name]
                        yield (
                            feat_name,
                            val.SerializeToString(),
                            entity_key_bin,
                            timestamp,
                            created_ts,
                            list(val.float_list_val.val),
                        )

            execute_concurrent_with_args(
                session,
                vec_insert_stmt,
                _vec_rows(),
                concurrency=online_store_config.write_concurrency,
            )

    # ------------------------------------------------------------------
    # OnlineStore interface — read
    # ------------------------------------------------------------------

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """Read feature values for the given entity keys from the regular table."""
        project = config.project
        online_store_config = config.online_store
        assert isinstance(online_store_config, ScyllaDBOnlineStoreConfig)

        entity_key_bins = [
            serialize_entity_key(
                ek,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            for ek in entity_keys
        ]

        fqtable = self._fq_table_name(self._keyspace, project, table)
        select_stmt = self._get_statement(
            config, "select", fqtable, columns="feature_name, value, event_ts"
        )

        session = self._get_session(config)
        retrieval = execute_concurrent_with_args(
            session,
            select_stmt,
            ((ek_bin,) for ek_bin in entity_key_bins),
            concurrency=online_store_config.read_concurrency,
        )

        results: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for success, rows_or_exc in retrieval:
            if not success:
                logger.error(
                    "ScyllaDB read error during concurrent fetch: %s", rows_or_exc
                )
                results.append((None, None))
                continue
            res: Dict[str, ValueProto] = {}
            res_ts: Optional[datetime] = None
            for row in rows_or_exc:
                if requested_features is None or row.feature_name in requested_features:
                    val = ValueProto()
                    val.ParseFromString(row.value)
                    res[row.feature_name] = val
                    res_ts = row.event_ts
            results.append((res_ts, res) if res else (None, None))

        return results

    # ------------------------------------------------------------------
    # Vector store interface
    # ------------------------------------------------------------------

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
        filters: Optional[Union[ComparisonFilter, CompoundFilter]] = None,
        include_feature_view_version_metadata: bool = False,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        """
        Approximate nearest-neighbour search using ScyllaDB's vector_index.

        The feature view must have exactly one feature tagged with
        ``vector_index=true``.  The query *embedding* is compared against all
        stored vectors; the top *top_k* results are returned together with
        their feature values fetched from the regular feature table.

        Args:
            config: RepoConfig for the current FeatureStore.
            table: FeatureView to search.
            requested_features: Feature names to include in the returned dicts.
            embedding: Query vector.  Must match the indexed column's dimension.
            top_k: Number of results to return.
            distance_metric: Override similarity function
                (``COSINE`` / ``DOT_PRODUCT`` / ``EUCLIDEAN``).  Defaults to the
                store-level ``vector_similarity_function`` config value or the
                per-feature tag.
            query_string: Unused (reserved for future hybrid text+vector search).
            filters: Unused (metadata filtering not yet supported for ScyllaDB).
            include_feature_view_version_metadata: Unused.

        Returns:
            List of ``(event_timestamp, entity_key_proto, feature_values)``
            tuples ordered from most to least similar.
        """
        if filters is not None:
            raise NotImplementedError(
                "Metadata filtering is not supported by the ScyllaDB online store."
            )
        if embedding is None:
            raise ValueError(
                "retrieve_online_documents_v2 requires a non-None 'embedding' "
                "for ScyllaDB ANN search."
            )

        online_store_config = config.online_store
        assert isinstance(online_store_config, ScyllaDBOnlineStoreConfig)

        default_sim = (
            distance_metric.upper()
            if distance_metric
            else online_store_config.vector_similarity_function
        )
        project = config.project
        vec_features = _get_vector_features(table, default_sim)

        if not vec_features:
            raise NotImplementedError(
                f"FeatureView '{table.name}' has no features tagged with "
                "'vector_index=true'.  Cannot perform vector search."
            )
        if len(vec_features) > 1:
            raise ValueError(
                f"FeatureView '{table.name}' has {len(vec_features)} vector features. "
                "retrieve_online_documents_v2 supports exactly one vector feature "
                "per feature view."
            )

        vec_feature, _dim, sim_func = vec_features[0]
        if distance_metric:
            # Normalize aliases: Feast core uses "L2" for Euclidean distance;
            # "cosine" (lowercase) is the default in feature_store.py;
            # "inner_product" / "dot" are aliases for dot-product similarity.
            _METRIC_ALIASES = {
                "L2": "EUCLIDEAN",
                "cosine": "COSINE",
                "inner_product": "DOT_PRODUCT",
                "dot": "DOT_PRODUCT",
            }
            candidate = distance_metric.upper()
            sim_func = _METRIC_ALIASES.get(
                distance_metric, _METRIC_ALIASES.get(candidate, candidate)
            )

        sim_expr_template = _SIM_FUNC_EXPR.get(sim_func)
        if sim_expr_template is None:
            raise ValueError(
                f"Unsupported similarity function '{sim_func}'. "
                "Choose from: COSINE, DOT_PRODUCT, EUCLIDEAN."
            )
        sim_expr = sim_expr_template

        fqtable = self._fq_table_name(self._keyspace, project, table)
        ann_cql = ANN_SELECT_CQL.format(
            sim_func_call=sim_expr,
            fqtable=fqtable,
        )

        session = self._get_session(config)
        if ann_cql not in self._prepared_statements:
            self._prepared_statements[ann_cql] = session.prepare(ann_cql)
        ann_stmt = self._prepared_statements[ann_cql]
        # Parameter order matches the placeholders in ANN_SELECT_CQL:
        #   1. similarity_<func>(vector_value, ?)  → embedding
        #   2. WHERE feature_name = ?              → vec_feature
        #   3. ORDER BY vector_value ANN OF ?      → embedding
        #   4. LIMIT ?                             → top_k
        ann_rows = list(
            session.execute(ann_stmt, (embedding, vec_feature, embedding, top_k))
        )

        if not ann_rows:
            return []

        # Ordered list of entity key bins from ANN results
        entity_key_bins: List[str] = [row.entity_key for row in ann_rows]
        timestamps: Dict[str, Optional[datetime]] = {
            row.entity_key: row.event_ts for row in ann_rows
        }

        # Batch-fetch full feature values from the same main table
        select_stmt = self._get_statement(
            config, "select", fqtable, columns="feature_name, value, event_ts"
        )

        retrieval = execute_concurrent_with_args(
            session,
            select_stmt,
            ((ek_bin,) for ek_bin in entity_key_bins),
            concurrency=online_store_config.read_concurrency,
        )

        # Map entity_key_bin -> feature dict, preserving ANN order
        feature_map: Dict[str, Dict[str, ValueProto]] = {
            ek: {} for ek in entity_key_bins
        }
        for ek_bin, (success, rows_or_exc) in zip(entity_key_bins, retrieval):
            if not success:
                logger.error("ScyllaDB ANN batch-read error: %s", rows_or_exc)
                continue
            for row in rows_or_exc:
                if requested_features and row.feature_name not in requested_features:
                    continue
                val = ValueProto()
                val.ParseFromString(row.value)
                feature_map[ek_bin][row.feature_name] = val

        # Assemble output in ANN rank order
        output: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []
        for ek_bin in entity_key_bins:
            try:
                ek_proto: Optional[EntityKeyProto] = deserialize_entity_key(
                    bytes.fromhex(ek_bin),
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
            except Exception:
                ek_proto = None

            output.append(
                (
                    timestamps.get(ek_bin),
                    ek_proto,
                    feature_map.get(ek_bin, {}),
                )
            )

        return output
