import contextlib
import logging
from collections import defaultdict
from datetime import datetime
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Generator,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from psycopg import AsyncConnection, sql
from psycopg.connection import Connection
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from feast import Entity, FeatureView, ValueType
from feast.filter_models import ComparisonFilter, CompoundFilter
from feast.infra.key_encoding_utils import get_list_val_str, serialize_entity_key
from feast.infra.online_stores.helpers import (
    _to_naive_utc,
    compute_table_id,
    extract_text_and_num,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.infra.utils.postgres.connection_utils import (
    _get_conn,
    _get_conn_async,
    _get_connection_pool,
    _get_connection_pool_async,
)
from feast.infra.utils.postgres.postgres_config import ConnectionType, PostgreSQLConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.utils import _build_retrieve_online_document_record

SUPPORTED_DISTANCE_METRICS_DICT = {
    "cosine": "<=>",
    "L1": "<+>",
    "L2": "<->",
    "inner_product": "<#>",
}

_PG_COMPARISON_OPS: Dict[str, str] = {
    "eq": "=",
    "ne": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
}


class PostgreSQLOnlineStoreConfig(PostgreSQLConfig, VectorStoreConfig):
    type: Literal["postgres"] = "postgres"
    enable_openai_compatible_store: Optional[bool] = False


class PostgreSQLOnlineStore(OnlineStore):
    _conn: Optional[Connection] = None
    _conn_pool: Optional[ConnectionPool] = None

    _conn_async: Optional[AsyncConnection] = None
    _conn_pool_async: Optional[AsyncConnectionPool] = None
    _table_has_value_num: Optional[Dict[str, bool]] = None

    @contextlib.contextmanager
    def _get_conn(
        self, config: RepoConfig, autocommit: bool = False
    ) -> Generator[Connection, Any, Any]:
        assert config.online_store.type == "postgres"

        if config.online_store.conn_type == ConnectionType.pool:
            if not self._conn_pool:
                self._conn_pool = _get_connection_pool(config.online_store)
                self._conn_pool.open()
            connection = self._conn_pool.getconn()
            connection.set_autocommit(autocommit)
            yield connection
            self._conn_pool.putconn(connection)
        else:
            if not self._conn:
                self._conn = _get_conn(config.online_store)
            self._conn.set_autocommit(autocommit)
            yield self._conn

    @contextlib.asynccontextmanager
    async def _get_conn_async(
        self, config: RepoConfig, autocommit: bool = False
    ) -> AsyncGenerator[AsyncConnection, Any]:
        if config.online_store.conn_type == ConnectionType.pool:
            if not self._conn_pool_async:
                self._conn_pool_async = await _get_connection_pool_async(
                    config.online_store
                )
                await self._conn_pool_async.open()
            connection = await self._conn_pool_async.getconn()
            await connection.set_autocommit(autocommit)
            yield connection
            await self._conn_pool_async.putconn(connection)
        else:
            if not self._conn_async:
                self._conn_async = await _get_conn_async(config.online_store)
            await self._conn_async.set_autocommit(autocommit)
            yield self._conn_async

    def _check_table_has_value_num(
        self, cur, table_name: str, config: RepoConfig
    ) -> bool:
        """Check if the value_num column exists in the given table, with caching."""
        if self._table_has_value_num is None:
            self._table_has_value_num = {}
        if table_name in self._table_has_value_num:
            return self._table_has_value_num[table_name]

        schema_name = config.online_store.db_schema or config.online_store.user
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = 'value_num'
            """,
            (schema_name, table_name),
        )
        exists = cur.fetchone() is not None
        self._table_has_value_num[table_name] = exists
        return exists

    def _filters_need_value_num(
        self, filters: Union[ComparisonFilter, CompoundFilter]
    ) -> bool:
        """Check if any filter in the tree requires the value_num column."""
        if isinstance(filters, ComparisonFilter):
            value = filters.value
            if isinstance(value, list):
                if value:
                    col, _ = self._filter_col_and_val(value[0])
                    return col == "value_num"
                return False
            col, _ = self._filter_col_and_val(value)
            return col == "value_num"
        elif isinstance(filters, CompoundFilter):
            return any(self._filters_need_value_num(f) for f in filters.filters)
        return False

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        table_name = _table_id(
            config.project, table, config.registry.enable_online_feature_view_versioning
        )

        with self._get_conn(config) as conn, conn.cursor() as cur:
            enable_value_num = getattr(
                config.online_store, "enable_openai_compatible_store", False
            )
            has_value_num_col = self._check_table_has_value_num(cur, table_name, config)
            compute_value_num = has_value_num_col
            if enable_value_num and not has_value_num_col:
                logging.warning(
                    "enable_openai_compatible_store is True but value_num column "
                    "not found in table '%s'. Run `feast apply` to add it. "
                    "Writing without value_num.",
                    table_name,
                )
                compute_value_num = False

            columns = ["entity_key", "feature_name", "value", "value_text"]
            if has_value_num_col:
                columns.append("value_num")
            columns.extend(["vector_value", "event_ts", "created_ts"])

            pk_cols = {"entity_key", "feature_name"}
            col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
            placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))
            update_set = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in columns
                if c not in pk_cols
            )
            sql_query = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) "
                "ON CONFLICT (entity_key, feature_name) DO UPDATE SET {};"
            ).format(sql.Identifier(table_name), col_ids, placeholders, update_set)

            insert_values: List[Tuple[Any, ...]] = []
            for entity_key, values, timestamp, created_ts in data:
                entity_key_bin = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                timestamp = _to_naive_utc(timestamp)
                if created_ts is not None:
                    created_ts = _to_naive_utc(created_ts)

                for feature_name, val in values.items():
                    value_text, value_num = extract_text_and_num(val, compute_value_num)

                    vector_val = None
                    if config.online_store.vector_enabled:
                        vector_val = get_list_val_str(val)

                    row: List[Any] = [
                        entity_key_bin,
                        feature_name,
                        val.SerializeToString(),
                        value_text,
                    ]
                    if has_value_num_col:
                        row.append(value_num)
                    row.extend([vector_val, timestamp, created_ts])
                    insert_values.append(tuple(row))

            cur.executemany(sql_query, insert_values)
            conn.commit()

        if progress:
            progress(len(data))

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        keys = self._prepare_keys(entity_keys, config.entity_key_serialization_version)
        query, params = self._construct_query_and_params(
            config, table, keys, requested_features
        )

        with self._get_conn(config, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        return self._process_rows(keys, rows)

    async def online_read_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        keys = self._prepare_keys(entity_keys, config.entity_key_serialization_version)
        query, params = self._construct_query_and_params(
            config, table, keys, requested_features
        )

        async with self._get_conn_async(config, autocommit=True) as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                rows = await cur.fetchall()

        return self._process_rows(keys, rows)

    @staticmethod
    def _construct_query_and_params(
        config: RepoConfig,
        table: FeatureView,
        keys: List[bytes],
        requested_features: Optional[List[str]] = None,
    ) -> Tuple[sql.Composed, Union[Tuple[List[bytes], List[str]], Tuple[List[bytes]]]]:
        """Construct the SQL query based on the given parameters."""
        if requested_features:
            query = sql.SQL(
                """
                SELECT entity_key, feature_name, value, event_ts
                FROM {} WHERE entity_key = ANY(%s) AND feature_name = ANY(%s);
                """
            ).format(
                sql.Identifier(
                    _table_id(
                        config.project,
                        table,
                        config.registry.enable_online_feature_view_versioning,
                    )
                ),
            )
            params = (keys, requested_features)
        else:
            query = sql.SQL(
                """
                SELECT entity_key, feature_name, value, event_ts
                FROM {} WHERE entity_key = ANY(%s);
                """
            ).format(
                sql.Identifier(
                    _table_id(
                        config.project,
                        table,
                        config.registry.enable_online_feature_view_versioning,
                    )
                ),
            )
            params = (keys, [])
        return query, params

    @staticmethod
    def _prepare_keys(
        entity_keys: List[EntityKeyProto], entity_key_serialization_version: int
    ) -> List[bytes]:
        """Prepare all keys in a list to make fewer round trips to the database."""
        return [
            serialize_entity_key(
                entity_key,
                entity_key_serialization_version=entity_key_serialization_version,
            )
            for entity_key in entity_keys
        ]

    @staticmethod
    def _process_rows(
        keys: List[bytes], rows: List[Tuple]
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """Transform the retrieved rows in the desired output.

        PostgreSQL may return rows in an unpredictable order. Therefore, `values_dict`
        is created to quickly look up the correct row using the keys, since these are
        actually in the correct order.
        """
        values_dict = defaultdict(list)
        for row in rows if rows is not None else []:
            values_dict[
                row[0] if isinstance(row[0], bytes) else row[0].tobytes()
            ].append(row[1:])

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for key in keys:
            if key in values_dict:
                value = values_dict[key]
                res = {}
                for feature_name, value_bin, event_ts in value:
                    val = ValueProto()
                    val.ParseFromString(bytes(value_bin))
                    res[feature_name] = val
                result.append((event_ts, res))
            else:
                result.append((None, None))
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
        project = config.project
        schema_name = config.online_store.db_schema or config.online_store.user
        with self._get_conn(config) as conn, conn.cursor() as cur:
            # If a db_schema is provided, then that schema gets created if it doesn't
            # exist. Else a schema is created for the feature store user.

            cur.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = %s
                """,
                (schema_name,),
            )
            schema_exists = cur.fetchone()
            if not schema_exists:
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION {}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(config.online_store.user),
                    ),
                )

            versioning = config.registry.enable_online_feature_view_versioning
            for table in tables_to_delete:
                if versioning:
                    _drop_all_version_tables(cur, project, table, schema_name)
                else:
                    table_name = _table_id(project, table)
                    cur.execute(_drop_table_and_index(table_name))

            for table in tables_to_keep:
                table_name = _table_id(project, table, versioning)
                if config.online_store.vector_enabled:
                    vector_value_type = "vector"
                else:
                    # keep the vector_value_type as BYTEA if pgvector is not enabled, to maintain compatibility
                    vector_value_type = "BYTEA"

                has_string_features = any(
                    f.dtype.to_value_type() == ValueType.STRING for f in table.features
                )

                value_num_col = (
                    sql.SQL("value_num DOUBLE PRECISION NULL,")
                    if getattr(
                        config.online_store, "enable_openai_compatible_store", False
                    )
                    else sql.SQL("")
                )

                cur.execute(
                    sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {}
                        (
                            entity_key BYTEA,
                            feature_name TEXT,
                            value BYTEA,
                            value_text TEXT NULL,
                            {}
                            vector_value {} NULL,
                            event_ts TIMESTAMPTZ,
                            created_ts TIMESTAMPTZ,
                            PRIMARY KEY(entity_key, feature_name)
                        );
                        CREATE INDEX IF NOT EXISTS {} ON {} (entity_key);
                        """
                    ).format(
                        sql.Identifier(table_name),
                        value_num_col,
                        sql.SQL(vector_value_type),
                        sql.Identifier(f"{table_name}_ek"),
                        sql.Identifier(table_name),
                    )
                )

                if getattr(
                    config.online_store, "enable_openai_compatible_store", False
                ):
                    cur.execute(
                        sql.SQL(
                            """ALTER TABLE {} ADD COLUMN IF NOT EXISTS value_num DOUBLE PRECISION NULL;"""
                        ).format(sql.Identifier(table_name))
                    )
                    if self._table_has_value_num is None:
                        self._table_has_value_num = {}
                    self._table_has_value_num[table_name] = True

                if has_string_features:
                    cur.execute(
                        sql.SQL(
                            """CREATE INDEX IF NOT EXISTS {} ON {} USING GIN (to_tsvector('english', value_text));"""
                        ).format(
                            sql.Identifier(f"{table_name}_fts_idx"),
                            sql.Identifier(table_name),
                        )
                    )

            conn.commit()

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        project = config.project
        schema_name = config.online_store.db_schema or config.online_store.user
        versioning = config.registry.enable_online_feature_view_versioning
        try:
            with self._get_conn(config) as conn, conn.cursor() as cur:
                for table in tables:
                    if versioning:
                        _drop_all_version_tables(cur, project, table, schema_name)
                    else:
                        table_name = _table_id(project, table)
                        cur.execute(_drop_table_and_index(table_name))
                conn.commit()
        except Exception:
            logging.exception("Teardown failed")
            raise

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: Optional[List[str]],
        embedding: List[float],
        top_k: int,
        distance_metric: Optional[str] = "L2",
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[ValueProto],
            Optional[ValueProto],
            Optional[ValueProto],
        ]
    ]:
        """

        Args:
            config: Feast configuration object
            table: FeatureView object as the table to search
            requested_features: The list of features whose embeddings should be used for retrieval.
            embedding: The query embedding to search for
            top_k: The number of items to return
            distance_metric: The distance metric to use for the search.G
        Returns:
            List of tuples containing the event timestamp and the document feature

        """
        project = config.project

        if not config.online_store.vector_enabled:
            raise ValueError(
                "pgvector is not enabled in the online store configuration"
            )

        if distance_metric not in SUPPORTED_DISTANCE_METRICS_DICT:
            raise ValueError(
                f"Distance metric {distance_metric} is not supported. Supported distance metrics are {SUPPORTED_DISTANCE_METRICS_DICT.keys()}"
            )

        if requested_features:
            required_feature_names = ", ".join(
                [feature for feature in requested_features]
            )

        distance_metric_sql = SUPPORTED_DISTANCE_METRICS_DICT[distance_metric]

        result: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[ValueProto],
                Optional[ValueProto],
                Optional[ValueProto],
            ]
        ] = []
        with self._get_conn(config, autocommit=True) as conn, conn.cursor() as cur:
            table_name = _table_id(
                project, table, config.registry.enable_online_feature_view_versioning
            )

            # Search query template to find the top k items that are closest to the given embedding
            # SELECT * FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 5;
            cur.execute(
                sql.SQL(
                    """
                    SELECT
                        entity_key,
                        {feature_names},
                        value,
                        vector_value,
                        vector_value {distance_metric_sql} %s::vector as distance,
                        event_ts FROM {table_name}
                    ORDER BY distance
                    LIMIT {top_k};
                    """
                ).format(
                    distance_metric_sql=sql.SQL(distance_metric_sql),
                    table_name=sql.Identifier(table_name),
                    feature_names=required_feature_names,
                    top_k=sql.Literal(top_k),
                ),
                (embedding,),
            )
            rows = cur.fetchall()
            for (
                entity_key,
                _,
                feature_val,
                vector_value,
                distance_val,
                event_ts,
            ) in rows:
                result.append(
                    _build_retrieve_online_document_record(
                        entity_key,
                        feature_val,
                        vector_value,
                        distance_val,
                        event_ts,
                        config.entity_key_serialization_version,
                    )
                )

        return result

    def _translate_filters(
        self,
        filters: Optional[Union[ComparisonFilter, CompoundFilter]],
        table_name: str,
        alias: Optional[str] = None,
    ) -> Tuple[sql.Composable, List[Any]]:
        """Translate filter objects into a SQL WHERE clause fragment and params.

        Returns a ``(clause, params)`` pair. When *filters* is ``None`` the
        clause is empty and params is ``[]``, so callers can always append it
        unconditionally.

        When *alias* is given (e.g. ``"t1"``), the generated clause references
        ``t1.entity_key`` instead of the bare ``entity_key`` column, which is
        necessary when the outer query joins multiple relations.
        """
        if filters is None:
            return sql.SQL(""), []
        return self._translate_single_filter(filters, table_name, alias)

    def _translate_single_filter(
        self,
        filter_obj: Union[ComparisonFilter, CompoundFilter],
        table_name: str,
        alias: Optional[str] = None,
    ) -> Tuple[sql.Composable, List[Any]]:
        if isinstance(filter_obj, ComparisonFilter):
            return self._translate_comparison_filter(filter_obj, table_name, alias)
        elif isinstance(filter_obj, CompoundFilter):
            return self._translate_compound_filter(filter_obj, table_name, alias)
        raise ValueError(f"Unknown filter type: {type(filter_obj)}")

    @staticmethod
    def _filter_col_and_val(
        value: Any,
    ) -> Tuple[str, Any]:
        """Return the appropriate column name and DB-ready value for a filter value."""
        if isinstance(value, bool):
            return "value_num", 1.0 if value else 0.0
        if isinstance(value, (int, float)):
            return "value_num", float(value)
        return "value_text", str(value)

    def _translate_comparison_filter(
        self,
        filter_obj: ComparisonFilter,
        table_name: str,
        alias: Optional[str] = None,
    ) -> Tuple[sql.Composable, List[Any]]:
        key, value, op_type = filter_obj.key, filter_obj.value, filter_obj.type
        ek_col = f"{alias}.entity_key" if alias else "entity_key"

        if op_type in _PG_COMPARISON_OPS:
            col, db_value = self._filter_col_and_val(value)
            clause = sql.SQL(
                "{ek_col} IN (SELECT entity_key FROM {tbl} WHERE feature_name = %s AND {col} {op} %s)"
            ).format(
                ek_col=sql.SQL(ek_col),
                tbl=sql.Identifier(table_name),
                col=sql.Identifier(col),
                op=sql.SQL(_PG_COMPARISON_OPS[op_type]),
            )
            return clause, [key, db_value]

        if op_type == "in":
            if not isinstance(value, list):
                raise ValueError(
                    f"'in' filter requires a list value, got {type(value)}"
                )
            placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(value))
            col, _ = (
                self._filter_col_and_val(value[0]) if value else ("value_text", None)
            )
            db_values = [self._filter_col_and_val(v)[1] for v in value]
            clause = sql.SQL(
                "{ek_col} IN (SELECT entity_key FROM {tbl} WHERE feature_name = %s AND {col} IN ({phs}))"
            ).format(
                ek_col=sql.SQL(ek_col),
                tbl=sql.Identifier(table_name),
                col=sql.Identifier(col),
                phs=placeholders,
            )
            return clause, [key] + db_values

        if op_type == "nin":
            if not isinstance(value, list):
                raise ValueError(
                    f"'nin' filter requires a list value, got {type(value)}"
                )
            placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(value))
            col, _ = (
                self._filter_col_and_val(value[0]) if value else ("value_text", None)
            )
            db_values = [self._filter_col_and_val(v)[1] for v in value]
            clause = sql.SQL(
                "{ek_col} IN (SELECT entity_key FROM {tbl} WHERE feature_name = %s AND {col} NOT IN ({phs}))"
            ).format(
                ek_col=sql.SQL(ek_col),
                tbl=sql.Identifier(table_name),
                col=sql.Identifier(col),
                phs=placeholders,
            )
            return clause, [key] + db_values

        raise ValueError(f"Unknown comparison operator: {op_type}")

    def _translate_compound_filter(
        self,
        filter_obj: CompoundFilter,
        table_name: str,
        alias: Optional[str] = None,
    ) -> Tuple[sql.Composable, List[Any]]:
        if not filter_obj.filters:
            return sql.SQL(""), []
        parts: List[sql.Composable] = []
        all_params: List[Any] = []
        for sub in filter_obj.filters:
            sub_clause, sub_params = self._translate_single_filter(
                sub, table_name, alias
            )
            parts.append(sub_clause)
            all_params.extend(sub_params)
        joiner = sql.SQL(" AND " if filter_obj.type == "and" else " OR ")
        combined = sql.SQL("(") + joiner.join(parts) + sql.SQL(")")
        return combined, all_params

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
        Retrieve documents using vector similarity search or keyword search in PostgreSQL.

        Args:
            config: Feast configuration object
            table: FeatureView object as the table to search
            requested_features: List of requested features to retrieve
            embedding: Query embedding to search for (optional)
            top_k: Number of items to return
            distance_metric: Distance metric to use (optional)
            query_string: The query string to search for using keyword search (optional)

        Returns:
            List of tuples containing the event timestamp, entity key, and feature values
        """
        if not config.online_store.vector_enabled:
            raise ValueError("Vector search is not enabled in the online store config")

        if embedding is None and query_string is None:
            raise ValueError("Either embedding or query_string must be provided")

        if filters is not None:
            if not getattr(
                config.online_store, "enable_openai_compatible_store", False
            ):
                raise ValueError(
                    "Metadata filtering requires `enable_openai_compatible_store: true` "
                    "in your online store config. After setting it, run `feast apply` "
                    "to update the database schema."
                )

        distance_metric = distance_metric or "L2"

        if distance_metric not in SUPPORTED_DISTANCE_METRICS_DICT:
            raise ValueError(
                f"Distance metric {distance_metric} is not supported. Supported distance metrics are {SUPPORTED_DISTANCE_METRICS_DICT.keys()}"
            )

        distance_metric_sql = SUPPORTED_DISTANCE_METRICS_DICT[distance_metric]

        string_fields = [
            feature.name
            for feature in table.features
            if feature.dtype.to_value_type().value == 2
            and feature.name in requested_features
        ]

        table_name = _table_id(
            config.project,
            table,
            config.registry.enable_online_feature_view_versioning,
        )

        with self._get_conn(config, autocommit=True) as conn, conn.cursor() as cur:
            if filters is not None and self._filters_need_value_num(filters):
                if not self._check_table_has_value_num(cur, table_name, config):
                    raise ValueError(
                        "Numerical filtering requires the `value_num` column. "
                        "Run `feast apply` to add it."
                    )

            query = None
            params: Any = None

            filter_clause, filter_params = self._translate_filters(
                filters, table_name, alias="t1"
            )
            has_filters = bool(filter_params) or (
                filters is not None
                and not filter_params
                and filter_clause.as_string(conn) != ""
            )

            if embedding is not None and query_string is not None and string_fields:
                # Case 1: Hybrid Search (vector + text)
                tsquery_str = " & ".join(query_string.split())

                outer_where_parts: list[sql.Composable] = [
                    sql.SQL("t1.feature_name = ANY(%s)")
                ]
                if has_filters:
                    outer_where_parts.append(filter_clause)
                outer_where = sql.SQL(" AND ").join(outer_where_parts)

                query = sql.SQL(
                    """
                    WITH vector_candidates AS (
                        SELECT entity_key,
                            MIN(vector_value {distance_metric_sql} %s::vector) as distance
                        FROM {table_name}
                        WHERE vector_value IS NOT NULL
                        GROUP BY entity_key
                        ORDER BY distance
                        LIMIT {top_k}
                    ),
                    text_candidates AS (
                        SELECT entity_key,
                            MAX(ts_rank(to_tsvector('english', value_text), to_tsquery('english', %s))) as text_rank
                        FROM {table_name}
                        WHERE feature_name = ANY(%s)
                            AND to_tsvector('english', value_text) @@ to_tsquery('english', %s)
                        GROUP BY entity_key
                        ORDER BY text_rank DESC
                        LIMIT {top_k}
                    ),
                    all_candidates AS (
                        SELECT entity_key FROM vector_candidates
                        UNION
                        SELECT entity_key FROM text_candidates
                    ),
                    scored AS (
                        SELECT
                            ac.entity_key,
                            COALESCE(vc.distance,
                                (SELECT MIN(t.vector_value {distance_metric_sql} %s::vector)
                                 FROM {table_name} t
                                 WHERE t.entity_key = ac.entity_key AND t.vector_value IS NOT NULL)
                            ) as distance,
                            COALESCE(tc.text_rank,
                                COALESCE(
                                    (SELECT MAX(ts_rank(to_tsvector('english', ft.value_text), to_tsquery('english', %s)))
                                     FROM {table_name} ft
                                     WHERE ft.entity_key = ac.entity_key AND ft.feature_name = ANY(%s) AND ft.value_text IS NOT NULL),
                                    0
                                )
                            ) as text_rank
                        FROM all_candidates ac
                        LEFT JOIN vector_candidates vc ON ac.entity_key = vc.entity_key
                        LEFT JOIN text_candidates tc ON ac.entity_key = tc.entity_key
                        ORDER BY text_rank DESC, distance
                        LIMIT {top_k}
                    )
                    SELECT
                        t1.entity_key,
                        t1.feature_name,
                        t1.value,
                        t1.vector_value,
                        s.distance,
                        s.text_rank,
                        t1.event_ts,
                        t1.created_ts
                    FROM {table_name} t1
                    INNER JOIN scored s ON t1.entity_key = s.entity_key
                    WHERE {outer_where}
                    ORDER BY s.text_rank DESC, s.distance
                    """
                ).format(
                    distance_metric_sql=sql.SQL(distance_metric_sql),
                    table_name=sql.Identifier(table_name),
                    outer_where=outer_where,
                    top_k=sql.Literal(top_k),
                )
                base_params: list[Any] = [
                    embedding,
                    tsquery_str,
                    string_fields,
                    tsquery_str,
                    embedding,
                    tsquery_str,
                    string_fields,
                    requested_features,
                ]
                if has_filters:
                    base_params.extend(filter_params)
                params = tuple(base_params)

            elif embedding is not None:
                # Case 2: Vector Search Only
                outer_where_parts = [sql.SQL("t1.feature_name = ANY(%s)")]
                if has_filters:
                    outer_where_parts.append(filter_clause)
                outer_where = sql.SQL(" AND ").join(outer_where_parts)

                query = sql.SQL(
                    """
                    WITH vector_matches AS (
                        SELECT entity_key,
                            MIN(vector_value {distance_metric_sql} %s::vector) as distance
                        FROM {table_name}
                        WHERE vector_value IS NOT NULL
                        GROUP BY entity_key
                        ORDER BY distance
                        LIMIT {top_k}
                    )
                    SELECT
                        t1.entity_key,
                        t1.feature_name,
                        t1.value,
                        t1.vector_value,
                        t2.distance,
                        NULL as text_rank,
                        t1.event_ts,
                        t1.created_ts
                    FROM {table_name} t1
                    INNER JOIN vector_matches t2 ON t1.entity_key = t2.entity_key
                    WHERE {outer_where}
                    ORDER BY t2.distance
                    """
                ).format(
                    distance_metric_sql=sql.SQL(distance_metric_sql),
                    table_name=sql.Identifier(table_name),
                    outer_where=outer_where,
                    top_k=sql.Literal(top_k),
                )
                base_params = [embedding, requested_features]
                if has_filters:
                    base_params.extend(filter_params)
                params = tuple(base_params)

            elif query_string is not None and string_fields:
                # Case 3: Text Search Only
                tsquery_str = " & ".join(query_string.split())

                outer_where_parts = [sql.SQL("t1.feature_name = ANY(%s)")]
                if has_filters:
                    outer_where_parts.append(filter_clause)
                outer_where = sql.SQL(" AND ").join(outer_where_parts)

                query = sql.SQL(
                    """
                    WITH text_matches AS (
                        SELECT DISTINCT entity_key, ts_rank(to_tsvector('english', value_text), to_tsquery('english', %s)) as text_rank
                        FROM {table_name}
                        WHERE feature_name = ANY(%s)
                            AND to_tsvector('english', value_text) @@ to_tsquery('english', %s)
                        ORDER BY text_rank DESC
                        LIMIT {top_k}
                    )
                    SELECT
                        t1.entity_key,
                        t1.feature_name,
                        t1.value,
                        t1.vector_value,
                        NULL as distance,
                        t2.text_rank,
                        t1.event_ts,
                        t1.created_ts
                    FROM {table_name} t1
                    INNER JOIN text_matches t2 ON t1.entity_key = t2.entity_key
                    WHERE {outer_where}
                    ORDER BY t2.text_rank DESC
                    """
                ).format(
                    table_name=sql.Identifier(table_name),
                    outer_where=outer_where,
                    top_k=sql.Literal(top_k),
                )
                base_params = [
                    tsquery_str,
                    string_fields,
                    tsquery_str,
                    requested_features,
                ]
                if has_filters:
                    base_params.extend(filter_params)
                params = tuple(base_params)

            else:
                raise ValueError(
                    "Either vector_enabled must be True for embedding search or string fields must be available for query_string search"
                )

            cur.execute(query, params)
            rows = cur.fetchall()

            # Group by entity_key to build feature records
            entities_dict: Dict[str, Dict[str, Any]] = defaultdict(
                lambda: {
                    "features": {},
                    "timestamp": None,
                    "entity_key_proto": None,
                    "vector_distance": float("inf"),
                    "text_rank": 0.0,
                }
            )

            for (
                entity_key_bytes,
                feature_name,
                feature_val_bytes,
                vector_val,
                distance,
                text_rank,
                event_ts,
                created_ts,
            ) in rows:
                entity_key_proto = None
                if entity_key_bytes:
                    from feast.infra.key_encoding_utils import deserialize_entity_key

                    entity_key_proto = deserialize_entity_key(entity_key_bytes)

                key = entity_key_bytes.hex() if entity_key_bytes else None

                if key is None:
                    continue

                entities_dict[key]["entity_key_proto"] = entity_key_proto

                if (
                    entities_dict[key]["timestamp"] is None
                    or event_ts > entities_dict[key]["timestamp"]
                ):
                    entities_dict[key]["timestamp"] = event_ts

                val = ValueProto()
                if feature_val_bytes:
                    val.ParseFromString(feature_val_bytes)

                entities_dict[key]["features"][feature_name] = val

                if distance is not None:
                    entities_dict[key]["vector_distance"] = min(
                        entities_dict[key]["vector_distance"], float(distance)
                    )
                if text_rank is not None:
                    entities_dict[key]["text_rank"] = max(
                        entities_dict[key]["text_rank"], float(text_rank)
                    )

            sorted_entities = sorted(
                entities_dict.values(),
                key=lambda x: (
                    (-x["text_rank"], x["vector_distance"])
                    if query_string is not None
                    else (x["vector_distance"],)
                ),
            )[:top_k]

            result: List[
                Tuple[
                    Optional[datetime],
                    Optional[EntityKeyProto],
                    Optional[Dict[str, ValueProto]],
                ]
            ] = []
            for entity_data in sorted_entities:
                features = (
                    entity_data["features"].copy()
                    if isinstance(entity_data["features"], dict)
                    else None
                )

                if features is not None:
                    if "vector_distance" in entity_data and entity_data[
                        "vector_distance"
                    ] != float("inf"):
                        dist_val = ValueProto()
                        dist_val.double_val = entity_data["vector_distance"]
                        features["distance"] = dist_val

                    if embedding is None or query_string is not None:
                        rank_val = ValueProto()
                        rank_val.double_val = entity_data["text_rank"]
                        features["text_rank"] = rank_val

                result.append(
                    (
                        entity_data["timestamp"],
                        entity_data["entity_key_proto"],
                        features,
                    )
                )
        return result


def _table_id(project: str, table: FeatureView, enable_versioning: bool = False) -> str:
    return compute_table_id(project, table, enable_versioning)


def _drop_table_and_index(table_name):
    return sql.SQL(
        """
        DROP TABLE IF EXISTS {};
        DROP INDEX IF EXISTS {};
        """
    ).format(
        sql.Identifier(table_name),
        sql.Identifier(f"{table_name}_ek"),
    )


def _drop_all_version_tables(
    cur, project: str, table: FeatureView, schema_name: Optional[str] = None
) -> None:
    """Drop the base table and all versioned tables (e.g. _v1, _v2, ...)."""
    base = f"{project}_{table.name}"
    if schema_name:
        cur.execute(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname = %s AND (tablename = %s OR tablename ~ %s)",
            (schema_name, base, f"^{base}_v[0-9]+$"),
        )
    else:
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE tablename = %s OR tablename ~ %s",
            (base, f"^{base}_v[0-9]+$"),
        )
    for (name,) in cur.fetchall():
        cur.execute(_drop_table_and_index(name))
