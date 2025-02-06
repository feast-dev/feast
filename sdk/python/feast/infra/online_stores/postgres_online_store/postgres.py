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

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import get_list_val_str, serialize_entity_key
from feast.infra.online_stores.helpers import _to_naive_utc
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


class PostgreSQLOnlineStoreConfig(PostgreSQLConfig, VectorStoreConfig):
    type: Literal["postgres"] = "postgres"


class PostgreSQLOnlineStore(OnlineStore):
    _conn: Optional[Connection] = None
    _conn_pool: Optional[ConnectionPool] = None

    _conn_async: Optional[AsyncConnection] = None
    _conn_pool_async: Optional[AsyncConnectionPool] = None

    @contextlib.contextmanager
    def _get_conn(self, config: RepoConfig) -> Generator[Connection, Any, Any]:
        assert config.online_store.type == "postgres"

        if config.online_store.conn_type == ConnectionType.pool:
            if not self._conn_pool:
                self._conn_pool = _get_connection_pool(config.online_store)
                self._conn_pool.open()
            connection = self._conn_pool.getconn()
            yield connection
            self._conn_pool.putconn(connection)
        else:
            if not self._conn:
                self._conn = _get_conn(config.online_store)
            yield self._conn

    @contextlib.asynccontextmanager
    async def _get_conn_async(
        self, config: RepoConfig
    ) -> AsyncGenerator[AsyncConnection, Any]:
        if config.online_store.conn_type == ConnectionType.pool:
            if not self._conn_pool_async:
                self._conn_pool_async = await _get_connection_pool_async(
                    config.online_store
                )
                await self._conn_pool_async.open()
            connection = await self._conn_pool_async.getconn()
            yield connection
            await self._conn_pool_async.putconn(connection)
        else:
            if not self._conn_async:
                self._conn_async = await _get_conn_async(config.online_store)
            yield self._conn_async

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        # Format insert values
        insert_values = []
        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            timestamp = _to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = _to_naive_utc(created_ts)

            for feature_name, val in values.items():
                vector_val = None
                if config.online_store.vector_enabled:
                    vector_val = get_list_val_str(val)
                insert_values.append(
                    (
                        entity_key_bin,
                        feature_name,
                        val.SerializeToString(),
                        vector_val,
                        timestamp,
                        created_ts,
                    )
                )

        # Create insert query
        sql_query = sql.SQL(
            """
            INSERT INTO {}
            (entity_key, feature_name, value, vector_value, event_ts, created_ts)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (entity_key, feature_name) DO
            UPDATE SET
                value = EXCLUDED.value,
                vector_value = EXCLUDED.vector_value,
                event_ts = EXCLUDED.event_ts,
                created_ts = EXCLUDED.created_ts;
        """
        ).format(sql.Identifier(_table_id(config.project, table)))

        # Push data into the online store
        with self._get_conn(config) as conn, conn.cursor() as cur:
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

        with self._get_conn(config) as conn, conn.cursor() as cur:
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

        async with self._get_conn_async(config) as conn:
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
                sql.Identifier(_table_id(config.project, table)),
            )
            params = (keys, requested_features)
        else:
            query = sql.SQL(
                """
                SELECT entity_key, feature_name, value, event_ts
                FROM {} WHERE entity_key = ANY(%s);
                """
            ).format(
                sql.Identifier(_table_id(config.project, table)),
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

            for table in tables_to_delete:
                table_name = _table_id(project, table)
                cur.execute(_drop_table_and_index(table_name))

            for table in tables_to_keep:
                table_name = _table_id(project, table)
                if config.online_store.vector_enabled:
                    vector_value_type = f"vector({config.online_store.vector_len})"
                else:
                    # keep the vector_value_type as BYTEA if pgvector is not enabled, to maintain compatibility
                    vector_value_type = "BYTEA"
                cur.execute(
                    sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {}
                        (
                            entity_key BYTEA,
                            feature_name TEXT,
                            value BYTEA,
                            vector_value {} NULL,
                            event_ts TIMESTAMPTZ,
                            created_ts TIMESTAMPTZ,
                            PRIMARY KEY(entity_key, feature_name)
                        );
                        CREATE INDEX IF NOT EXISTS {} ON {} (entity_key);
                        """
                    ).format(
                        sql.Identifier(table_name),
                        sql.SQL(vector_value_type),
                        sql.Identifier(f"{table_name}_ek"),
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
        try:
            with self._get_conn(config) as conn, conn.cursor() as cur:
                for table in tables:
                    table_name = _table_id(project, table)
                    cur.execute(_drop_table_and_index(table_name))
        except Exception:
            logging.exception("Teardown failed")
            raise

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_feature: Optional[str],
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
            requested_feature: The requested feature as the column to search
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
        with self._get_conn(config) as conn, conn.cursor() as cur:
            table_name = _table_id(project, table)

            # Search query template to find the top k items that are closest to the given embedding
            # SELECT * FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 5;
            cur.execute(
                sql.SQL(
                    """
                    SELECT
                        entity_key,
                        feature_name,
                        value,
                        vector_value,
                        vector_value {distance_metric_sql} %s::vector as distance,
                        event_ts FROM {table_name}
                    WHERE feature_name = {feature_name}
                    ORDER BY distance
                    LIMIT {top_k};
                    """
                ).format(
                    distance_metric_sql=sql.SQL(distance_metric_sql),
                    table_name=sql.Identifier(table_name),
                    feature_name=sql.Literal(requested_feature),
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


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


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
