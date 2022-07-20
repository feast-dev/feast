import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import psycopg2
import pytz
from psycopg2 import sql
from psycopg2.extras import execute_values
from pydantic.schema import Literal

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.utils.postgres.connection_utils import _get_conn
from feast.infra.utils.postgres.postgres_config import PostgreSQLConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.usage import log_exceptions_and_usage


class PostgreSQLOnlineStoreConfig(PostgreSQLConfig):
    type: Literal["postgres"] = "postgres"


class PostgreSQLOnlineStore(OnlineStore):
    _conn: Optional[psycopg2._psycopg.connection] = None

    def _get_conn(self, config: RepoConfig):
        if not self._conn:
            assert config.online_store.type == "postgres"
            self._conn = _get_conn(config.online_store)
        return self._conn

    @log_exceptions_and_usage(online_store="postgres")
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        project = config.project

        with self._get_conn(config) as conn, conn.cursor() as cur:
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
                    insert_values.append(
                        (
                            entity_key_bin,
                            feature_name,
                            val.SerializeToString(),
                            timestamp,
                            created_ts,
                        )
                    )
            # Control the batch so that we can update the progress
            batch_size = 5000
            for i in range(0, len(insert_values), batch_size):
                cur_batch = insert_values[i : i + batch_size]
                execute_values(
                    cur,
                    sql.SQL(
                        """
                        INSERT INTO {}
                        (entity_key, feature_name, value, event_ts, created_ts)
                        VALUES %s
                        ON CONFLICT (entity_key, feature_name) DO
                        UPDATE SET
                            value = EXCLUDED.value,
                            event_ts = EXCLUDED.event_ts,
                            created_ts = EXCLUDED.created_ts;
                        """,
                    ).format(sql.Identifier(_table_id(project, table))),
                    cur_batch,
                    page_size=batch_size,
                )
                if progress:
                    progress(len(cur_batch))

    @log_exceptions_and_usage(online_store="postgres")
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        project = config.project
        with self._get_conn(config) as conn, conn.cursor() as cur:
            # Collecting all the keys to a list allows us to make fewer round trips
            # to PostgreSQL
            keys = []
            for entity_key in entity_keys:
                keys.append(
                    serialize_entity_key(
                        entity_key,
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    )
                )

            cur.execute(
                sql.SQL(
                    """
                    SELECT entity_key, feature_name, value, event_ts
                    FROM {} WHERE entity_key = ANY(%s);
                    """
                ).format(
                    sql.Identifier(_table_id(project, table)),
                ),
                (keys,),
            )

            rows = cur.fetchall()

            # Since we don't know the order returned from PostgreSQL we'll need
            # to construct a dict to be able to quickly look up the correct row
            # when we iterate through the keys since they are in the correct order
            values_dict = defaultdict(list)
            for row in rows if rows is not None else []:
                values_dict[row[0].tobytes()].append(row[1:])

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

    @log_exceptions_and_usage(online_store="postgres")
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
                cur.execute(
                    sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {}
                        (
                            entity_key BYTEA,
                            feature_name TEXT,
                            value BYTEA,
                            event_ts TIMESTAMPTZ,
                            created_ts TIMESTAMPTZ,
                            PRIMARY KEY(entity_key, feature_name)
                        );
                        CREATE INDEX IF NOT EXISTS {} ON {} (entity_key);
                        """
                    ).format(
                        sql.Identifier(table_name),
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


def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)
