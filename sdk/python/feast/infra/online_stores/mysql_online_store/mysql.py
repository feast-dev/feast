from __future__ import absolute_import

from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

import pymysql
from pydantic import StrictStr
from pymysql.connections import Connection
from pymysql.cursors import Cursor

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.helpers import compute_table_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.utils import to_naive_utc


class MySQLOnlineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the MySQL online store.
    NOTE: The class *must* end with the `OnlineStoreConfig` suffix.
    """

    type: Literal["mysql"] = "mysql"

    host: Optional[StrictStr] = None
    user: Optional[StrictStr] = None
    password: Optional[StrictStr] = None
    database: Optional[StrictStr] = None
    port: Optional[int] = None
    batch_write: Optional[bool] = False
    batch_size: Optional[int] = None


class MySQLOnlineStore(OnlineStore):
    """
    An online store implementation that uses MySQL.
    NOTE: The class *must* end with the `OnlineStore` suffix.
    """

    _conn: Optional[Connection] = None

    def _get_conn(self, config: RepoConfig) -> Connection:
        online_store_config = config.online_store
        assert isinstance(online_store_config, MySQLOnlineStoreConfig)

        if not self._conn:
            self._conn = pymysql.connect(
                host=online_store_config.host or "127.0.0.1",
                user=online_store_config.user or "test",
                password=online_store_config.password or "test",
                database=online_store_config.database or "feast",
                port=online_store_config.port or 3306,
                autocommit=(not online_store_config.batch_write),
            )
        return self._conn

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        conn = self._get_conn(config)
        cur = conn.cursor()

        project = config.project
        versioning = config.registry.enable_online_feature_view_versioning

        batch_write = config.online_store.batch_write
        if not batch_write:
            for entity_key, values, timestamp, created_ts in data:
                entity_key_bin = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=3,
                ).hex()
                timestamp = to_naive_utc(timestamp)
                if created_ts is not None:
                    created_ts = to_naive_utc(created_ts)

                for feature_name, val in values.items():
                    self.write_to_table(
                        created_ts,
                        cur,
                        entity_key_bin,
                        feature_name,
                        project,
                        table,
                        timestamp,
                        val,
                        versioning,
                    )
                conn.commit()
                if progress:
                    progress(1)
        else:
            batch_size = config.online_store.bacth_size
            if not batch_size or batch_size < 2:
                raise ValueError("Batch size must be at least 2")
            insert_values = []
            for entity_key, values, timestamp, created_ts in data:
                entity_key_bin = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=2,
                ).hex()
                timestamp = to_naive_utc(timestamp)
                if created_ts is not None:
                    created_ts = to_naive_utc(created_ts)

                for feature_name, val in values.items():
                    serialized_val = val.SerializeToString()
                    insert_values.append(
                        (
                            entity_key_bin,
                            feature_name,
                            serialized_val,
                            timestamp,
                            created_ts,
                        )
                    )

                    if len(insert_values) >= batch_size:
                        try:
                            self._execute_batch(
                                cur, project, table, insert_values, versioning
                            )
                            conn.commit()
                            if progress:
                                progress(len(insert_values))
                        except Exception as e:
                            conn.rollback()
                            raise e
                        insert_values.clear()

            if insert_values:
                try:
                    self._execute_batch(cur, project, table, insert_values, versioning)
                    conn.commit()
                    if progress:
                        progress(len(insert_values))
                except Exception as e:
                    conn.rollback()
                    raise e

    def _execute_batch(
        self, cur, project, table, insert_values, enable_versioning=False
    ):
        table_name = _table_id(project, table, enable_versioning)
        stmt = f"""
            INSERT INTO {table_name}
            (entity_key, feature_name, value, event_ts, created_ts)
            values (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value),
                event_ts = VALUES(event_ts),
                created_ts = VALUES(created_ts);
        """
        try:
            cur.executemany(stmt, insert_values)
        except Exception as e:
            first_sample = insert_values[0] if insert_values else None
            raise RuntimeError(
                f"Failed to execute batch insert into table '{table_name}' "
                f"(rows={len(insert_values)}, sample={first_sample}): {e}"
            ) from e

    @staticmethod
    def write_to_table(
        created_ts,
        cur,
        entity_key_bin,
        feature_name,
        project,
        table,
        timestamp,
        val,
        enable_versioning=False,
    ) -> None:
        cur.execute(
            f"""
            INSERT INTO {_table_id(project, table, enable_versioning)}
            (entity_key, feature_name, value, event_ts, created_ts)
            values (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            value = %s,
            event_ts = %s,
            created_ts = %s;
            """,
            (
                # Insert
                entity_key_bin,
                feature_name,
                val.SerializeToString(),
                timestamp,
                created_ts,
                # Update on duplicate key
                val.SerializeToString(),
                timestamp,
                created_ts,
            ),
        )

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        conn = self._get_conn(config)
        cur = conn.cursor()

        result: List[Tuple[Optional[datetime], Optional[Dict[str, Any]]]] = []

        project = config.project
        versioning = config.registry.enable_online_feature_view_versioning
        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=3,
            ).hex()

            cur.execute(
                f"SELECT feature_name, value, event_ts FROM {_table_id(project, table, versioning)} WHERE entity_key = %s",
                (entity_key_bin,),
            )

            res = {}
            res_ts: Optional[datetime] = None
            records = cur.fetchall()
            if records:
                for feature_name, val_bin, ts in records:
                    val = ValueProto()
                    val.ParseFromString(val_bin)
                    res[feature_name] = val
                    res_ts = ts

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
    ) -> None:
        conn = self._get_conn(config)
        cur = conn.cursor()
        project = config.project
        versioning = config.registry.enable_online_feature_view_versioning

        # We don't create any special state for the entities in this implementation.
        for table in tables_to_keep:
            table_name = _table_id(project, table, versioning)
            index_name = f"{table_name}_ek"
            cur.execute(
                f"""CREATE TABLE IF NOT EXISTS {table_name} (entity_key VARCHAR(512),
                feature_name VARCHAR(256),
                value BLOB,
                event_ts timestamp NULL DEFAULT NULL,
                created_ts timestamp NULL DEFAULT NULL,
                PRIMARY KEY(entity_key, feature_name))"""
            )

            index_exists = cur.execute(
                f"""
                SELECT 1 FROM information_schema.statistics
                WHERE table_schema = DATABASE() AND table_name = '{table_name}' AND index_name = '{index_name}'
                """
            )
            if not index_exists:
                cur.execute(
                    f"ALTER TABLE {table_name} ADD INDEX {index_name} (entity_key);"
                )

        for table in tables_to_delete:
            if versioning:
                _drop_all_version_tables(cur, project, table)
            else:
                _drop_table_and_index(cur, _table_id(project, table))

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        conn = self._get_conn(config)
        cur = conn.cursor()
        project = config.project
        versioning = config.registry.enable_online_feature_view_versioning

        for table in tables:
            if versioning:
                _drop_all_version_tables(cur, project, table)
            else:
                _drop_table_and_index(cur, _table_id(project, table))


def _drop_table_and_index(cur: Cursor, table_name: str) -> None:
    cur.execute(f"DROP INDEX {table_name}_ek ON {table_name};")
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def _drop_all_version_tables(cur: Cursor, project: str, table: FeatureView) -> None:
    """Drop the base table and all versioned tables (e.g. _v1, _v2, ...)."""
    base = f"{project}_{table.name}"
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND (table_name = %s OR table_name LIKE %s)",
        (base, f"{base}_v%"),
    )
    for (name,) in cur.fetchall():
        cur.execute(f"DROP INDEX IF EXISTS {name}_ek ON {name};")
        cur.execute(f"DROP TABLE IF EXISTS {name}")


def _table_id(project: str, table: FeatureView, enable_versioning: bool = False) -> str:
    return compute_table_id(project, table, enable_versioning)
