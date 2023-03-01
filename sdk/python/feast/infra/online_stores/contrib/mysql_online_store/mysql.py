from __future__ import absolute_import

from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pymysql
import pytz
from pydantic import StrictStr
from enum import Enum
from pymysql.connections import Connection
from pymysql.cursors import Cursor

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel


# RB: Power of two for better (theoretical) page alignment. Don't make this too large because we need to fulfill
#     <5 second latency requirements for Aurora / RDS.
INSERT_BATCH_SIZE = 1 << 8


class ReleaseMode(Enum):
    overwrite = 'overwrite'
    update = 'update'


class MySQLOnlineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the MySQL online store.
    NOTE: The class *must* end with the `OnlineStoreConfig` suffix.
    """

    type = "mysql"

    host: Optional[StrictStr] = None
    user: Optional[StrictStr] = None
    password: Optional[StrictStr] = None
    database: Optional[StrictStr] = None
    port: Optional[int] = None


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
                autocommit=True,
            )
        return self._conn

    def unpack_write_entity(self, entity: EntityKeyProto,
                                  values: Dict[str, ValueProto],
                                  ts: datetime,
                                  created_ts: Optional[datetime]):
        entity_key_bin = serialize_entity_key(
            entity,
            entity_key_serialization_version=2,
        ).hex()
        ts = _to_naive_utc(ts)
        if created_ts is not None:
            created_ts = _to_naive_utc(created_ts)
        return entity_key_bin, values, ts, created_ts

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
        insert_values = []
        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=2,
            ).hex()
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

                        # Update on duplicate key
                        val.SerializeToString(),
                        timestamp,
                        created_ts,
                    )
                )

        for i in range(0, len(insert_values), INSERT_BATCH_SIZE):
            insertion_batch = insert_values[i: i + INSERT_BATCH_SIZE]
            cur.executemany(
                f"""
                INSERT INTO {_table_id(project, table)}
                (entity_key, feature_name, value, event_ts, created_ts)
                values (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                value = %s,
                event_ts = %s,
                created_ts = %s;
                """,
                insertion_batch
            )
        conn.commit()

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
        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=2,
            ).hex()

            cur.execute(
                f"SELECT feature_name, value, event_ts FROM {_table_id(project, table)} WHERE entity_key = %s",
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

        # We don't create any special state for the entities in this implementation.
        for table in tables_to_keep:
            cur.execute(
                f"""CREATE TABLE IF NOT EXISTS {_table_id(project, table)} (entity_key VARCHAR(512),
                feature_name VARCHAR(256),
                value BLOB,
                event_ts timestamp NULL DEFAULT NULL,
                created_ts timestamp NULL DEFAULT NULL,
                PRIMARY KEY(entity_key, feature_name))"""
            )

            release_mode = ReleaseMode.update if 'release_mode' not in table.tags \
                else ReleaseMode(table.tags['release_mode'])

            if release_mode == ReleaseMode.overwrite:
                cur.execute(f'DELETE FROM {_table_id(project, table)};')

            cur.execute(
                f"SHOW INDEXES FROM {_table_id(project, table)};"
            )

            index_exists = False
            for index in cur.fetchall():
                if index[2] == f"{_table_id(project, table)}_ek":
                    if release_mode == ReleaseMode.overwrite:
                        cur.execute(f"DROP INDEX {_table_id(project, table)}_ek ON {_table_id(project, table)};")
                    else:
                        index_exists = True
                    break

            if not index_exists:
                cur.execute(
                    f"ALTER TABLE {_table_id(project, table)} ADD INDEX {_table_id(project, table)}_ek (entity_key);"
                )

        for table in tables_to_delete:
            _drop_table_and_index(cur, project, table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        conn = self._get_conn(config)
        cur = conn.cursor()
        project = config.project

        for table in tables:
            _drop_table_and_index(cur, project, table)


def _drop_table_and_index(cur: Cursor, project: str, table: FeatureView) -> None:
    table_name = _table_id(project, table)
    cur.execute(f"DROP INDEX {table_name}_ek ON {table_name};")
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


def _to_naive_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)
