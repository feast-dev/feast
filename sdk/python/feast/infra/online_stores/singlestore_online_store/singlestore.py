from __future__ import absolute_import

from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

import singlestoredb
from pydantic import StrictStr
from singlestoredb.connection import Connection, Cursor
from singlestoredb.exceptions import InterfaceError

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.helpers import _to_naive_utc
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel


class SingleStoreOnlineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the SingleStore online store.
    NOTE: The class *must* end with the `OnlineStoreConfig` suffix.
    """

    type: Literal["singlestore"] = "singlestore"

    host: Optional[StrictStr] = None
    user: Optional[StrictStr] = None
    password: Optional[StrictStr] = None
    database: Optional[StrictStr] = None
    port: Optional[int] = None


class SingleStoreOnlineStore(OnlineStore):
    """
    An online store implementation that uses SingleStore.
    NOTE: The class *must* end with the `OnlineStore` suffix.
    """

    _conn: Optional[Connection] = None

    def _init_conn(self, config: RepoConfig) -> Connection:
        online_store_config = config.online_store
        assert isinstance(online_store_config, SingleStoreOnlineStoreConfig)
        return singlestoredb.connect(
            host=online_store_config.host or "127.0.0.1",
            user=online_store_config.user or "test",
            password=online_store_config.password or "test",
            database=online_store_config.database or "feast",
            port=online_store_config.port or 3306,
            conn_attrs={"_connector_name": "SingleStore Feast Online Store"},
            autocommit=True,
        )

    def _get_cursor(self, config: RepoConfig) -> Any:
        # This will try to reconnect also.
        # In case it fails, we will have to create a new connection.
        if not self._conn:
            self._conn = self._init_conn(config)
        try:
            self._conn.ping(reconnect=True)
        except InterfaceError:
            self._conn = self._init_conn(config)
        return self._conn.cursor()

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
        with self._get_cursor(config) as cur:
            insert_values = []
            for entity_key, values, timestamp, created_ts in data:
                entity_key_bin = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=3,
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
                        )
                    )
            # Control the batch so that we can update the progress
            batch_size = 50000
            for i in range(0, len(insert_values), batch_size):
                current_batch = insert_values[i : i + batch_size]
                cur.executemany(
                    f"""
                    INSERT INTO {_table_id(project, table)}
                    (entity_key, feature_name, value, event_ts, created_ts)
                    values (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    value = VALUES(value),
                    event_ts = VALUES(event_ts),
                    created_ts = VALUES(created_ts);
                    """,
                    current_batch,
                )
                if progress:
                    progress(len(current_batch))

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        project = config.project
        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        with self._get_cursor(config) as cur:
            keys = []
            for entity_key in entity_keys:
                keys.append(
                    serialize_entity_key(
                        entity_key,
                        entity_key_serialization_version=3,
                    ).hex()
                )

            if not requested_features:
                entity_key_placeholders = ",".join(["%s" for _ in keys])
                cur.execute(
                    f"""
                    SELECT entity_key, feature_name, value, event_ts FROM {_table_id(project, table)}
                    WHERE entity_key IN ({entity_key_placeholders})
                    ORDER BY event_ts;
                    """,
                    tuple(keys),
                )
            else:
                entity_key_placeholders = ",".join(["%s" for _ in keys])
                requested_features_placeholders = ",".join(
                    ["%s" for _ in requested_features]
                )
                cur.execute(
                    f"""
                    SELECT entity_key, feature_name, value, event_ts FROM {_table_id(project, table)}
                    WHERE entity_key IN ({entity_key_placeholders}) and feature_name IN ({requested_features_placeholders})
                    ORDER BY event_ts;
                    """,
                    tuple(keys + requested_features),
                )
            rows = cur.fetchall() or []

            # Since we don't know the order returned from MySQL we'll need
            # to construct a dict to be able to quickly look up the correct row
            # when we iterate through the keys since they are in the correct order
            values_dict = defaultdict(list)
            for row in rows:
                values_dict[row[0]].append(row[1:])

            for key in keys:
                if key in values_dict:
                    key_values = values_dict[key]
                    res = {}
                    res_ts: Optional[datetime] = None
                    for feature_name, value_bin, event_ts in key_values:
                        val = ValueProto()
                        val.ParseFromString(bytes(value_bin))
                        res[feature_name] = val
                        res_ts = event_ts
                    result.append((res_ts, res))
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
    ) -> None:
        project = config.project
        with self._get_cursor(config) as cur:
            # We don't create any special state for the entities in this implementation.
            for table in tables_to_keep:
                cur.execute(
                    f"""CREATE TABLE IF NOT EXISTS {_table_id(project, table)} (entity_key VARCHAR(512),
                    feature_name VARCHAR(256),
                    value BLOB,
                    event_ts timestamp NULL DEFAULT NULL,
                    created_ts timestamp NULL DEFAULT NULL,
                    PRIMARY KEY(entity_key, feature_name),
                    INDEX {_table_id(project, table)}_ek (entity_key))"""
                )

            for table in tables_to_delete:
                _drop_table_and_index(cur, project, table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        project = config.project
        with self._get_cursor(config) as cur:
            for table in tables:
                _drop_table_and_index(cur, project, table)


def _drop_table_and_index(cur: Cursor, project: str, table: FeatureView) -> None:
    table_name = _table_id(project, table)
    cur.execute(f"DROP INDEX {table_name}_ek ON {table_name};")
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"
