from __future__ import absolute_import

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

import singlestoredb
from pydantic import StrictStr
from singlestoredb.connection import Connection, Cursor
from singlestoredb.exceptions import InterfaceError

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.helpers import _to_naive_utc, online_store_table_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel

logger = logging.getLogger(__name__)


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

    @property
    def supports_versioned_online_reads(self) -> bool:
        return True

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
                    entity_key_serialization_version=config.entity_key_serialization_version,
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
                    INSERT INTO {_quote_identifier(online_store_table_id(project, table, config.registry.enable_online_feature_view_versioning))}
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
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    ).hex()
                )

            if not requested_features:
                entity_key_placeholders = ",".join(["%s" for _ in keys])
                cur.execute(
                    f"""
                    SELECT entity_key, feature_name, value, event_ts FROM {_quote_identifier(online_store_table_id(project, table, config.registry.enable_online_feature_view_versioning))}
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
                    SELECT entity_key, feature_name, value, event_ts FROM {_quote_identifier(online_store_table_id(project, table, config.registry.enable_online_feature_view_versioning))}
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
        versioning = config.registry.enable_online_feature_view_versioning
        with self._get_cursor(config) as cur:
            # We don't create any special state for the entities in this implementation.
            for table in tables_to_keep:
                table_name = online_store_table_id(project, table, versioning)
                cur.execute(
                    f"""CREATE TABLE IF NOT EXISTS {_quote_identifier(table_name)} (entity_key VARCHAR(512),
                    feature_name VARCHAR(256),
                    value BLOB,
                    event_ts timestamp NULL DEFAULT NULL,
                    created_ts timestamp NULL DEFAULT NULL,
                    PRIMARY KEY(entity_key, feature_name),
                    INDEX {_quote_identifier(table_name + "_ek")} (entity_key))"""
                )

            for table in tables_to_delete:
                _drop_table_and_index(cur, project, table, versioning)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
        registry: Optional[BaseRegistry] = None,
    ) -> None:
        project = config.project
        versioning = config.registry.enable_online_feature_view_versioning
        with self._get_cursor(config) as cur:
            for table in tables:
                if not versioning:
                    _drop_table_and_index(cur, project, table, enable_versioning=False)
                    continue

                versions = []
                if registry is not None:
                    try:
                        versions = registry.list_feature_view_versions(
                            name=table.name, project=project
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to list feature view versions for %s during teardown; will fall back to dropping discovered versioned tables. Error: %s",
                            table.name,
                            e,
                        )
                        versions = []

                if not versions:
                    _drop_table_and_index(cur, project, table, enable_versioning=False)
                    _drop_table_and_index(cur, project, table, enable_versioning=True)
                    _drop_discovered_versioned_tables(cur, project, table)
                    continue

                for record in versions:
                    version_number = record.get("version_number")
                    if version_number is None:
                        continue
                    _drop_table_and_index(
                        cur,
                        project,
                        table,
                        enable_versioning=True,
                        version=version_number,
                    )

                _drop_discovered_versioned_tables(cur, project, table)


def _drop_table_and_index(
    cur: Cursor,
    project: str,
    table: FeatureView,
    enable_versioning: bool,
    version: Optional[int] = None,
) -> None:
    table_name = online_store_table_id(project, table, enable_versioning, version)
    table_name_quoted = _quote_identifier(table_name)
    index_name_quoted = _quote_identifier(f"{table_name}_ek")
    cur.execute(f"DROP INDEX IF EXISTS {index_name_quoted} ON {table_name_quoted};")
    cur.execute(f"DROP TABLE IF EXISTS {table_name_quoted}")


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def _drop_discovered_versioned_tables(
    cur: Cursor, project: str, table: FeatureView
) -> None:
    base_table_name = online_store_table_id(project, table, enable_versioning=False)
    escaped_base_table_name = base_table_name.replace("\\", "\\\\")
    escaped_base_table_name = escaped_base_table_name.replace("%", "\\%")
    escaped_base_table_name = escaped_base_table_name.replace("_", "\\_")
    like_pattern = f"{escaped_base_table_name}\\_v%"
    try:
        cur.execute("SHOW TABLES LIKE %s ESCAPE '\\\\'", (like_pattern,))
        rows = cur.fetchall() or []
        for row in rows:
            table_name = row[0]
            index_name = f"{table_name}_ek"
            cur.execute(
                f"DROP INDEX IF EXISTS {_quote_identifier(index_name)} ON {_quote_identifier(table_name)};"
            )
            cur.execute(f"DROP TABLE IF EXISTS {_quote_identifier(table_name)}")
    except Exception as e:
        logger.warning(
            "Failed to discover/drop versioned tables for %s during teardown fallback. Error: %s",
            table.name,
            e,
        )
