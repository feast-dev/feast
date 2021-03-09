import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from feast import FeatureTable
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.provider import Provider
from feast.repo_config import LocalOnlineStoreConfig
from feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.types.Value_pb2 import Value as ValueProto


def _table_id(project: str, table: FeatureTable) -> str:
    return f"{project}_{table.name}"


class LocalSqlite(Provider):
    _db_path: str

    def __init__(self, config: LocalOnlineStoreConfig):
        self._db_path = config.path

    def _get_conn(self):
        return sqlite3.connect(
            self._db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )

    def update_infra(
        self,
        project: str,
        tables_to_delete: List[FeatureTable],
        tables_to_keep: List[FeatureTable],
    ):
        conn = self._get_conn()
        for table in tables_to_keep:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {_table_id(project, table)} (entity_key BLOB, feature_name TEXT, value BLOB, event_ts timestamp, created_ts timestamp,  PRIMARY KEY(entity_key, feature_name))"
            )
            conn.execute(
                f"CREATE INDEX {_table_id(project, table)}_ek ON {_table_id(project, table)} (entity_key);"
            )

        for table in tables_to_delete:
            conn.execute(f"DROP TABLE IF EXISTS {_table_id(project, table)}")

    def teardown_infra(self, project: str, tables: List[FeatureTable]) -> None:
        os.unlink(self._db_path)

    def online_write_batch(
        self,
        project: str,
        table: FeatureTable,
        data: List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime]],
        created_ts: datetime,
    ) -> None:
        conn = self._get_conn()
        with conn:
            for entity_key, values, timestamp in data:
                for feature_name, val in values.items():
                    entity_key_bin = serialize_entity_key(entity_key)
                    conn.execute(
                        f"""INSERT INTO {_table_id(project, table)}
                            (entity_key, feature_name, value, event_ts, created_ts)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT (entity_key, feature_name) DO UPDATE
                            SET value = ?, event_ts = ?, created_ts = ?
                            WHERE event_ts < ? OR (event_ts = ? AND created_ts < ?)
                        """,
                        (
                            # INSERT
                            entity_key_bin,
                            feature_name,
                            val.SerializeToString(),
                            timestamp,
                            created_ts,
                            # SET
                            val.SerializeToString(),
                            timestamp,
                            created_ts,
                            # WHERE
                            timestamp,
                            timestamp,
                            created_ts,
                        ),
                    )

    def online_read(
        self, project: str, table: FeatureTable, entity_key: EntityKeyProto
    ) -> Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]:
        entity_key_bin = serialize_entity_key(entity_key)

        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            f"SELECT feature_name, value, event_ts FROM {_table_id(project, table)} WHERE entity_key = ?",
            (entity_key_bin,),
        )

        res = {}
        res_ts = None
        for feature_name, val_bin, ts in cur.fetchall():
            val = ValueProto()
            val.ParseFromString(val_bin)
            res[feature_name] = val
            res_ts = ts

        if not res:
            return None, None
        else:
            return res_ts, res
