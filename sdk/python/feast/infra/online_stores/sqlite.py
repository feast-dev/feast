# Copyright 2021 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pytz

from feast import FeatureTable
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig


class SqliteOnlineStore(OnlineStore):
    """
    OnlineStore is an object used for all interaction between Feast and the service used for offline storage of
    features.
    """

    @staticmethod
    def _get_conn(config: RepoConfig):
        assert config.online_store.type == "sqlite"

        if config.repo_path:
            db_path = config.repo_path / config.online_store.path
        else:
            db_path = config.online_store.path

        Path(db_path).parent.mkdir(exist_ok=True)
        return sqlite3.connect(
            db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )

    @classmethod
    def online_write_batch(
        cls,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        conn = cls._get_conn(config)

        project = config.project

        with conn:
            for entity_key, values, timestamp, created_ts in data:
                entity_key_bin = serialize_entity_key(entity_key)
                timestamp = _to_naive_utc(timestamp)
                if created_ts is not None:
                    created_ts = _to_naive_utc(created_ts)

                for feature_name, val in values.items():
                    conn.execute(
                        f"""
                            UPDATE {_table_id(project, table)}
                            SET value = ?, event_ts = ?, created_ts = ?
                            WHERE (entity_key = ? AND feature_name = ?)
                        """,
                        (
                            # SET
                            val.SerializeToString(),
                            timestamp,
                            created_ts,
                            # WHERE
                            entity_key_bin,
                            feature_name,
                        ),
                    )

                    conn.execute(
                        f"""INSERT OR IGNORE INTO {_table_id(project, table)}
                            (entity_key, feature_name, value, event_ts, created_ts)
                            VALUES (?, ?, ?, ?, ?)""",
                        (
                            entity_key_bin,
                            feature_name,
                            val.SerializeToString(),
                            timestamp,
                            created_ts,
                        ),
                    )
                if progress:
                    progress(1)

    @classmethod
    def online_read(
        cls,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        pass
        conn = cls._get_conn(config)
        cur = conn.cursor()

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        project = config.project
        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(entity_key)

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
                result.append((None, None))
            else:
                result.append((res_ts, res))
        return result


def _table_id(project: str, table: Union[FeatureTable, FeatureView]) -> str:
    return f"{project}_{table.name}"


def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)
