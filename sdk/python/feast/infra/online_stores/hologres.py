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

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import psycopg2
import pytz
from pydantic import PositiveInt, StrictStr
from pydantic.schema import Literal

from feast import Entity, FeatureTable
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig


class HologresOnlineStoreConfig(FeastConfigBaseModel):
    """ Online store config for local (SQLite-based) store """

    type: Literal[
        "hologres", "feast.infra.online_stores.hologres.HologresOnlineStore"
    ] = "hologres"
    """ Online store type selector"""

    host: StrictStr = ""
    """ host to hologres db """

    port: PositiveInt = 1
    """ port to hologres db"""

    dbname: StrictStr = ""
    """ db name to hologres db"""

    user: StrictStr = ""
    """ user name to hologres db"""

    password: StrictStr = ""
    """ passwrod to hologres db"""


class HologresOnlineStore(OnlineStore):
    """
    OnlineStore is an object used for all interaction between Feast and the service used for offline storage of
    features.
    """

    _conn: Optional[psycopg2.extensions.connection] = None

    @staticmethod
    def _get_db_connect_param(config: RepoConfig) -> Tuple[Any, Any, Any, Any, Any]:
        assert (
            config.online_store.type == "hologres"
            or config.online_store.type.endswith("HologresOnlineStore")
        )

        host = config.online_store.host
        port = config.online_store.port
        dbname = config.online_store.dbname
        user = config.online_store.user
        password = config.online_store.password
        return host, port, dbname, user, password

    def _get_conn(self, config: RepoConfig):
        if not self._conn:
            host, port, dbname, user, password = self._get_db_connect_param(config)
            self._conn = psycopg2.connect(
                host=host, port=port, dbname=dbname, user=user, password=password
            )
        return self._conn

    def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:

        conn = self._get_conn(config)

        project = config.project

        with conn:
            cur = conn.cursor()
            for entity_key, values, timestamp, created_ts in data:
                entity_key_bin = serialize_entity_key(entity_key)
                timestamp = _to_naive_utc(timestamp)
                if created_ts is not None:
                    created_ts = _to_naive_utc(created_ts)

                for feature_name, val in values.items():
                    cur.execute(
                        f"""
                            UPDATE {_table_id(project, table)}
                            SET value = %s, event_ts = %s, created_ts = %s
                            WHERE (entity_key = %s AND feature_name = %s)
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

                    cur.execute(
                        f"""INSERT INTO {_table_id(project, table)}
                            (entity_key, feature_name, value, event_ts, created_ts)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT(entity_key, feature_name) DO UPDATE SET 
                            (entity_key, feature_name, value, event_ts, created_ts) = ROW(excluded.*);
                            """,
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

    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        conn = self._get_conn(config)
        cur = conn.cursor()

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        project = config.project
        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(entity_key)

            cur.execute(
                f"SELECT feature_name, value, event_ts FROM {_table_id(project, table)} WHERE entity_key = %s",
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

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        conn = self._get_conn(config)
        cur = conn.cursor()
        project = config.project

        # create feast schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS feast")
        for table in tables_to_keep:
            sql = f"CREATE TABLE IF NOT EXISTS {_table_id(project, table)} (entity_key BYTEA, feature_name TEXT, value BYTEA, event_ts timestamp, created_ts timestamp,  PRIMARY KEY(entity_key, feature_name))"
            cur.execute(sql)
            conn.commit()

        for table in tables_to_delete:
            cur.execute(f"DROP TABLE IF EXISTS {_table_id(project, table)}")
            conn.commit()

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ):
        conn = self._get_conn(config)
        conn.close()


def _table_id(project: str, table: Union[FeatureTable, FeatureView]) -> str:
    return f"feast.{project}_{table.name}"


def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)
