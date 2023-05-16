from __future__ import absolute_import
import logging
import time

from enum import Enum
from importlib import import_module
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pymysql
import pytz
from pydantic import StrictStr
from pymysql.connections import Connection
from pymysql.cursors import Cursor

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel

MYSQL_DEADLOCK_ERR = 1213
MYSQL_WRITE_RETRIES = 3
MYSQL_READ_RETRIES = 3


class ConnectionType(Enum):
    RAW = 0
    SESSION = 1


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
    session_manager_module: Optional[StrictStr] = None


class MySQLOnlineStore(OnlineStore):
    """
    An online store implementation that uses MySQL.
    NOTE: The class *must* end with the `OnlineStore` suffix.
    """

    """
    RB: Connections should not be shared between threads: https://stackoverflow.com/questions/45636492/can-mysqldb-connection-and-cursor-objects-be-safely-used-from-with-multiple-thre
    """

    conn: Optional[Connection] = None

    def __init__(self) -> None:
        self.dbsession = None
        self.ro_dbsession = None

    def _get_conn_session_manager(self, session_manager_module: str, readonly: bool = False) -> Connection:
        dbsession = self.ro_dbsession if readonly else self.dbsession
        if dbsession is None:
            mod = import_module(session_manager_module)
            dbsession, cache_session = mod.generate_session(readonly=readonly)
            if cache_session and readonly:
                self.ro_dbsession = dbsession
            elif cache_session:
                self.dbsession = dbsession
        return dbsession.get_bind(0).contextual_connect(close_with_result=False)

    def _get_conn(self, config: RepoConfig, readonly: bool = False) -> Union[Connection, ConnectionType]:
        online_store_config = config.online_store
        assert isinstance(online_store_config, MySQLOnlineStoreConfig)

        if online_store_config.session_manager_module:
            return (
                self._get_conn_session_manager(
                    session_manager_module=online_store_config.session_manager_module,
                    readonly=readonly
                ),
                ConnectionType.SESSION
            )
        elif self.conn is None or not self.conn.open:
            self.conn = pymysql.connect(
                host=online_store_config.host or "127.0.0.1",
                user=online_store_config.user or "test",
                password=online_store_config.password or "test",
                database=online_store_config.database or "feast",
                port=online_store_config.port or 3306,
                autocommit=False,
            )
            assert self.conn.get_autocommit() is False
        return self.conn, ConnectionType.RAW

    def _close_conn(self, conn: Connection, conn_type: ConnectionType) -> None:
        if conn_type == ConnectionType.SESSION:
            try:
                conn.close()
            except Exception as exc:
                if str(exc) != 'Already closed':
                    raise exc
        else:
            try:
                conn.close()
            except Exception:
                pass

    def _execute_query_with_retry(self, cur: Cursor,
                                        conn: Connection,
                                        query: str,
                                        values: Union[List, Tuple],
                                        retries: int,
                                        progress=None
    ) -> bool:
        for _ in range(retries):
            try:
                cur.execute(query, values)
                conn.commit()
                if progress:
                    progress(1)
                return True
            except pymysql.Error as e:
                if e.args[0] == MYSQL_DEADLOCK_ERR:
                    time.sleep(0.5)
                else:
                    conn.rollback()
                    logging.error("Error %d: %s" % (e.args[0], e.args[1]))
                    return False
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
        raw_conn, conn_type = self._get_conn(config)
        conn = raw_conn.connection if conn_type == ConnectionType.SESSION else raw_conn
        with conn.cursor() as cur:
            project = config.project

            for entity_key, values, timestamp, created_ts in data:
                entity_key_bin = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=2,
                ).hex()
                timestamp = _to_naive_utc(timestamp)
                if created_ts is not None:
                    created_ts = _to_naive_utc(created_ts)

                rows_to_insert = [(entity_key_bin, feature_name, val.SerializeToString(), timestamp, created_ts)
                                  for feature_name, val in values.items()]
                value_formatters = ', '.join(['(%s, %s, %s, %s, %s)'] * len(rows_to_insert))
                query = f"""
                        INSERT INTO {_table_id(project, table)}
                        (entity_key, feature_name, value, event_ts, created_ts)
                        VALUES {value_formatters}
                        ON DUPLICATE KEY UPDATE 
                        value = VALUES(value),
                        event_ts = VALUES(event_ts),
                        created_ts = VALUES(created_ts)
                        """
                query_values = [item for row in rows_to_insert for item in row]
                self._execute_query_with_retry(cur=cur,
                                               conn=conn,
                                               query=query,
                                               values=query_values,
                                               retries=MYSQL_WRITE_RETRIES)
        self._close_conn(raw_conn, conn_type)

    def bulk_insert(
            self,
            config: RepoConfig,
            table: FeatureView,
            data: List[
                Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
            ],
            batch_size: int = 10000
    ) -> None:
        raw_conn, conn_type = self._get_conn(config)
        conn = raw_conn.connection if conn_type == ConnectionType.SESSION else raw_conn

        with conn.cursor() as cur:
            project = config.project
            for i in range(0, len(data), batch_size):
                print(f'Inserting batch {batch} of {batch_size}....')
                start = time.time()
                batch = data[i:i + batch_size]
                rows_to_insert = []
                for entity_key, values, timestamp, created_ts in batch:
                    entity_key_bin = serialize_entity_key(
                        entity_key,
                        entity_key_serialization_version=2,
                    ).hex()
                    timestamp = _to_naive_utc(timestamp)
                    if created_ts is not None:
                        created_ts = _to_naive_utc(created_ts)

                    rows_to_insert += [(entity_key_bin, feature_name, val.SerializeToString(), timestamp, created_ts)
                                      for feature_name, val in values.items()]
                value_formatters = ', '.join(['(%s, %s, %s, %s, %s)'] * len(rows_to_insert))
                query = f"""
                        INSERT INTO {_table_id(project, table)}
                        (entity_key, feature_name, value, event_ts, created_ts)
                        VALUES {value_formatters}
                        """
                cur.execute(query)
                conn.commit()
                end = time.time()
                print(f'batch elapsed time: {end - start}')

    def online_read(
            self,
            config: RepoConfig,
            table: FeatureView,
            entity_keys: List[EntityKeyProto],
            _: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        raw_conn, conn_type = self._get_conn(config, readonly=True)
        conn = raw_conn.connection if conn_type == ConnectionType.SESSION else raw_conn
        with conn.cursor() as cur:
            result: List[Tuple[Optional[datetime], Optional[Dict[str, Any]]]] = []
            project = config.project
            for entity_key in entity_keys:
                entity_key_bin = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=2,
                ).hex()
                query = f"SELECT feature_name, value, event_ts FROM {_table_id(project, table)} WHERE entity_key = %s"
                if self._execute_query_with_retry(cur=cur,
                                                  conn=conn,
                                                  query=query,
                                                  values=(entity_key_bin,),
                                                  retries=MYSQL_READ_RETRIES):
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
                else:
                    logging.error(f'Skipping read for (entity, table)): ({entity_key}, {_table_id(project, table)})')
        self._close_conn(raw_conn, conn_type)
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
        raw_conn, conn_type = self._get_conn(config)
        conn = raw_conn.connection if conn_type == ConnectionType.SESSION else raw_conn
        with conn.cursor() as cur:
            project = config.project
            # We don't create any special state for the entities in this implementation.
            for table in tables_to_keep:
                try:
                    cur.execute(
                        f"""CREATE TABLE IF NOT EXISTS {_table_id(project, table)} (entity_key VARCHAR(512),
                        feature_name VARCHAR(256),
                        value BLOB,
                        event_ts timestamp NULL DEFAULT NULL,
                        created_ts timestamp NULL DEFAULT NULL,
                        PRIMARY KEY(entity_key, feature_name))"""
                    )
                    cur.execute(
                        f"SHOW INDEXES FROM {_table_id(project, table)};"
                    )

                    index_exists = False
                    for index in cur.fetchall():
                        if index[2] == f"{_table_id(project, table)}_ek":
                            index_exists = True
                            break

                    if not index_exists:
                        cur.execute(
                            f"ALTER TABLE {_table_id(project, table)} ADD INDEX {_table_id(project, table)}_ek (entity_key);"
                        )
                    conn.commit()
                except pymysql.Error as e:
                    conn.rollback()
                    logging.error("Error %d: %s" % (e.args[0], e.args[1]))

            for table in tables_to_delete:
                try:
                    _drop_table_and_index(cur, project, table)
                    conn.commit()
                except pymysql.Error as e:
                    conn.rollback()
                    logging.error("Error %d: %s" % (e.args[0], e.args[1]))
        self._close_conn(raw_conn, conn_type)

    def clear_table(
            self,
            config: RepoConfig,
            table: FeatureView
    ) -> None:
        raw_conn, conn_type = self._get_conn(config)
        conn = raw_conn.connection if conn_type == ConnectionType.SESSION else raw_conn
        with conn.cursor() as cur:
            table_name = _table_id(config.project, table)
            try:
                cur.execute(f"DELETE FROM {table_name};")
                conn.commit()
            except pymysql.Error as e:
                conn.rollback()
                logging.error("Error %d: %s"
                              "" % (e.args[0], e.args[1]))
        self._close_conn(raw_conn, conn_type)

    def teardown(
            self,
            config: RepoConfig,
            tables: Sequence[FeatureView],
            entities: Sequence[Entity],
    ) -> None:
        raw_conn, conn_type = self._get_conn(config)
        conn = raw_conn.connection if conn_type == ConnectionType.SESSION else raw_conn
        with conn.cursor() as cur:
            project = config.project
            for table in tables:
                try:
                    _drop_table_and_index(cur, project, table)
                    conn.commit()
                except pymysql.Error as e:
                    conn.rollback()
                    logging.error("Error %d: %s"
                                  "" % (e.args[0], e.args[1]))
        self._close_conn(raw_conn, conn_type)


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
