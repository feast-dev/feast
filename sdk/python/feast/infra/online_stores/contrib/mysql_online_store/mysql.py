from __future__ import absolute_import
import logging
import time

from enum import Enum
from importlib import import_module
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union, cast

import pymysql
from pymysql.connections import Connection
from pymysql.cursors import Cursor

from feast import Entity, FeatureView, StreamFeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto

from feast.infra.online_stores.contrib.mysql_online_store.util import (
    MySQLOnlineStoreConfig,
    default_version_id,
    _drop_table_and_index,
    _table_id,
    _to_naive_utc,
    _execute_query_with_retry,
    _get_feature_view_version_id,
    create_feature_view_partition,
    drop_feature_view_partitions,
    drop_feature_view,
    insert_feature_view_version
)


MYSQL_WRITE_RETRIES = 3
MYSQL_READ_RETRIES = 3
MYSQL_PARTITION_EXISTS_ERROR = 1517


class ConnectionType(Enum):
    RAW = 0
    SESSION = 1


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

    def _get_store_config(self, config: RepoConfig) -> MySQLOnlineStoreConfig:
        assert isinstance(config.online_store, MySQLOnlineStoreConfig)
        return cast(MySQLOnlineStoreConfig, config.online_store)


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
        online_store_config = self._get_store_config(config=config)
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
                _execute_query_with_retry(
                    cur=cur, conn=conn, query=query, values=query_values, retries=MYSQL_WRITE_RETRIES
                )
        self._close_conn(raw_conn, conn_type)

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
                if _execute_query_with_retry(cur=cur,
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

    def online_delete(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
    ) -> bool:
        raw_conn, conn_type = self._get_conn(config, readonly=True)
        conn = raw_conn.connection if conn_type == ConnectionType.SESSION else raw_conn
        corresponding_records_deleted = True

        with conn.cursor() as cur:
            project = config.project
            for entity_key in entity_keys:
                entity_key_bin = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=2,
                ).hex()
                query = f"DELETE FROM {_table_id(project, table)} WHERE entity_key = %s"
                query_executed = self._execute_query_with_retry(
                    cur=cur,
                    conn=conn,
                    query=query,
                    values=(entity_key_bin,),
                    retries=MYSQL_READ_RETRIES)

                if not query_executed:
                    corresponding_records_deleted = False
                    logging.error(f'Skipping delete for (entity, table)): ({entity_key}, {_table_id(project, table)})')
        self._close_conn(raw_conn, conn_type)
        return corresponding_records_deleted

    def online_read_many(self,
            config: RepoConfig,
            table_list: List[FeatureView],
            entity_keys_list: List[List[EntityKeyProto]],
            _: Optional[List[Optional[List[str]]]] = None,
    ) -> List[List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]]:
        output = []
        raw_conn, conn_type = self._get_conn(config, readonly=True)
        conn = raw_conn.connection if conn_type == ConnectionType.SESSION else raw_conn
        queries = []
        with conn.cursor() as cur:
            project = config.project
            entity_key_bins = []
            for i, (table, entity_keys) in enumerate(zip(table_list, entity_keys_list)):
                for entity_key in entity_keys:
                    entity_key_bins.append(serialize_entity_key(
                        entity_key,
                        entity_key_serialization_version=2,
                    ).hex())
                    query = f"SELECT feature_name, value, event_ts, {i} as __i__ FROM {_table_id(project, table)} WHERE entity_key = %s"
                    queries.append(query)
            union_query = f"{' UNION ALL '.join(queries)}"
            if _execute_query_with_retry(cur=cur,
                                            conn=conn,
                                            query=union_query,
                                            values=entity_key_bins,
                                            retries=MYSQL_READ_RETRIES):
                res = {}
                res_ts: Optional[datetime] = None
                all_records = cur.fetchall()
                sorted_records = {i:[] for i in range(len(table_list))}
                for feature_name, val_bin, ts, i in all_records:
                    sorted_records[i].append((feature_name, val_bin, ts))
                for i in range(len(table_list)):
                    result: List[Tuple[Optional[datetime], Optional[Dict[str, Any]]]] = []
                    records = sorted_records[i]
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
                    output.append(result)
            else:
                for table, entity_keys in zip(table_list, entity_keys_list):
                    logging.error(f'Skipping read for (table, entities): ({_table_id(project, table)}, {entity_keys})')
        self._close_conn(raw_conn, conn_type)
        return output

    def _batch_create(
        self,
        cur: Cursor,
        conn: Connection,
        config: MySQLOnlineStoreConfig,
        table: FeatureView,
        tries: int = 3
    ) -> None:
        """
        1.
        """
        version_id = _get_feature_view_version_id(cur=cur, conn=conn, config=config, feature_view_name=table.name)
        if version_id is not None:
            return  # FeatureView already created

        for offset in range(tries):
            version_id = default_version_id(table.name, offset=offset)
            try:
                # open transaction but do not commit
                insert_feature_view_version(
                    cur=cur,
                    conn=conn,
                    config=config,
                    feature_view_name=table.name,
                    version_id=version_id,
                    lock_row=False,
                    commit=False
                )

                # alter table
                create_feature_view_partition(
                    cur=cur, conn=conn, config=config, version_id=version_id
                )
                break
            except pymysql.Error as e:
                if e.args[0] == MYSQL_PARTITION_EXISTS_ERROR:
                    conn.rollback()
                    logging.warning(f'Partition ({version_id}) already exists in table ({config.feature_data_table}).'
                                    f'Attempting insert with a different version id.')
                    continue
                raise e

    def _legacy_create(
            self,
            cur: Cursor,
            conn: Connection,
            project: str,
            table: FeatureView,
    ) -> None:
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
        mysql_config = self._get_store_config(config=config)

        with conn.cursor() as cur:
            project = config.project
            # We don't create any special state for the entities in this implementation.
            for table in tables_to_keep:
                try:
                    if isinstance(table, StreamFeatureView):
                        self._legacy_create(
                            cur=cur, conn=conn, project=project, table=table,
                        )
                    else:
                        self._batch_create(
                            cur=cur, conn=conn, config=mysql_config, table=table
                        )
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
        # RB / TODO: this function has questionable functionality in prod
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

    def _legacy_teardown(
            self,
            cur: Cursor,
            conn: Connection,
            table: StreamFeatureView,
            project: str
    ):
        try:
            _drop_table_and_index(cur, project, table)
            conn.commit()
        except pymysql.Error as e:
            conn.rollback()
            logging.error("Error %d: %s"
                          "" % (e.args[0], e.args[1]))

    def _batch_teardown(
            self,
            cur: Cursor,
            conn: Connection,
            table: FeatureView,
            config: MySQLOnlineStoreConfig
    ) -> None:
        try:
            version_id = _get_feature_view_version_id(
                cur=cur,
                conn=conn,
                config=config,
                feature_view_name=table.name
            )
            if version_id is None:
                logging.error(f'Unable to teardown FeatureView ({table.name}) '
                              f'because version_id is None.')
            else:
                # RB / TODO: this code only drops the latest partitions; GC during apply time in prod can be hairy
                #            for local we should definitely drop all.
                drop_feature_view_partitions(
                    cur=cur, conn=conn, config=config, version_ids=[version_id]
                )
                drop_feature_view(
                    cur=cur, conn=conn, config=config, feature_view_name=table.name, version_id=version_id
                )
        except Exception as e:
            logging.error(f"Unable to teardown FeatureView ({table.name}) table ({config.feature_data_table})"
                          f"due to exception {e}.")
            conn.rollback()
            raise e

    def teardown(
            self,
            config: RepoConfig,
            tables: Sequence[FeatureView],
            entities: Sequence[Entity],
    ) -> None:
        raw_conn, conn_type = self._get_conn(config)
        mysql_config = self._get_store_config(config)
        conn = raw_conn.connection if conn_type == ConnectionType.SESSION else raw_conn
        with conn.cursor() as cur:
            for table in tables:
                if isinstance(table, StreamFeatureView):
                    self._legacy_teardown(
                        cur=cur,
                        conn=conn,
                        table=cast(StreamFeatureView, table),
                        project=config.project
                    )
                else:
                    self._batch_teardown(
                        cur=cur,
                        conn=conn,
                        table=table,
                        config=mysql_config
                    )
        self._close_conn(raw_conn, conn_type)
