from __future__ import absolute_import
import logging

from importlib import import_module
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union, cast

import pymysql
from pymysql.cursors import Cursor

from feast import Entity, FeatureView, StreamFeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key, serialize_entity_key_plaintext
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.exceptions import OnlineStoreError

from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto

from feast.infra.online_stores.contrib.mysql_online_store.defs import (
    ConnectionType,
    Connection,
    PyMySQLConnection,
    MYSQL_WRITE_RETRIES,
    MYSQL_READ_RETRIES,
    MYSQL_PARTITION_EXISTS_ERROR
)

from feast.infra.online_stores.contrib.mysql_online_store.util import (
    MySQLOnlineStoreConfig,
    default_version_id,
    unpack_read_features,
    drop_table_and_index,
    table_id,
    to_naive_utc,
    execute_query_with_retry,
    get_feature_view_version_id,
    create_feature_view_partition,
    drop_feature_view_partitions,
    drop_feature_view,
    insert_feature_view_version,
    build_feature_view_features_read_query,
    build_legacy_features_read_query,
    build_insert_query_for_entity,
    build_legacy_insert_query_for_entity
)


class MySQLOnlineStore(OnlineStore):
    """
    An online store implementation that uses MySQL.
    NOTE: The class *must* end with the `OnlineStore` suffix.
    """

    # pymysql connection used when working with a raw PyMySQL connection (not managed by SQLAlchemy)
    # don't use this unless you need to.
    conn: Optional[Connection] = None

    def __init__(self) -> None:
        self.dbsession = None
        self.ro_dbsession = None

    @staticmethod
    def get_store_config(config: RepoConfig) -> MySQLOnlineStoreConfig:
        # extracts online store config from RepoConfig
        assert isinstance(config.online_store, MySQLOnlineStoreConfig)
        return cast(MySQLOnlineStoreConfig, config.online_store)

    def _get_conn_session_manager(self, session_manager_module: str, readonly: bool = False) -> Connection:
        dbsession = self.ro_dbsession if readonly else self.dbsession
        if dbsession is None:
            mod = import_module(session_manager_module)
            dbsession, cache_session = mod.generate_session(readonly=readonly)

            # save dbsessions, if specified (used for stream processing)
            if cache_session and readonly:
                self.ro_dbsession = dbsession
            elif cache_session:
                self.dbsession = dbsession
        return dbsession.get_bind(0).contextual_connect(close_with_result=False)

    def _get_conn(self, config: RepoConfig, readonly: bool = False) -> Union[Connection, ConnectionType]:
        online_store_config = self.get_store_config(config=config)
        if online_store_config.session_manager_module:
            return (
                self._get_conn_session_manager(
                    session_manager_module=online_store_config.session_manager_module,
                    readonly=readonly
                ),
                ConnectionType.SESSION
            )
        elif self.conn is None or not cast(PyMySQLConnection, self.conn).open:
            self.conn = pymysql.connect(
                host=online_store_config.host or "127.0.0.1",
                user=online_store_config.user or "test",
                password=online_store_config.password or "test",
                database=online_store_config.database or "feast",
                port=online_store_config.port or 3306,
                autocommit=False,
            )
            assert self.conn.get_autocommit() is False
        # remark: autocommit=0 for both SQLAlchemy connections and PyMySQL connections
        return self.conn, ConnectionType.RAW

    @staticmethod
    def _get_pymysql_conn(raw_conn: Connection, conn_type: ConnectionType) -> PyMySQLConnection:
        # extracts PyMySQL Connection object, which is used for committing transactions.
        # SQLAlchemy connections don't allow you to bind a cursor, so we need the db connection.
        return raw_conn.connection if conn_type == ConnectionType.SESSION else raw_conn

    @staticmethod
    def _close_conn(conn: Connection, conn_type: ConnectionType) -> None:
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

    @staticmethod
    def _legacy_batch_write(
            cur: Cursor,
            conn: PyMySQLConnection,
            table: FeatureView,
            data: List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]],
            project: str
    ) -> None:
        table_name = table_id(project, table)
        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=2,
            ).hex()
            timestamp = to_naive_utc(timestamp)

            if created_ts is not None:
                created_ts = to_naive_utc(created_ts)

            rows_to_insert = [
                (entity_key_bin, feature_name, val.SerializeToString(), timestamp, created_ts)
                for feature_name, val in values.items()
            ]
            value_formatters = ', '.join(['(%s, %s, %s, %s, %s)'] * len(rows_to_insert))
            query = build_legacy_insert_query_for_entity(table=table_name, value_formatters=value_formatters)
            query_values = [item for row in rows_to_insert for item in row]
            execute_query_with_retry(
                cur=cur, conn=conn, query=query, values=query_values, retries=MYSQL_WRITE_RETRIES
            )

    @staticmethod
    def _batch_write(
            cur: Cursor,
            conn: PyMySQLConnection,
            table: FeatureView,
            config: MySQLOnlineStoreConfig,
            data: List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]],
    ) -> None:
        raw_version_id = get_feature_view_version_id(cur=cur, conn=conn, config=config, feature_view_name=table.name)
        if raw_version_id is None:
            raise OnlineStoreError(f'FeatureView ({table.name}) does not have a version id. '
                                   f'Rows cannot be inserted until a version id is created.')
        version_id = cast(int, raw_version_id)
        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key_plaintext(
                entity_key,
            )

            event_ts = to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = to_naive_utc(created_ts)

            rows_to_insert = [
                (version_id, table.name, entity_key_bin, feature_name, val.SerializeToString(), event_ts, created_ts)
                for feature_name, val in values.items()
            ]
            value_formatters = ', '.join(['(%s, %s, %s, %s, %s, %s, %s)'] * len(rows_to_insert))
            query = build_insert_query_for_entity(table=table.name, value_formatters=value_formatters)
            query_values = [item for row in rows_to_insert for item in row]
            execute_query_with_retry(
                cur=cur, conn=conn, query=query, values=query_values, commit=True, retries=MYSQL_WRITE_RETRIES
            )

    def online_write_batch(
            self,
            config: RepoConfig,
            table: FeatureView,
            data: List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]],
            progress: Optional[Callable[[int], Any]],
    ) -> None:
        raw_conn, conn_type = self._get_conn(config)
        pymysql_conn = self._get_pymysql_conn(raw_conn=raw_conn, conn_type=conn_type)

        mysql_config = self.get_store_config(config=config)
        legacy_table = isinstance(table, StreamFeatureView)
        try:
            with pymysql_conn.cursor() as cur:
                if legacy_table:
                    self._legacy_batch_write(cur=cur, conn=pymysql_conn, table=table, data=data, project=config.project)
                elif isinstance(table, FeatureView):
                    self._batch_write(cur=cur, conn=pymysql_conn, table=table, config=mysql_config, data=data)
                else:
                    raise OnlineStoreError(f'Attempted batch write to unknown feature view type ({table}).')
        except Exception as e:
            pymysql_conn.rollback()
            if not legacy_table:
                raise e
            logging.error(
                f"Unable to perform online_write_batch on FeatureView ({table.name}) due to exception ({e})."
            )
        finally:
            self._close_conn(raw_conn, conn_type)

    def online_read(
            self,
            config: RepoConfig,
            table: FeatureView,
            entity_keys: List[EntityKeyProto],
            _: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        raw_conn, conn_type = self._get_conn(config, readonly=True)
        pymysql_conn = self._get_pymysql_conn(raw_conn=raw_conn, conn_type=conn_type)

        mysql_config = self.get_store_config(config)
        project = config.project
        legacy_table = isinstance(table, StreamFeatureView)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, Any]]]] = []
        try:
            with pymysql_conn.cursor() as cur:
                for entity_key in entity_keys:
                    if legacy_table:
                        entity_key_bin = serialize_entity_key(
                            entity_key,
                            entity_key_serialization_version=2,
                        ).hex()
                        query = build_legacy_features_read_query(project=project, table=table)
                    elif isinstance(table, FeatureView):
                        entity_key_bin = serialize_entity_key_plaintext(
                            entity_key,
                        )
                        query = build_feature_view_features_read_query(config=mysql_config, feature_view_name=table.name)
                    else:
                        raise OnlineStoreError(f'Attempted batch read to unknown feature view type ({table}).')

                    try:
                        execute_query_with_retry(
                            cur=cur,
                            conn=pymysql_conn,
                            query=query,
                            values=(entity_key_bin,),
                            commit=True,
                            retries=MYSQL_READ_RETRIES
                        )
                        records = cur.fetchall()
                        result.append(unpack_read_features(records=records))
                    except Exception as e:
                        if legacy_table:
                            logging.exception(
                                f'Skipping read for (entity, table)): ({entity_key}, {table_id(project, table)})'
                            )
                            continue
                        raise e
        finally:
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
        raw_conn, conn_type = self._get_conn(config, readonly=True)
        pymysql_conn = self._get_pymysql_conn(raw_conn=raw_conn, conn_type=conn_type)

        mysql_config = self.get_store_config(config)
        queries, output = [], []
        use_legacy_handling = False
        try:
            with pymysql_conn.cursor() as cur:
                project = config.project
                entity_key_bins = []
                for i, (table, entity_keys) in enumerate(zip(table_list, entity_keys_list)):
                    # TODO: this can probably be made more efficient now that we're querying from one table
                    use_legacy_handling |= isinstance(table, StreamFeatureView)
                    for entity_key in entity_keys:
                        if isinstance(table, StreamFeatureView):
                            entity_key_bins.append(serialize_entity_key(
                                entity_key,
                                entity_key_serialization_version=2,
                            ).hex())
                            query = build_legacy_features_read_query(project=project, table=table, union_offset=i)
                        else:
                            entity_key_bins.append(serialize_entity_key_plaintext(
                                entity_key
                            ))
                            query = build_feature_view_features_read_query(
                                config=mysql_config, feature_view_name=table.name, union_offset=i
                            )
                        queries.append(query)

                union_query = f"{' UNION ALL '.join(queries)}"
                try:
                    execute_query_with_retry(
                        cur=cur,
                        conn=pymysql_conn,
                        query=union_query,
                        values=entity_key_bins,
                        commit=True,
                        retries=MYSQL_READ_RETRIES
                    )
                except Exception as e:
                    if use_legacy_handling:
                        for table, entity_keys in zip(table_list, entity_keys_list):
                            logging.error(
                                f'Skipping read for (table, entities): ({table_id(project, table)}, {entity_keys})'
                            )
                        return output
                    raise e

                res = {}
                res_ts: Optional[datetime] = None
                all_records = cur.fetchall()
                sorted_records = {i: [] for i in range(len(table_list))}
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
        finally:
            self._close_conn(raw_conn, conn_type)
        return output

    def _batch_create(
        self,
        cur: Cursor,
        conn: PyMySQLConnection,
        config: MySQLOnlineStoreConfig,
        table: FeatureView,
        tries: int = 3,
        **kwargs
    ) -> None:
        for offset in range(tries):
            version_id = get_feature_view_version_id(cur=cur, conn=conn, config=config, feature_view_name=table.name)
            if version_id is not None:
                return  # FeatureView already created

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
                return
            except Exception as e:
                conn.rollback()
                if isinstance(e, pymysql.Error) and e.args[0] == MYSQL_PARTITION_EXISTS_ERROR:
                    logging.warning(f'Partition ({version_id}) already exists in table ({config.feature_data_table}).'
                                    f'Attempting insert with a different version id.')
                    continue
                raise e
        raise OnlineStoreError(f'Max retries ({tries}) reached for _batch_create and FeatureView ({table.name})')

    def _legacy_create(
            self,
            cur: Cursor,
            conn: PyMySQLConnection,
            project: str,
            table: FeatureView,
    ) -> None:
        try:
            cur.execute(
                f"""CREATE TABLE IF NOT EXISTS {table_id(project, table)} (entity_key VARCHAR(512),
                                    feature_name VARCHAR(256),
                                    value BLOB,
                                    event_ts timestamp NULL DEFAULT NULL,
                                    created_ts timestamp NULL DEFAULT NULL,
                                    PRIMARY KEY(entity_key, feature_name))"""
            )
            cur.execute(
                f"SHOW INDEXES FROM {table_id(project, table)};"
            )

            index_exists = False
            for index in cur.fetchall():
                if index[2] == f"{table_id(project, table)}_ek":
                    index_exists = True
                    break

            if not index_exists:
                cur.execute(
                    f"ALTER TABLE {table_id(project, table)} ADD INDEX {table_id(project, table)}_ek (entity_key);"
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e

    def update(
            self,
            config: RepoConfig,
            tables_to_delete: Sequence[FeatureView],
            tables_to_keep: Sequence[FeatureView],
            entities_to_delete: Sequence[Entity],
            entities_to_keep: Sequence[Entity],
            partial: bool,
            **kwargs
    ) -> None:
        raw_conn, conn_type = self._get_conn(config)
        pymysql_conn = self._get_pymysql_conn(raw_conn=raw_conn, conn_type=conn_type)
        mysql_config = self.get_store_config(config=config)

        project = config.project
        try:
            with pymysql_conn.cursor() as cur:
                # We don't create any special state for the entities in this implementation.
                for table in tables_to_keep:
                    if isinstance(table, StreamFeatureView):
                        self._legacy_create(
                            cur=cur, conn=pymysql_conn, project=project, table=table,
                        )
                    else:
                        self._batch_create(
                            cur=cur, conn=pymysql_conn, config=mysql_config, table=table
                        )

                for table in tables_to_delete:
                    drop_table_and_index(cur, pymysql_conn, project, table)
        finally:
            self._close_conn(raw_conn, conn_type)

    def clear_table(
            self,
            config: RepoConfig,
            table: FeatureView
    ) -> None:
        # RB / TODO: this function has questionable functionality in prod
        raw_conn, conn_type = self._get_conn(config)
        pymysql_conn = self._get_pymysql_conn(raw_conn=raw_conn, conn_type=conn_type)

        with pymysql_conn.cursor() as cur:
            table_name = table_id(config.project, table)
            try:
                cur.execute(f"DELETE FROM {table_name};")
                pymysql_conn.commit()
            except pymysql.Error as e:
                pymysql_conn.rollback()
                logging.error("Error %d: %s"
                              "" % (e.args[0], e.args[1]))
        self._close_conn(raw_conn, conn_type)

    def _legacy_teardown(
            self,
            cur: Cursor,
            conn: PyMySQLConnection,
            table: StreamFeatureView,
            project: str
    ):
        drop_table_and_index(cur, conn, project, table)

    def _batch_teardown(
            self,
            cur: Cursor,
            conn: PyMySQLConnection,
            table: FeatureView,
            config: MySQLOnlineStoreConfig
    ) -> None:
        try:
            version_id = get_feature_view_version_id(
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
        pymysql_conn = self._get_pymysql_conn(raw_conn=raw_conn, conn_type=conn_type)

        mysql_config = self.get_store_config(config)
        try:
            with pymysql_conn.cursor() as cur:
                for table in tables:
                    if isinstance(table, StreamFeatureView):
                        drop_table_and_index(
                            cur=cur, conn=pymysql_conn, project=config.project, table=table
                        )
                    elif isinstance(table, FeatureView):
                        self._batch_teardown(
                            cur=cur,
                            conn=pymysql_conn,
                            table=table,
                            config=mysql_config
                        )
                    else:
                        raise ValueError(f'Unknown feature view type: {table}.')
        finally:
            self._close_conn(raw_conn, conn_type)
