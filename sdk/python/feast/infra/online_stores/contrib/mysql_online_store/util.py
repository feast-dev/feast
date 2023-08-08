from __future__ import absolute_import
import logging
import time
import hashlib

from datetime import datetime
from typing import Any, Callable, List, Optional, Tuple, Union, Dict, Iterable

import pymysql
import pytz
from pydantic import StrictStr
from pymysql.connections import Connection
from pymysql.cursors import Cursor

from feast import FeatureView
from feast.repo_config import FeastConfigBaseModel

from feast.infra.online_stores.contrib.mysql_online_store.defs import (
    MYSQL_DEADLOCK_ERR
)
from feast.infra.online_stores.exceptions import OnlineStoreError
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


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

    feature_version_table: str = ''
    feature_data_table: str = ''


def table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


def drop_table_and_index(cur: Cursor, project: str, table: FeatureView) -> None:
    table_name = table_id(project, table)
    cur.execute(f"DROP INDEX {table_name}_ek ON {table_name};")
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def get_partition_id_from_version(version_id: int) -> str:
    return f'p_{version_id}'


def to_naive_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)


def default_version_id(feature_view_name: str, offset: int = 0) -> int:
    offset_bstr = (feature_view_name + '_' + str(offset)).encode('utf-8')
    return int.from_bytes(hashlib.sha256(offset_bstr).digest()[:8], 'little')


def build_legacy_features_read_query(
    project: str,
    table: FeatureView,
    union_offset: Optional[int] = None
) -> str:
    base = "SELECT feature_name, value, event_ts" + " " if union_offset is None else f", {union_offset} as __i__ "
    base += f"FROM {table_id(project, table)} WHERE entity_key = %s"
    return base


def build_feature_view_features_read_query(
    config: MySQLOnlineStoreConfig,
    feature_view_name: str,
    union_offset: Optional[int] = None
) -> str:
    version_table, data_table = config.feature_version_table, config.feature_version_table
    base = "SELECT feature_name, value, event_ts" + "" if union_offset is None else f", {union_offset} as __i__"
    base += f" FROM {data_table} WHERE feature_view_name = {feature_view_name} AND entity_id = %s IN "
    base += f"(SELECT version_id FROM {data_table} WHERE feature_view_name = {feature_view_name});"
    return base


def build_insert_query_for_entity(
    table: str,
    value_formatters: str
) -> str:
    return \
        f"""
        INSERT INTO {table}
        (entity_key, feature_name, value, event_ts, created_ts)
        VALUES {value_formatters}
        ON DUPLICATE KEY UPDATE 
        value = VALUES(value),
        event_ts = VALUES(event_ts),
        created_ts = VALUES(created_ts)
        """


def unpack_read_features(
    records: Optional[Iterable],
) -> Tuple[Optional[datetime], Optional[Dict[str, Any]]]:
    res: Dict[str, Any] = {}
    res_ts: Optional[datetime] = None
    if records:
        for feature_name, val_bin, ts in records:
            val = ValueProto()
            val.ParseFromString(val_bin)
            res[feature_name] = val
            res_ts = ts
    return res if res else None, res_ts if res else None


def execute_query_with_retry(
    cur: Cursor,
    conn: Connection,
    query: str,
    values: Union[List, Tuple],
    retries: int = 3,
    progress: Optional[Callable[[int], Any]] = None,
    commit: bool = True,
    exponential: bool = False
) -> None:
    for i in range(retries):
        try:
            cur.execute(query, values)
            if commit:
                conn.commit()
            if progress:
                progress(1)
            return
        except pymysql.Error as e:
            if e.args[0] == MYSQL_DEADLOCK_ERR:
                time.sleep(0.5 * (2 ** i) if exponential else 0.5)
                continue
            conn.rollback()
            raise e
    raise OnlineStoreError(f'Max retries ({retries}) reached for query ({query}) with values ({values}).')


def get_feature_view_version_id(
    cur: Cursor,
    conn: Connection,
    config: MySQLOnlineStoreConfig,
    feature_view_name: str,
) -> Optional[int]:
    table, version_id = config.feature_version_table, None
    cur.execute(
        "SELECT version_id FROM %s WHERE feature_view_name = %s;",
        (table, feature_view_name)
    )
    record = cur.fetchone()
    if record is not None:
        version_id = record[0]
    else:
        logging.info(f'FeatureView ({feature_view_name} not found in {table}).')
    conn.commit()
    return version_id


def insert_feature_view_version(
    cur: Cursor,
    conn: Connection,
    config: MySQLOnlineStoreConfig,
    feature_view_name: str,
    version_id: int,
    lock_row: bool = False,
    commit: bool = True
) -> None:
    write_query, table = "INSERT INTO %s VALUES (%s, %s, %s,)", config.feature_version_table
    cur.execute(write_query, (table, feature_view_name, version_id, lock_row,))
    if commit:
        conn.commit()


def _get_and_lock_feature_view_version(
    cur: Cursor,
    conn: Connection,
    config: MySQLOnlineStoreConfig,
    feature_view_name: str,
) -> Optional[int]:
    table, version_id = config.feature_version_table, None

    read_query = "SELECT version_id, alter_in_progress FROM %s WHERE feature_view_name = %s FOR UPDATE;"
    execute_query_with_retry(
        cur=cur,
        conn=conn,
        query=read_query,
        values=(table, feature_view_name,),
        commit=False,
    )
    record = cur.fetchone()
    if record is None:
        logging.info(f'FeatureView ({feature_view_name} not found in {table}).')
        conn.commit()
        return record
    version_id, alter_in_progress = record

    # RB: obey the logical row lock in FeatureViewVersion table. this code path should be unlikely.
    if alter_in_progress:
        # unlock!
        conn.commit()
        raise ValueError(f'Unable to obtain row lock for table ({table}) and row ({feature_view_name}). ALTER '
                         f'operation is in progress.')

    lock_query = "UPDATE %s SET alter_in_progress = True FROM %s WHERE feature_view_name = %s;"
    execute_query_with_retry(
        cur=cur,
        conn=conn,
        query=lock_query,
        values=(table, feature_view_name,),
        retries=20,
        commit=False,
    )
    conn.commit()
    return version_id


def _update_feature_view_version_id(
    cur: Cursor,
    conn: Connection,
    config: MySQLOnlineStoreConfig,
    feature_view_name: str,
    new_version_id: int,
) -> None:
    table, version_id = config.feature_version_table, None

    query = "SELECT alter_in_progress FROM %s WHERE feature_view_name = %s FOR UPDATE;"
    execute_query_with_retry(
        cur=cur,
        conn=conn,
        query=query,
        values=(table, feature_view_name,),
        retries=5,
        commit=False,
    )
    record = cur.fetchone()
    if record is None:
        logging.info(f'FeatureView ({feature_view_name} not found in {table}).')
        conn.commit()
        return record

    alter_in_progress = record

    if not alter_in_progress:
        conn.commit()
        raise ValueError(f'Invalid operation: table ({table}) cannot be updated for FeatureView ({feature_view_name})'
                         f'without alter_in_progress being set to True.')

    lock_query = "UPDATE %s SET version_id = %s, alter_in_progress = False FROM %s WHERE feature_view_name = %s;"
    execute_query_with_retry(
        cur=cur,
        conn=conn,
        query=lock_query,
        values=(table, new_version_id, feature_view_name,),
        retries=20,
        commit=False,
    )
    conn.commit()
    return version_id


def create_feature_view_partition(
    cur: Cursor,
    conn: Connection,
    config: MySQLOnlineStoreConfig,
    version_id: int
) -> str:
    table, partition_name = config.feature_data_table, get_partition_id_from_version(version_id=version_id)

    cur.execute(
        "SELECT COUNT(*) FROM information.schema.partitions WHERE TABLE_NAME = %s AND partition_name IS NOT NULL",
        (table,)
    )
    result = cur.fetchone()
    table_not_partitioned = result is None or result == 0
    conn.commit()

    partition_query = "ALTER TABLE %s"
    partition_query += "PARTITION BY LIST (version_id)(" if table_not_partitioned else "ADD PARTITION ("
    partition_query += "PARTITION `%s` VALUES IN (%s));"
    cur.execute(partition_query, (table, partition_name, version_id,))

    conn.commit()
    return partition_name


def drop_feature_view_partitions(
    cur: Cursor,
    conn: Connection,
    config: MySQLOnlineStoreConfig,
    version_ids: List[int],
) -> None:
    partition_names = [get_partition_id_from_version(version_id=vid) for vid in version_ids]
    partition_list = ', '.join([f"`{partition_name}`" for partition_name in partition_names])

    alter_query, query_values = "ALTER TABLE %s DROP PARTITION %s", (config.feature_data_table, partition_list,)
    execute_query_with_retry(
        cur=cur, conn=conn, query=alter_query, values=query_values, propagate_exceptions=True
    )


def unlock_feature_view_version(
    cur: Cursor,
    conn: Connection,
    config: MySQLOnlineStoreConfig,
    feature_view_name: str,
    version_id: int
) -> None:
    try:
        _update_feature_view_version_id(
            cur=cur, conn=conn, config=config, feature_view_name=feature_view_name, new_version_id=version_id
        )
    except Exception as e:
        logging.error('CRITICAL: Failed to unlock feature view version. Manual intervention may be needed if '
                      f'alter_in_progress is set for FeatureView ({feature_view_name}) and version_id ({version_id}).'
                      f'Future alter operations will be blocked until alter_in_progress is set to False.'
                      f'Please run the following query to determine if alter_in_progress is set: \n'
                      f'SELECT version_id, alter_in_progress FROM {config.feature_version_table} WHERE'
                      f'feature_view_name = \'{feature_view_name}\'')
        raise e


def drop_feature_view(
    cur: Cursor,
    conn: Connection,
    config: MySQLOnlineStoreConfig,
    feature_view_name: str,
    version_id: int
) -> None:
    # RB: version_id in this case roughly acts as a "proof of lock" value.
    query, values = "DELETE FROM %s WHERE feature_view_name = %s", (config.feature_version_table, feature_view_name,)
    try:
        execute_query_with_retry(
            cur=cur, conn=conn, query=query, values=values, propagate_exceptions=True
        )
    except Exception as e:
        unlock_feature_view_version(
            cur=cur, conn=conn, config=config, feature_view_name=feature_view_name, version_id=version_id
        )
        conn.rollback()

