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

from feast import FeatureView, StreamFeatureView
from feast.repo_config import FeastConfigBaseModel

from feast.infra.online_stores.contrib.mysql_online_store.defs import (
    MYSQL_DEADLOCK_ERR,
    FEATURE_VIEW_VERSION_TABLE_NAME
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


def table_id(project: str, fv_name: str) -> str:
    return f"{project}_{fv_name}"


def drop_table_and_index(cur: Cursor, conn: Connection, project: str, table: FeatureView) -> None:
    table_name = table_id(project, table.name)
    try:
        if isinstance(table, StreamFeatureView):
            cur.execute(f"DROP INDEX {table_name}_ek ON {table_name};")
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def get_partition_id_from_version(version_id: int) -> str:
    return f'p_{version_id}'


def to_naive_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)


def default_version_id() -> int:
    # deterministic version_id used when initializing tables for the first time
    return 0


def build_legacy_features_read_query(
    project: str,
    fv_name: str,
    union_offset: Optional[int] = None
) -> str:
    base = "SELECT feature_name, value, event_ts" + " " if union_offset is None else f", {union_offset} as __i__ "
    base += f"FROM {table_id(project, fv_name.name)} WHERE entity_key = %s"
    return base


def build_feature_view_features_read_query(
    project: str,
    fv_name: str,
    union_offset: Optional[int] = None
) -> str:
    version_table = table_id(project=project, fv_name=FEATURE_VIEW_VERSION_TABLE_NAME)
    data_table = table_id(project=project, fv_name=fv_name)

    base = "SELECT feature_name, value, event_ts" + "" if union_offset is None else f", {union_offset} as __i__"
    base += f" FROM {data_table} WHERE entity_key = %s AND version_id IN "
    base += f"(SELECT version_id FROM {version_table} WHERE feature_view_name = {fv_name});"
    return base


def build_legacy_insert_query_for_entity(
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


def build_insert_query_for_entity(
    table: str,
    value_formatters: str
) -> str:
    return \
        f"""
        INSERT INTO {table}
        (version_id, feature_view_name, entity_key, feature_name, value, event_ts, created_ts)
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
    return (res_ts, res) if res else (None, None)


def execute_query_with_retry(
    cur: Cursor,
    conn: Connection,
    query: str,
    values: Union[List, Tuple],
    retries: int = 3,
    progress: Optional[Callable[[int], Any]] = None,
    commit: bool = True,
    sleep_time_sec: float = 0.25,
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
            logging.error(f'Query ({query}) with values ({values}) failed due to exception {e}.'
                          f'Rolling back and attempting retry {i + 1} of {retries}.')
            time.sleep(sleep_time_sec * (2 ** i) if exponential else sleep_time_sec)
            conn.rollback()
            continue
        except Exception as e:
            conn.rollback()
            raise e
    raise OnlineStoreError(f'Max retries ({retries}) reached for query ({query}) with values ({values}).')


def get_feature_view_version_id(
    cur: Cursor,
    conn: Connection,
    project: str,
    feature_view: FeatureView,
) -> Optional[int]:
    table, version_id = table_id(project=project, table=FEATURE_VIEW_VERSION_TABLE_NAME), None
    cur.execute(
        "SELECT version_id FROM %s WHERE feature_view_name = %s;",
        (table, feature_view.name)
    )
    record = cur.fetchone()
    if record is not None:
        version_id = record[0]
    else:
        logging.info(f'FeatureView ({feature_view.name} not found in {table}).')
    conn.commit()
    return version_id


def insert_feature_view_version(
    cur: Cursor,
    conn: Connection,
    fv_name: str,
    version_id: int,
    project: str,
    lock_row: bool = False,
    commit: bool = True
) -> None:
    write_query = "INSERT INTO %s VALUES (%s, %s, %s,)"
    table = table_id(project=project, table=FEATURE_VIEW_VERSION_TABLE_NAME)
    cur.execute(write_query, (table, fv_name, version_id, lock_row,))
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
    fv_table: str,
    version_id: int
) -> str:
    partition_name = get_partition_id_from_version(version_id=version_id)

    cur.execute(
        "SELECT COUNT(*) FROM information.schema.partitions WHERE TABLE_NAME = %s AND partition_name IS NOT NULL",
        (fv_table,)
    )
    result = cur.fetchone()
    table_not_partitioned = result is None or result == 0

    partition_query = "ALTER TABLE %s"
    partition_query += "PARTITION BY LIST (version_id)(" if table_not_partitioned else "ADD PARTITION ("
    partition_query += "PARTITION `%s` VALUES IN (%s));"
    cur.execute(partition_query, (fv_table, partition_name, version_id,))

    conn.commit()
    return partition_name


def drop_feature_view_partitions(
    cur: Cursor,
    conn: Connection,
    fv_table: str,
    version_ids: List[int],
) -> None:
    partition_names = [get_partition_id_from_version(version_id=vid) for vid in version_ids]
    partition_list = ', '.join([f"`{partition_name}`" for partition_name in partition_names])

    alter_query, query_values = "ALTER TABLE %s DROP PARTITION %s", (fv_table, partition_list,)
    execute_query_with_retry(
        cur=cur, conn=conn, query=alter_query, values=query_values,
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


def drop_feature_view_version(
    cur: Cursor,
    conn: Connection,
    project: str,
    feature_view_name: str,
    version_id: int
) -> None:
    # RB: version_id in this case roughly acts as a "proof of lock" value.
    version_table = table_id(project=project, table=FEATURE_VIEW_VERSION_TABLE_NAME)
    query, values = "DELETE FROM %s WHERE feature_view_name = %s", (version_table, feature_view_name,)
    try:
        execute_query_with_retry(
            cur=cur, conn=conn, query=query, values=values,
        )
    except Exception as e:
        unlock_feature_view_version(
            cur=cur, conn=conn, project=project, feature_view_name=feature_view_name, version_id=version_id
        )
        raise e


def build_feature_view_version_table(
    cur: Cursor,
    conn: Connection,
    project: str
) -> str:
    table_name = table_id(project=project, table=FEATURE_VIEW_VERSION_TABLE_NAME)
    try:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS %s (feature_view_name VARCHAR(255), version_id BIGINT, version_locked BOOL)",
            table_name
        )
    except Exception as e:
        conn.rollback()
        raise e
    return table_name


def build_feature_view_data_table(
    cur: Cursor,
    conn: Connection,
    feature_view: FeatureView,
    project: str
) -> str:
    table_name = table_id(project=project, table=feature_view)
    try:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS %s ("
            "version_id BIGINT, "
            "entity_key VARCHAR(255), "
            "feature_name VARCHAR(255), "
            "value BLOB, "
            "event_ts timestamp NULL DEFAULT NULL, "
            "created_ts timestamp NULL DEFAULT NULL, "
            "PRIMARY KEY(version_id, entity_key, feature_name))"
        )
    except Exception as e:
        conn.rollback()
        raise e
    return table_name
