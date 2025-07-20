from typing import Any, Dict

import numpy as np
import pandas as pd
import psycopg
import pyarrow as pa
from psycopg import AsyncConnection, Connection
from psycopg.conninfo import make_conninfo
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from feast.infra.utils.postgres.postgres_config import PostgreSQLConfig
from feast.type_map import arrow_to_pg_type


def _get_conn(config: PostgreSQLConfig) -> Connection:
    """Get a psycopg `Connection`."""
    conn = psycopg.connect(
        conninfo=_get_conninfo(config),
        keepalives_idle=config.keepalives_idle,
        **_get_conn_kwargs(config),
    )
    return conn


async def _get_conn_async(config: PostgreSQLConfig) -> AsyncConnection:
    """Get a psycopg `AsyncConnection`."""
    conn = await psycopg.AsyncConnection.connect(
        conninfo=_get_conninfo(config),
        keepalives_idle=config.keepalives_idle,
        **_get_conn_kwargs(config),
    )
    return conn


def _get_connection_pool(config: PostgreSQLConfig) -> ConnectionPool:
    """Get a psycopg `ConnectionPool`."""
    return ConnectionPool(
        conninfo=_get_conninfo(config),
        min_size=config.min_conn,
        max_size=config.max_conn,
        open=False,
        kwargs=_get_conn_kwargs(config),
    )


async def _get_connection_pool_async(config: PostgreSQLConfig) -> AsyncConnectionPool:
    """Get a psycopg `AsyncConnectionPool`."""
    return AsyncConnectionPool(
        conninfo=_get_conninfo(config),
        min_size=config.min_conn,
        max_size=config.max_conn,
        open=False,
        kwargs=_get_conn_kwargs(config),
    )


def _get_conninfo(config: PostgreSQLConfig) -> str:
    """Get the `conninfo` argument required for connection objects."""
    return make_conninfo(
        conninfo="",
        user=config.user,
        password=config.password,
        host=config.host,
        port=int(config.port),
        dbname=config.database,
    )


def _get_conn_kwargs(config: PostgreSQLConfig) -> Dict[str, Any]:
    """Get the additional `kwargs` required for connection objects."""
    return {
        "sslmode": config.sslmode,
        "sslkey": config.sslkey_path,
        "sslcert": config.sslcert_path,
        "sslrootcert": config.sslrootcert_path,
        "options": "-c search_path={}".format(config.db_schema or config.user),
    }


def _df_to_create_table_sql(entity_df, table_name) -> str:
    pa_table = pa.Table.from_pandas(entity_df)
    columns = [
        f""""{f.name}" {arrow_to_pg_type(str(f.type))}""" for f in pa_table.schema
    ]
    return f"""
        CREATE TABLE "{table_name}" (
            {", ".join(columns)}
        );
        """


def df_to_postgres_table(
    config: PostgreSQLConfig, df: pd.DataFrame, table_name: str
) -> Dict[str, np.dtype]:
    """
    Create a table for the data frame, insert all the values, and return the table schema
    """
    nr_columns = df.shape[1]
    placeholders = ", ".join(["%s"] * nr_columns)
    query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    values = df.replace({np.nan: None}).to_numpy().tolist()

    with _get_conn(config) as conn, conn.cursor() as cur:
        cur.execute(_df_to_create_table_sql(df, table_name))
        cur.executemany(query, values)
        return dict(zip(df.columns, df.dtypes))


def get_query_schema(config: PostgreSQLConfig, sql_query: str) -> Dict[str, np.dtype]:
    """
    We'll use the statement when we perform the query rather than copying data to a
    new table
    """
    with _get_conn(config) as conn:
        conn.read_only = True
        df = pd.read_sql(
            f"SELECT * FROM {sql_query} LIMIT 0",
            conn,
        )
        return dict(zip(df.columns, df.dtypes))
