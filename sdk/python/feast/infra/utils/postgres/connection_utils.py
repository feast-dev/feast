from typing import Dict

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow as pa

from feast.infra.utils.postgres.postgres_config import PostgreSQLConfig
from feast.type_map import arrow_to_pg_type


def _get_conn(config: PostgreSQLConfig):
    conn = psycopg2.connect(
        dbname=config.database,
        host=config.host,
        port=int(config.port),
        user=config.user,
        password=config.password,
        sslmode=config.sslmode,
        sslkey=config.sslkey_path,
        sslcert=config.sslcert_path,
        sslrootcert=config.sslrootcert_path,
        options="-c search_path={}".format(config.db_schema or config.user),
    )
    return conn


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
    with _get_conn(config) as conn, conn.cursor() as cur:
        cur.execute(_df_to_create_table_sql(df, table_name))
        psycopg2.extras.execute_values(
            cur,
            f"""
            INSERT INTO {table_name}
            VALUES %s
            """,
            df.replace({np.NaN: None}).to_numpy(),
        )
        return dict(zip(df.columns, df.dtypes))


def get_query_schema(config: PostgreSQLConfig, sql_query: str) -> Dict[str, np.dtype]:
    """
    We'll use the statement when we perform the query rather than copying data to a
    new table
    """
    with _get_conn(config) as conn:
        conn.set_session(readonly=True)
        df = pd.read_sql(
            f"SELECT * FROM {sql_query} LIMIT 0",
            conn,
        )
        return dict(zip(df.columns, df.dtypes))
