import urllib.parse
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow as pa

from feast.infra.utils.postgres.postgres_config import PostgreSQLConfig
from feast.type_map import arrow_to_pg_type


def _get_conn(config: PostgreSQLConfig):
    scheme: str = getattr(config, "scheme", "postgresql")
    dbname: str = config.database
    host: str = config.host
    port: int = int(getattr(config, "port", 5432))
    user: str = config.user
    password: Optional[str] = getattr(config, "password", None)
    sslmode: Optional[str] = getattr(config, "sslmode", None)
    sslkey: Optional[str] = getattr(config, "sslkey_path", None)
    sslcert: Optional[str] = getattr(config, "sslcert_path", None)
    sslrootcert: Optional[str] = getattr(config, "sslrootcert_path", None)
    options: str = f"-c search_path={config.db_schema or config.user}"

    kwargs: Dict[str, Any] = {}

    if scheme is not None and scheme not in ["postgres", "postgresql"]:
        dsn: List[str] = [scheme, "://", user]
        if password is not None:
            dsn.extend([":", password])
        dsn.extend(["@", host])
        dsn.extend([":", str(port)])
        dsn.extend(["/", dbname])

        params: Dict[str, Any] = {"options": options}

        for key in ["sslmode", "sslkey", "sslcert", "sslrootcert"]:
            val: Optional[str] = locals()[key]
            if val is not None:
                params[key] = val

        if params:
            dsn.extend(["?", urllib.parse.urlencode(params)])

        kwargs["dsn"] = "".join(dsn)

    else:
        kwargs.update(
            {
                "dbname": dbname,
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "sslmode": sslmode,
                "sslkey": sslkey,
                "sslcert": sslcert,
                "sslrootcert": sslrootcert,
                "options": options,
            }
        )

    conn = psycopg2.connect(
        **kwargs,
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
