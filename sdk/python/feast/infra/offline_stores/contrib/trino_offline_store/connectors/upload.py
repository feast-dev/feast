"""
Connectors can be found in the following doc https://trino.io/docs/current/connector.html

For the hive connector, all file formats are here
https://trino.io/docs/current/connector/hive.html#supported-file-types

Example yaml config
```yaml
offline_store:
    type: trino
    host: localhost
    port: 8080
    catalog: hive
    dataset: ci
    connector:
        type: hive
        file_format: parquet
```
"""

from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional, Set

import numpy as np
import pandas as pd
import pyarrow

from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import Trino
from feast.infra.offline_stores.contrib.trino_offline_store.trino_type_map import (
    pa_to_trino_value_type,
)

CONNECTORS_DONT_SUPPORT_CREATE_TABLE: Set[str] = {
    "druid",
    "elasticsearch",
    "googlesheets",
    "jmx",
    "kafka",
    "kinesis",
    "localfile",
    "pinot",
    "postgresql",
    "prometheus",
    "redis",
    "thrift",
    "tpcds",
    "tpch",
    "qdrant",
}
CONNECTORS_WITHOUT_WITH_STATEMENTS: Set[str] = {
    "bigquery",
    "cassandra",
    "memory",
    "mongodb",
    "mysql",
    "oracle",
    "redshift",
    "memsql",
}

CREATE_SCHEMA_QUERY_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {table} (
    {schema}
)
{with_statement}
"""

INSERT_ROWS_QUERY_TEMPLATE = """
INSERT INTO {table} ({columns})
VALUES {values}
"""


def pyarrow_schema_from_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    pyarrow_schema = pyarrow.Table.from_pandas(df).schema
    trino_schema: Dict[str, Any] = {}
    for field in pyarrow_schema:
        try:
            trino_type = pa_to_trino_value_type(str(field.type))
        except KeyError:
            raise ValueError(
                f"Not supported type '{field.type}' in entity_df for '{field.name}'."
            )
        trino_schema[field.name] = trino_type
    return trino_schema


def trino_table_schema_from_dataframe(df: pd.DataFrame) -> str:
    return ",".join(
        [
            f"{field_name} {field_type}"
            for field_name, field_type in pyarrow_schema_from_dataframe(df=df).items()
        ]
    )


def pandas_dataframe_fix_batches(
    df: pd.DataFrame, batch_size: int
) -> Iterator[pd.DataFrame]:
    for pos in range(0, len(df), batch_size):
        yield df[pos : pos + batch_size]


def format_pandas_row(df: pd.DataFrame) -> str:
    pyarrow_schema = pyarrow_schema_from_dataframe(df=df)

    def _is_nan(value: Any) -> bool:
        if value is None:
            return True

        try:
            return np.isnan(value)
        except TypeError:
            return False

    def _format_value(row: pd.Series, schema: Dict[str, Any]) -> str:
        formated_values = []
        for row_name, row_value in row.items():
            if schema[row_name].startswith("timestamp"):
                if isinstance(row_value, datetime):
                    row_value = format_datetime(row_value)
                formated_values.append(f"TIMESTAMP '{row_value}'")
            elif isinstance(row_value, list):
                formated_values.append(f"ARRAY{row_value}")
            elif isinstance(row_value, np.ndarray):
                formated_values.append(f"ARRAY{row_value.tolist()}")
            elif isinstance(row_value, tuple):
                formated_values.append(f"ARRAY{list(row_value)}")
            elif isinstance(row_value, str):
                formated_values.append(f"'{row_value}'")
            elif _is_nan(row_value):
                formated_values.append("NULL")
            else:
                formated_values.append(f"{row_value}")

        return f"({','.join(formated_values)})"

    results = df.apply(_format_value, args=(pyarrow_schema,), axis=1).tolist()
    return ",".join(results)


def format_datetime(t: datetime) -> str:
    if t.tzinfo:
        t = t.astimezone(tz=timezone.utc)
    return t.strftime("%Y-%m-%d %H:%M:%S.%f")


def upload_pandas_dataframe_to_trino(
    client: Trino,
    df: pd.DataFrame,
    table: str,
    connector_args: Optional[Dict[str, str]] = None,
    batch_size: int = 1000000,  # 1 million rows by default
) -> None:
    connector_args = connector_args or {}
    connector_name = connector_args["type"]

    if connector_name in CONNECTORS_DONT_SUPPORT_CREATE_TABLE:
        raise ValueError(
            f"The connector '{connector_name}' is not supported because it does not support CREATE TABLE operations"
        )
    elif connector_name in CONNECTORS_WITHOUT_WITH_STATEMENTS:
        with_statement = ""
    elif connector_name in {"hive", "iceberg"}:
        if "file_format" not in connector_args.keys():
            raise ValueError(
                f"The connector {connector_name} needs the argument 'file_format' in order to create tables with Trino"
            )
        with_statement = f"WITH (format = '{connector_args['file_format']}')"
    elif connector_name in {"kudu", "phoenix", "sqlserver"}:
        raise ValueError(
            f"The connector {connector_name} is not yet supported. PRs welcome :)"
        )
    else:
        raise ValueError(
            f"The connector {connector_name} is not part of the connectors listed in the Trino website: https://trino.io/docs/current/connector.html"
        )

    client.execute_query(
        CREATE_SCHEMA_QUERY_TEMPLATE.format(
            table=table,
            schema=trino_table_schema_from_dataframe(df=df),
            with_statement=with_statement,
        )
    )

    # Upload batchs of 1M rows at a time
    for batch_df in pandas_dataframe_fix_batches(df=df, batch_size=batch_size):
        client.execute_query(
            INSERT_ROWS_QUERY_TEMPLATE.format(
                table=table,
                columns=",".join(batch_df.columns),
                values=format_pandas_row(batch_df),
            )
        )
