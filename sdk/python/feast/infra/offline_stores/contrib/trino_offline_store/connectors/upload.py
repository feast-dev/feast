"""
Connectors can be found in the following doc https://trino.io/docs/current/connector.html

For the hive connector, all file formats are here
https://trino.io/docs/current/connector/hive.html#supported-file-types

Example yaml config
```yaml
offline_store:
    type: feast_trino.trino.TrinoOfflineStore
    host: localhost
    port: 8080
    catalog: hive
    dataset: ci
    connector:
        type: hive
        file_format: parquet
```
"""

from typing import Any, Dict, Optional, Set

import pandas as pd

from feast_trino.connectors.utils import (
    CREATE_SCHEMA_QUERY_TEMPLATE,
    INSERT_ROWS_QUERY_TEMPLATE,
    format_pandas_row,
    pandas_dataframe_fix_batches,
    trino_table_schema_from_dataframe,
)
from feast_trino.trino_utils import Trino

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


def upload_pandas_dataframe_to_trino(
    client: Trino,
    df: pd.DataFrame,
    table_ref: str,
    connector_args: Optional[Dict[str, Any]] = None,
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
            table_ref=table_ref,
            schema=trino_table_schema_from_dataframe(df=df),
            with_statement=with_statement,
        )
    )

    # Upload batchs of 1M rows at a time
    for batch_df in pandas_dataframe_fix_batches(df=df, batch_size=1000000):
        client.execute_query(
            INSERT_ROWS_QUERY_TEMPLATE.format(
                table_ref=table_ref,
                columns=",".join(batch_df.columns),
                values=format_pandas_row(batch_df),
            )
        )
