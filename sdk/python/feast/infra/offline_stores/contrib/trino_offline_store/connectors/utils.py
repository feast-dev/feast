from datetime import datetime
from typing import Any, Dict, Iterator

import numpy as np
import pandas as pd
import pyarrow
from pytz import utc

from feast_trino.trino_type_map import pa_to_trino_value_type

CREATE_SCHEMA_QUERY_TEMPLATE = """
    CREATE TABLE IF NOT EXISTS {table_ref} (
        {schema}
    )
    {with_statement}
"""

INSERT_ROWS_QUERY_TEMPLATE = """
    INSERT INTO {table_ref} ({columns})
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
        trino_schema.update({field.name: trino_type})
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
        for row_name, row_value in row.iteritems():
            if schema[row_name].startswith("timestamp"):
                if isinstance(row_value, datetime):
                    row_value = format_datetime(row_value)
                formated_values.append(f"TIMESTAMP '{row_value}'")
            elif isinstance(row_value, list):
                formated_values.append(f"ARRAY{row_value}")
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
        t = t.astimezone(tz=utc)
    return t.strftime("%Y-%m-%d %H:%M:%S.%f")
