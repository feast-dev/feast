import tempfile
from typing import Dict, List

import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq

GRPC_CONNECTION_TIMEOUT_DEFAULT = 3  # type: int
GRPC_CONNECTION_TIMEOUT_APPLY = 300  # type: int
FEAST_SERVING_URL_ENV_KEY = "FEAST_SERVING_URL"  # type: str
FEAST_CORE_URL_ENV_KEY = "FEAST_CORE_URL"  # type: str
BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS = 300
BATCH_INGESTION_PRODUCTION_TIMEOUT = 120  # type: int


def _check_field_mappings(
    column_names: List[str],
    feature_table_name: str,
    feature_table_timestamp_column: str,
    feature_table_field_mappings: Dict[str, str],
) -> None:
    """
    Checks that all specified field mappings in FeatureTable can be found in
    column names of specified ingestion source.

    Args:
        column_names: Column names in provided ingestion source
        feature_table_name: Name of FeatureTable
        feature_table_field_mappings: Field mappings of FeatureTable
    """

    if feature_table_timestamp_column not in column_names:
        raise ValueError(
            f"Provided data source does not contain timestamp column {feature_table_timestamp_column} in columns {column_names}"
        )

    specified_field_mappings = list()
    for k, v in feature_table_field_mappings.items():
        specified_field_mappings.append(v)

    is_valid = all(col_name in column_names for col_name in specified_field_mappings)

    if not is_valid:
        raise Exception(
            f"Provided data source does not contain all field mappings previously "
            f"defined for FeatureTable, {feature_table_name}."
        )


def _partition_by_date(
    column_names: List[str],
    feature_table_date_partition_column: str,
    feature_table_timestamp_column: str,
    file_path: str,
) -> str:
    """
    Partitions dataset by date based on timestamp_column.
    Assumes date_partition_column is in date format if provided.

    Args:
        column_names: Column names in provided ingestion source
        feature_table: FeatureTable
        file_path: File path to existing parquet file that's not yet partitioned

    Returns:
        str:
            Root directory which contains date partitioned files.
    """
    df = pd.read_parquet(file_path)
    # Date-partitioned dataset temp path
    dir_path = tempfile.mkdtemp()

    # Case: date_partition_column is provided and dataset does not contain it
    if feature_table_date_partition_column not in column_names:
        df[feature_table_date_partition_column] = df[
            feature_table_timestamp_column
        ].dt.date

    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table=table,
        root_path=dir_path,
        partition_cols=[feature_table_date_partition_column],
    )

    return dir_path
