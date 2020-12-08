import glob
import os
import tempfile
import time
from math import ceil
from typing import Dict, List, Tuple, Union

import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq

from feast.config import Config
from feast.staging.storage_client import get_staging_client


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
        feature_table_timestamp_column: Timestamp column of FeatureTable
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


def _write_non_partitioned_table_from_source(
    column_names: List[str], table: pa.Table, chunk_size: int, max_workers: int
) -> Tuple[str, str]:
    """
    Partitions dataset by date based on timestamp_column.
    Assumes date_partition_column is in date format if provided.

    Args:
        column_names: Column names in provided ingestion source
        table: PyArrow table of Dataset
        chunk_size: Number of worker processes to use to encode values.
        max_workers: Amount of rows to load and ingest at a time.
    Returns:
        Tuple[str,str]:
            Tuple containing parent directory path, destination path to
            parquet file.
    """
    dir_path = tempfile.mkdtemp()

    # Write table as parquet file with a specified row_group_size
    tmp_table_name = f"{int(time.time())}.parquet"
    dest_path = f"{dir_path}/{tmp_table_name}"
    row_group_size = min(ceil(table.num_rows / max_workers), chunk_size)
    pq.write_table(table=table, where=dest_path, row_group_size=row_group_size)

    # Remove table from memory
    del table

    return dir_path, dest_path


def _write_partitioned_table_from_source(
    column_names: List[str],
    table: pa.Table,
    feature_table_date_partition_column: str,
    feature_table_timestamp_column: str,
) -> str:
    """
    Partitions dataset by date based on timestamp_column.
    Assumes date_partition_column is in date format if provided.

    Args:
        column_names: Column names in provided ingestion source
        table: PyArrow table of Dataset
        feature_table_date_partition_column: Date-partition column of FeatureTable
        feature_table_timestamp_column: Timestamp column of FeatureTable
    Returns:
        str:
            Root directory which contains date partitioned files.
    """
    dir_path = tempfile.mkdtemp()

    # Case: date_partition_column is provided and dataset does not contain it
    if feature_table_date_partition_column not in column_names:
        df = table.to_pandas()
        df[feature_table_date_partition_column] = df[
            feature_table_timestamp_column
        ].dt.date
        table = pa.Table.from_pandas(df)

    pq.write_to_dataset(
        table=table,
        root_path=dir_path,
        partition_cols=[feature_table_date_partition_column],
    )

    # Remove table from memory
    del table

    return dir_path


def _read_table_from_source(
    source: Union[pd.DataFrame, str]
) -> Tuple[pa.Table, List[str]]:
    """
    Infers a data source type (path or Pandas DataFrame) and reads it in as
    a PyArrow Table.

    Args:
        source (Union[pd.DataFrame, str]):
            Either a string path or Pandas DataFrame.

    Returns:
        Tuple[pa.Table, List[str]]:
            Tuple containing PyArrow table of dataset, and column names of PyArrow table.
    """

    # Pandas DataFrame detected
    if isinstance(source, pd.DataFrame):
        table = pa.Table.from_pandas(df=source)

    # Inferring a string path
    elif isinstance(source, str):
        file_path = source
        filename, file_ext = os.path.splitext(file_path)

        if ".csv" in file_ext:
            from pyarrow import csv

            table = csv.read_csv(filename)
        elif ".json" in file_ext:
            from pyarrow import json

            table = json.read_json(filename)
        else:
            table = pq.read_table(file_path)
    else:
        raise ValueError(f"Unknown data source provided for ingestion: {source}")

    # Ensure that PyArrow table is initialised
    assert isinstance(table, pa.lib.Table)

    column_names = table.column_names

    return table, column_names


def _upload_to_file_source(
    file_url: str, with_partitions: bool, dest_path: str, config: Config
) -> None:
    """
    Uploads data into a FileSource. Currently supports GCS, S3 and Local FS.

    Args:
        file_url: file url of FileSource defined for FeatureTable
        with_partitions: whether to treat dest_path as dir with partitioned table
        dest_path: path to file or dir to be uploaded
        config: Config instance to configure FileSource
    """
    from urllib.parse import urlparse

    uri = urlparse(file_url)
    staging_client = get_staging_client(uri.scheme, config)

    if with_partitions:
        for path in glob.glob(os.path.join(dest_path, "**/*")):
            file_name = path.split("/")[-1]
            partition_col = path.split("/")[-2]
            with open(path, "rb") as f:
                staging_client.upload_fileobj(
                    f,
                    path,
                    remote_uri=uri._replace(
                        path=str(uri.path).rstrip("/")
                        + "/"
                        + partition_col
                        + "/"
                        + file_name
                    ),
                )
    else:
        file_name = dest_path.split("/")[-1]
        with open(dest_path, "rb") as f:
            staging_client.upload_fileobj(
                f,
                dest_path,
                remote_uri=uri._replace(
                    path=str(uri.path).rstrip("/") + "/" + file_name
                ),
            )


def _upload_to_bq_source(
    bq_table_ref: str, feature_table_timestamp_column: str, dest_path: str
) -> None:
    """
    Uploads data into a BigQuerySource.

    Args:
        bq_table_ref: BigQuery table reference of format "project:dataset_name.table_name" defined for FeatureTable
        feature_table_timestamp_column: Timestamp column of FeatureTable
        dest_path: File path to existing parquet file
    """
    from google.cloud import bigquery

    gcp_project, _ = bq_table_ref.split(":")

    bq_client = bigquery.Client(project=gcp_project)

    bq_table_ref = bq_table_ref.replace(":", ".")
    table = bigquery.table.Table(bq_table_ref)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET

    time_partitioning_obj = bigquery.table.TimePartitioning(
        field=feature_table_timestamp_column
    )
    job_config.time_partitioning = time_partitioning_obj
    with open(dest_path, "rb") as source_file:
        bq_client.load_table_from_file(
            source_file, table, job_config=job_config
        ).result()
