import os
import tempfile
import uuid
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse

import pandas as pd

from feast.config import Config
from feast.data_format import ParquetFormat
from feast.data_source import BigQuerySource, FileSource
from feast.staging.storage_client import get_staging_client

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None


def stage_entities_to_fs(
    entity_source: pd.DataFrame, staging_location: str, config: Config
) -> FileSource:
    """
    Dumps given (entities) dataframe as parquet file and stage it to remote file storage (subdirectory of staging_location)

    :return: FileSource with remote destination path
    """
    entity_staging_uri = urlparse(os.path.join(staging_location, str(uuid.uuid4())))
    staging_client = get_staging_client(entity_staging_uri.scheme, config)
    with tempfile.NamedTemporaryFile() as df_export_path:
        # prevent casting ns -> ms exception inside pyarrow
        entity_source["event_timestamp"] = entity_source["event_timestamp"].dt.floor(
            "ms"
        )

        entity_source.to_parquet(df_export_path.name)

        with open(df_export_path.name, "rb") as f:
            staging_client.upload_fileobj(
                f, df_export_path.name, remote_uri=entity_staging_uri
            )

    # ToDo: support custom event_timestamp_column
    return FileSource(
        event_timestamp_column="event_timestamp",
        file_format=ParquetFormat(),
        file_url=entity_staging_uri.geturl(),
    )


def table_reference_from_string(table_ref: str):
    """
    Parses reference string with format "{project}:{dataset}.{table}" into bigquery.TableReference
    """
    project, dataset_and_table = table_ref.split(":")
    dataset, table_id = dataset_and_table.split(".")
    return bigquery.TableReference(
        bigquery.DatasetReference(project, dataset), table_id
    )


def stage_entities_to_bq(
    entity_source: pd.DataFrame, project: str, dataset: str
) -> BigQuerySource:
    """
    Stores given (entity) dataframe as new table in BQ. Name of the table generated based on current time.
    Table will expire in 1 day.
    Returns BigQuerySource with reference to created table.
    """
    bq_client = bigquery.Client()
    destination = bigquery.TableReference(
        bigquery.DatasetReference(project, dataset),
        f"_entities_{datetime.now():%Y%m%d%H%M%s}",
    )

    # prevent casting ns -> ms exception inside pyarrow
    entity_source["event_timestamp"] = entity_source["event_timestamp"].dt.floor("ms")

    load_job: bigquery.LoadJob = bq_client.load_table_from_dataframe(
        entity_source, destination
    )
    load_job.result()  # wait until complete

    dest_table: bigquery.Table = bq_client.get_table(destination)
    dest_table.expires = datetime.now() + timedelta(days=1)
    bq_client.update_table(dest_table, fields=["expires"])

    return BigQuerySource(
        event_timestamp_column="event_timestamp",
        table_ref=f"{destination.project}:{destination.dataset_id}.{destination.table_id}",
    )


JOIN_TEMPLATE = """SELECT
  source.*
FROM
  `{entities.project}.{entities.dataset_id}.{entities.table_id}` entities
JOIN
  `{source.project}.{source.dataset_id}.{source.table_id}` source
ON
  ({entity_key})"""


def create_bq_view_of_joined_features_and_entities(
    source: BigQuerySource, entity_source: BigQuerySource, entity_names: List[str]
) -> BigQuerySource:
    """
    Creates BQ view that joins tables from `source` and `entity_source` with join key derived from `entity_names`.
    Returns BigQuerySource with reference to created view.
    """
    bq_client = bigquery.Client()

    source_ref = table_reference_from_string(source.bigquery_options.table_ref)
    entities_ref = table_reference_from_string(entity_source.bigquery_options.table_ref)

    destination_ref = bigquery.TableReference(
        bigquery.DatasetReference(source_ref.project, source_ref.dataset_id),
        f"_view_{source_ref.table_id}_{datetime.now():%Y%m%d%H%M%s}",
    )

    view = bigquery.Table(destination_ref)
    view.view_query = JOIN_TEMPLATE.format(
        entities=entities_ref,
        source=source_ref,
        entity_key=",".join([f"source.{e} = entities.{e}" for e in entity_names]),
    )
    view.expires = datetime.now() + timedelta(days=1)
    bq_client.create_table(view)

    return BigQuerySource(
        event_timestamp_column=source.event_timestamp_column,
        created_timestamp_column=source.created_timestamp_column,
        table_ref=f"{view.project}:{view.dataset_id}.{view.table_id}",
        field_mapping=source.field_mapping,
        date_partition_column=source.date_partition_column,
    )
