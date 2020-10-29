import os
import tempfile
import uuid
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse

import pandas as pd

from feast.data_format import ParquetFormat
from feast.data_source import BigQuerySource, FileSource
from feast.staging.storage_client import get_staging_client

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None


def stage_entities_to_fs(
    entity_source: pd.DataFrame, staging_location: str
) -> FileSource:
    entity_staging_uri = urlparse(os.path.join(staging_location, str(uuid.uuid4())))
    staging_client = get_staging_client(entity_staging_uri.scheme)
    with tempfile.NamedTemporaryFile() as df_export_path:
        entity_source.to_parquet(df_export_path.name)
        bucket = (
            None if entity_staging_uri.scheme == "file" else entity_staging_uri.netloc
        )
        staging_client.upload_file(
            df_export_path.name, bucket, entity_staging_uri.path.lstrip("/")
        )

    return FileSource(
        event_timestamp_column="event_timestamp",
        file_format=ParquetFormat(),
        file_url=entity_staging_uri.geturl(),
    )


def table_reference_from_string(table_ref: str):
    project, dataset_and_table = table_ref.split(":")
    dataset, table_id = dataset_and_table.split(".")
    return bigquery.TableReference(
        bigquery.DatasetReference(project, dataset), table_id
    )


def stage_entities_to_bq(
    entity_source: pd.DataFrame, project: str, dataset: str
) -> BigQuerySource:
    bq_client = bigquery.Client()
    destination = bigquery.TableReference(
        bigquery.DatasetReference(project, dataset),
        f"_entities_{datetime.now():%Y%m%d%H%M%s}",
    )

    load_job: bigquery.LoadJob = bq_client.load_table_from_dataframe(
        entity_source, destination
    )
    load_job.result()  # wait until complete

    dest_table: bigquery.Table = bq_client.get_table(destination)
    dest_table.expires = datetime.now() + timedelta(days=1)
    bq_client.update_table(dest_table, fields=["expires"])

    return BigQuerySource(
        "event_timestamp",
        "created_timestamp",
        f"{destination.project}:{destination.dataset_id}.{destination.table_id}",
    )


JOIN_TEMPLATE = """SELECT
  source.*
FROM
  `{entities.project}.{entities.dataset_id}.{entities.table_id}` entities
JOIN
  `{source.project}.{source.dataset_id}.{source.table_id}` source
ON
  ({entity_key})"""


def replace_table_with_joined_view(
    source: BigQuerySource, entity_source: BigQuerySource, entity_names: List[str]
) -> BigQuerySource:
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
        source.event_timestamp_column,
        source.created_timestamp_column,
        f"{view.project}:{view.dataset_id}.{view.table_id}",
        source.field_mapping,
        source.date_partition_column,
    )
