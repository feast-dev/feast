import contextlib
import random
import tempfile
import time

from google.cloud import bigquery

from feast import BigQuerySource, FileSource
from feast.data_format import ParquetFormat
from feast.infra.offline_stores.bigquery import _write_df_to_bq


@contextlib.contextmanager
def prep_file_source(df, event_timestamp_column=None) -> FileSource:
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        f.close()
        df.to_parquet(f.name)
        file_source = FileSource(
            file_format=ParquetFormat(),
            path=f.name,
            event_timestamp_column=event_timestamp_column,
        )
        yield file_source


def simple_bq_source_using_table_ref_arg(
    df, event_timestamp_column=None
) -> BigQuerySource:
    client = bigquery.Client()
    gcp_project = client.project
    bigquery_dataset = f"ds_{time.time_ns()}"
    dataset = bigquery.Dataset(f"{gcp_project}.{bigquery_dataset}")
    client.create_dataset(dataset, exists_ok=True)
    dataset.default_table_expiration_ms = (
        1000
        * 60
        * 60  # 60 minutes in milliseconds (seems to be minimum limit for gcloud)
    )
    client.update_dataset(dataset, ["default_table_expiration_ms"])
    table_ref = f"{gcp_project}.{bigquery_dataset}.table_{random.randrange(100, 999)}"

    job = _write_df_to_bq(client, df, table_ref)
    job.result()

    return BigQuerySource(
        table_ref=table_ref, event_timestamp_column=event_timestamp_column,
    )


def simple_bq_source_using_query_arg(df, event_timestamp_column=None) -> BigQuerySource:
    bq_source_using_table_ref = simple_bq_source_using_table_ref_arg(
        df, event_timestamp_column
    )
    return BigQuerySource(
        query=f"SELECT * FROM {bq_source_using_table_ref.table_ref}",
        event_timestamp_column=event_timestamp_column,
    )
