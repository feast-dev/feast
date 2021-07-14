import contextlib
import tempfile

from google.cloud import bigquery

from feast import BigQuerySource, FileSource
from feast.data_format import ParquetFormat


@contextlib.contextmanager
def prep_file_source(df, event_timestamp_column=None) -> FileSource:
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        f.close()
        df.to_parquet(f.name)
        file_source = FileSource(
            file_format=ParquetFormat(),
            file_url=f.name,
            event_timestamp_column=event_timestamp_column,
        )
        yield file_source


def simple_bq_source_using_table_ref_arg(
    df, event_timestamp_column=None
) -> BigQuerySource:
    client = bigquery.Client()
    gcp_project = client.project
    bigquery_dataset = "ds"
    dataset = bigquery.Dataset(f"{gcp_project}.{bigquery_dataset}")
    client.create_dataset(dataset, exists_ok=True)
    dataset.default_table_expiration_ms = (
        1000
        * 60
        * 60  # 60 minutes in milliseconds (seems to be minimum limit for gcloud)
    )
    client.update_dataset(dataset, ["default_table_expiration_ms"])
    table_ref = f"{gcp_project}.{bigquery_dataset}.table_1"

    job = client.load_table_from_dataframe(
        df, table_ref, job_config=bigquery.LoadJobConfig()
    )
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
