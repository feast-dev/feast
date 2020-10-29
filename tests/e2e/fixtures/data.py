import os
import time
from datetime import datetime

import pytest
from google.cloud import bigquery

from feast import BigQuerySource, FileSource
from feast.data_format import ParquetFormat

__all__ = ("bq_dataset", "batch_source")


@pytest.fixture(scope="session")
def bq_dataset(pytestconfig):
    if pytestconfig.getoption("env") != "gcloud":
        return

    client = bigquery.Client(project=pytestconfig.getoption("bq_project"))
    timestamp = int(time.time())
    name = f"feast-e2e-{timestamp}"
    client.create_dataset(name)
    yield name
    client.delete_dataset(name)


@pytest.fixture
def batch_source(local_staging_path: str, pytestconfig, bq_dataset):
    if pytestconfig.getoption("env") == "gcloud":
        bq_project = pytestconfig.getoption("bq_project")
        bq_dataset = bq_dataset
        return BigQuerySource(
            "event_timestamp",
            "created_timestamp",
            f"{bq_project}:{bq_dataset}.source_{datetime.now():%Y%m%d%H%M%s}",
        )
    else:
        return FileSource(
            event_timestamp_column="event_timestamp",
            created_timestamp_column="created_timestamp",
            file_format=ParquetFormat(),
            file_url=os.path.join(local_staging_path, "transactions"),
        )
