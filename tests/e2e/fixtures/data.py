import os
import time
from datetime import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from google.cloud import bigquery

from feast import BigQuerySource, FileSource
from feast.data_format import ParquetFormat

__all__ = ("bq_dataset", "batch_source")


@pytest.fixture(scope="session")
def bq_dataset(pytestconfig):
    client = bigquery.Client(project=pytestconfig.getoption("bq_project"))
    timestamp = int(time.time())
    name = f"feast_e2e_{timestamp}"
    client.create_dataset(name)
    yield name
    client.delete_dataset(name, delete_contents=True)


@pytest.fixture
def batch_source(local_staging_path: str, pytestconfig, request: FixtureRequest):
    if pytestconfig.getoption("env") == "gcloud":
        bq_project = pytestconfig.getoption("bq_project")
        bq_dataset = request.getfixturevalue("bq_dataset")
        return BigQuerySource(
            event_timestamp_column="event_timestamp",
            created_timestamp_column="created_timestamp",
            table_ref=f"{bq_project}:{bq_dataset}.source_{datetime.now():%Y%m%d%H%M%s}",
        )
    else:
        return FileSource(
            event_timestamp_column="event_timestamp",
            created_timestamp_column="created_timestamp",
            file_format=ParquetFormat(),
            file_url=os.path.join(local_staging_path, "transactions"),
        )
