import os
from datetime import datetime

import pytest

from feast import BigQuerySource, FileSource
from feast.data_format import ParquetFormat


@pytest.fixture
def batch_source(local_staging_path: str, pytestconfig):
    if pytestconfig.getoption("env") == "gcloud":
        bq_project = pytestconfig.getoption("bq_project")
        bq_dataset = pytestconfig.getoption("bq_dataset")
        return BigQuerySource(
            "event_timestamp",
            "created_timestamp",
            f"{bq_project}:{bq_dataset}.source_{datetime.now():%Y%m%d%H%M%s}",
        )
    else:
        return FileSource(
            "event_timestamp",
            "created_timestamp",
            ParquetFormat(),
            os.path.join(local_staging_path, "transactions"),
        )
