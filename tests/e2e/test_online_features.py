import os
import time
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyspark
import pytest

from feast import Client, Entity, Feature, FeatureTable, FileSource, ValueType
from feast.pyspark.abc import SparkJobStatus
from feast.wait import wait_retry_backoff


def generate_data():
    df = pd.DataFrame(columns=["s2_id", "unique_drivers", "event_timestamp"])
    df["s2id"] = np.random.randint(1000, 9999, 100)
    df["unique_drivers"] = np.random.randint(0, 1000, 100)
    df["event_timestamp"] = pd.to_datetime(
        np.random.randint(int(time.time()), int(time.time()) + 3600, 100), unit="s"
    )
    df["date"] = df["event_timestamp"].dt.date

    return df


@pytest.fixture(scope="session")
def feast_client(pytestconfig):
    if pytestconfig.getoption("env") == "local":
        return Client(
            core_url="localhost:6565",
            serving_url="localhost:6566",
            spark_launcher="standalone",
            spark_standalone_master="local",
            spark_home=os.path.dirname(pyspark.__file__),
        )

    if pytestconfig.getoption("env") == "gcloud":
        return Client(
            core_url=pytestconfig.getoption("core_url"),
            serving_url=pytestconfig.getoption("serving_url"),
            spark_launcher="dataproc",
            dataproc_cluster_name=pytestconfig.getoption("dataproc_cluster_name"),
            dataproc_project=pytestconfig.getoption("dataproc_project"),
            dataproc_region=pytestconfig.getoption("dataproc_region"),
            dataproc_staging_location=os.path.join(
                pytestconfig.getoption("staging_path"), "dataproc"
            ),
        )


@pytest.fixture(scope="function")
def staging_path(pytestconfig, tmp_path):
    if pytestconfig.getoption("env") == "local":
        return f"file://{tmp_path}"

    staging_path = pytestconfig.getoption("staging_path")
    return os.path.join(staging_path, str(uuid.uuid4()))


@pytest.mark.skip
def test_offline_ingestion(feast_client: Client, staging_path: str):
    entity = Entity(name="s2id", description="S2id", value_type=ValueType.INT64,)

    feature_table = FeatureTable(
        name="drivers",
        entities=["s2id"],
        features=[Feature("unique_drivers", ValueType.INT64)],
        batch_source=FileSource(
            "event_timestamp",
            "event_timestamp",
            "parquet",
            os.path.join(staging_path, "batch-storage"),
        ),
    )

    feast_client.apply_entity(entity)
    feast_client.apply_feature_table(feature_table)

    df = generate_data()
    feast_client.ingest(feature_table, df)  # write to batch (offline) storage

    job = feast_client.start_offline_to_online_ingestion(
        feature_table, datetime.today(), datetime.today() + timedelta(days=1)
    )

    status = wait_retry_backoff(
        lambda: (job.get_status(), job.get_status() != SparkJobStatus.IN_PROGRESS), 300
    )

    assert status == SparkJobStatus.COMPLETED

    feast_client.get_online_features(
        ["drivers:unique_drivers"],
        entity_rows=[{"s2id": s2_id} for s2_id in df["s2id"].tolist()],
    )
