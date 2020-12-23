import tempfile
import uuid
from datetime import datetime
from os import path
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import pytest
from google.cloud import storage
from pytz import utc

from feast.pyspark.abc import RetrievalJobParameters


@pytest.fixture(scope="module")
def customer_entity() -> pd.DataFrame:
    return pd.DataFrame(
        np.array([[1001, datetime(year=2020, month=9, day=1, tzinfo=utc)]]),
        columns=["customer", "event_timestamp"],
    )


@pytest.fixture(scope="module")
def customer_feature() -> pd.DataFrame:
    return pd.DataFrame(
        np.array(
            [
                [
                    1001,
                    100.0,
                    datetime(year=2020, month=9, day=1, tzinfo=utc),
                    datetime(year=2020, month=9, day=1, tzinfo=utc),
                ],
            ]
        ),
        columns=[
            "customer",
            "total_transactions",
            "event_timestamp",
            "created_timestamp",
        ],
    )


def upload_dataframe_to_gcs_as_parquet(df: pd.DataFrame, staging_location: str):
    gcs_client = storage.Client()
    staging_location_uri = urlparse(staging_location)
    staging_bucket = staging_location_uri.netloc
    remote_path = staging_location_uri.path.lstrip("/")
    gcs_bucket = gcs_client.get_bucket(staging_bucket)
    temp_dir = str(uuid.uuid4())
    df_remote_path = path.join(remote_path, temp_dir)
    blob = gcs_bucket.blob(df_remote_path)
    with tempfile.NamedTemporaryFile() as df_local_path:
        df.to_parquet(df_local_path.name)
        blob.upload_from_filename(df_local_path.name)
    return path.join(staging_location, df_remote_path)


def new_retrieval_job_params(
    entity_source_uri: str,
    feature_source_uri: str,
    destination_uri: str,
    output_format: str,
) -> RetrievalJobParameters:
    entity_source = {
        "file": {
            "format": {"json_class": "ParquetFormat"},
            "path": entity_source_uri,
            "event_timestamp_column": "event_timestamp",
        }
    }

    feature_tables_sources = [
        {
            "file": {
                "format": {"json_class": "ParquetFormat"},
                "path": feature_source_uri,
                "event_timestamp_column": "event_timestamp",
                "created_timestamp_column": "created_timestamp",
            }
        }
    ]

    feature_tables = [
        {
            "name": "customer_transactions",
            "entities": [{"name": "customer", "type": "int64"}],
            "features": [{"name": "total_transactions", "type": "double"}],
        }
    ]

    destination = {"format": output_format, "path": destination_uri}

    return RetrievalJobParameters(
        feature_tables=feature_tables,
        feature_tables_sources=feature_tables_sources,
        entity_source=entity_source,
        destination=destination,
        extra_packages=["com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.3.0"],
    )


@pytest.fixture(scope="module")
def dataproc_retrieval_job_params(
    pytestconfig, customer_entity, customer_feature
) -> RetrievalJobParameters:
    staging_location = pytestconfig.getoption("--dataproc-staging-location")
    entity_source_uri = upload_dataframe_to_gcs_as_parquet(
        customer_entity, staging_location
    )
    feature_source_uri = upload_dataframe_to_gcs_as_parquet(
        customer_feature, staging_location
    )
    destination_uri = path.join(staging_location, str(uuid.uuid4()))

    return new_retrieval_job_params(
        entity_source_uri, feature_source_uri, destination_uri, "parquet"
    )


@pytest.fixture(scope="module")
def dataproc_retrieval_job_params_with_tfrecord_output(
    pytestconfig, customer_entity, customer_feature
) -> RetrievalJobParameters:
    staging_location = pytestconfig.getoption("--dataproc-staging-location")
    entity_source_uri = upload_dataframe_to_gcs_as_parquet(
        customer_entity, staging_location
    )
    feature_source_uri = upload_dataframe_to_gcs_as_parquet(
        customer_feature, staging_location
    )
    destination_uri = path.join(staging_location, str(uuid.uuid4()))

    return new_retrieval_job_params(
        entity_source_uri, feature_source_uri, destination_uri, "tfrecord"
    )
