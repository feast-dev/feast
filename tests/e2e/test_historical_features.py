import os
from datetime import datetime, timedelta
from urllib.parse import urlparse

import gcsfs
import numpy as np
import pandas as pd
from google.protobuf.duration_pb2 import Duration
from pandas._testing import assert_frame_equal
from pyarrow import parquet

from feast import Client, Entity, Feature, FeatureTable, FileSource, ValueType
from feast.data_format import ParquetFormat

np.random.seed(0)


def read_parquet(uri):
    parsed_uri = urlparse(uri)
    if parsed_uri.scheme == "file":
        return pd.read_parquet(parsed_uri.path)
    elif parsed_uri.scheme == "gs":
        fs = gcsfs.GCSFileSystem()
        files = ["gs://" + path for path in gcsfs.GCSFileSystem().glob(uri + "/part-*")]
        ds = parquet.ParquetDataset(files, filesystem=fs)
        return ds.read().to_pandas()
    else:
        raise ValueError("Unsupported scheme")


def test_historical_features(feast_client: Client, local_staging_path: str):
    customer_entity = Entity(
        name="user_id", description="Customer", value_type=ValueType.INT64
    )
    feast_client.apply_entity(customer_entity)

    max_age = Duration()
    max_age.FromSeconds(2 * 86400)

    transactions_feature_table = FeatureTable(
        name="transactions",
        entities=["user_id"],
        features=[
            Feature("daily_transactions", ValueType.DOUBLE),
            Feature("total_transactions", ValueType.DOUBLE),
        ],
        batch_source=FileSource(
            event_timestamp_column="event_timestamp",
            created_timestamp_column="created_timestamp",
            file_format=ParquetFormat(),
            file_url=os.path.join(local_staging_path, "transactions"),
        ),
        max_age=max_age,
    )

    feast_client.apply_feature_table(transactions_feature_table)

    retrieval_date = (
        datetime.utcnow()
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .replace(tzinfo=None)
    )
    retrieval_outside_max_age_date = retrieval_date + timedelta(1)
    event_date = retrieval_date - timedelta(2)
    creation_date = retrieval_date - timedelta(1)

    customers = [1001, 1002, 1003, 1004, 1005]
    daily_transactions = [np.random.rand() * 10 for _ in customers]
    total_transactions = [np.random.rand() * 100 for _ in customers]

    transactions_df = pd.DataFrame(
        {
            "event_timestamp": [event_date for _ in customers],
            "created_timestamp": [creation_date for _ in customers],
            "user_id": customers,
            "daily_transactions": daily_transactions,
            "total_transactions": total_transactions,
        }
    )

    feast_client.ingest(transactions_feature_table, transactions_df)

    feature_refs = ["transactions:daily_transactions"]

    customer_df = pd.DataFrame(
        {
            "event_timestamp": [retrieval_date for _ in customers]
            + [retrieval_outside_max_age_date for _ in customers],
            "user_id": customers + customers,
        }
    )

    job = feast_client.get_historical_features(feature_refs, customer_df)
    output_dir = job.get_output_file_uri()
    joined_df = read_parquet(output_dir)

    expected_joined_df = pd.DataFrame(
        {
            "event_timestamp": [retrieval_date for _ in customers]
            + [retrieval_outside_max_age_date for _ in customers],
            "user_id": customers + customers,
            "transactions__daily_transactions": daily_transactions
            + [None] * len(customers),
        }
    )

    assert_frame_equal(
        joined_df.sort_values(by=["user_id", "event_timestamp"]).reset_index(drop=True),
        expected_joined_df.sort_values(by=["user_id", "event_timestamp"]).reset_index(
            drop=True
        ),
    )
