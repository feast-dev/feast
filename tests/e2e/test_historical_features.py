from datetime import datetime, timedelta
from typing import Union
from urllib.parse import urlparse

import gcsfs
import numpy as np
import pandas as pd
from google.protobuf.duration_pb2 import Duration
from pandas._testing import assert_frame_equal
from pyarrow import parquet

from feast import Client, Entity, Feature, FeatureTable, ValueType
from feast.data_source import BigQuerySource, FileSource

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


def generate_data():
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
    customer_df = pd.DataFrame(
        {
            "event_timestamp": [retrieval_date for _ in customers]
            + [retrieval_outside_max_age_date for _ in customers],
            "user_id": customers + customers,
        }
    )
    return transactions_df, customer_df


def test_historical_features(
    feast_client: Client, batch_source: Union[BigQuerySource, FileSource]
):
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
        batch_source=batch_source,
        max_age=max_age,
    )

    feast_client.apply_feature_table(transactions_feature_table)

    transactions_df, customers_df = generate_data()
    feast_client.ingest(transactions_feature_table, transactions_df)

    feature_refs = ["transactions:daily_transactions"]

    job = feast_client.get_historical_features(feature_refs, customers_df)
    output_dir = job.get_output_file_uri()
    joined_df = read_parquet(output_dir)

    expected_joined_df = pd.DataFrame(
        {
            "event_timestamp": customers_df.event_timestamp.tolist(),
            "user_id": customers_df.user_id.tolist(),
            "transactions__daily_transactions": transactions_df.daily_transactions.tolist()
            + [None] * transactions_df.shape[0],
        }
    )

    assert_frame_equal(
        joined_df.sort_values(by=["user_id", "event_timestamp"]).reset_index(drop=True),
        expected_joined_df.sort_values(by=["user_id", "event_timestamp"]).reset_index(
            drop=True
        ),
    )
