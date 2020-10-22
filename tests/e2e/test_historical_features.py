import os
import tempfile
from datetime import datetime, timedelta
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from google.protobuf.duration_pb2 import Duration
from pandas._testing import assert_frame_equal

from feast import Client, Entity, Feature, FeatureTable, FileSource, ValueType
from feast.data_format import ParquetFormat
from feast.staging.storage_client import get_staging_client

np.random.seed(0)


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
            "event_timestamp",
            "created_timestamp",
            ParquetFormat(),
            os.path.join(local_staging_path, "transactions"),
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

    with tempfile.TemporaryDirectory() as tempdir:
        df_export_path = os.path.join(tempdir, "customers.parquets")
        customer_df.to_parquet(df_export_path)
        scheme, _, remote_path, _, _, _ = urlparse(local_staging_path)
        staging_client = get_staging_client(scheme)
        staging_client.upload_file(df_export_path, None, remote_path)
        customer_source = FileSource(
            "event_timestamp",
            "event_timestamp",
            ParquetFormat(),
            os.path.join(local_staging_path, os.path.basename(df_export_path)),
        )

        job = feast_client.get_historical_features(feature_refs, customer_source)
        output_dir = job.get_output_file_uri()

        _, _, joined_df_destination_path, _, _, _ = urlparse(output_dir)
        joined_df = pd.read_parquet(joined_df_destination_path)

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
            joined_df.sort_values(by=["user_id", "event_timestamp"]).reset_index(
                drop=True
            ),
            expected_joined_df.sort_values(
                by=["user_id", "event_timestamp"]
            ).reset_index(drop=True),
        )
