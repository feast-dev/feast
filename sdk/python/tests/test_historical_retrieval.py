import os
import random
import string
import time
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

import assertpy
import numpy as np
import pandas as pd
import pytest
from google.cloud import bigquery
from pandas.testing import assert_frame_equal
from pytz import utc

import feast.driver_test_data as driver_data
from feast import utils
from feast.data_source import BigQuerySource, FileSource
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView
from feast.infra.provider import DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
from feast.repo_config import (
    BigQueryOfflineStoreConfig,
    RepoConfig,
    SqliteOnlineStoreConfig,
)
from feast.value_type import ValueType

np.random.seed(0)

PROJECT_NAME = "default"


def generate_entities(date, infer_event_timestamp_col):
    end_date = date
    before_start_date = end_date - timedelta(days=14)
    start_date = end_date - timedelta(days=7)
    after_end_date = end_date + timedelta(days=7)
    customer_entities = [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]
    driver_entities = [5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010]
    orders_df = driver_data.create_orders_df(
        customer_entities,
        driver_entities,
        before_start_date,
        after_end_date,
        20,
        infer_event_timestamp_col=infer_event_timestamp_col,
    )
    return customer_entities, driver_entities, end_date, orders_df, start_date


def stage_driver_hourly_stats_parquet_source(directory, df):
    # Write to disk
    driver_stats_path = os.path.join(directory, "driver_stats.parquet")
    df.to_parquet(path=driver_stats_path, allow_truncated_timestamps=True)
    return FileSource(
        path=driver_stats_path,
        event_timestamp_column="datetime",
        created_timestamp_column="",
    )


def stage_driver_hourly_stats_bigquery_source(df, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    df.reset_index(drop=True, inplace=True)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def create_driver_hourly_stats_feature_view(source):
    driver_stats_feature_view = FeatureView(
        name="driver_stats",
        entities=["driver"],
        features=[
            Feature(name="conv_rate", dtype=ValueType.FLOAT),
            Feature(name="acc_rate", dtype=ValueType.FLOAT),
            Feature(name="avg_daily_trips", dtype=ValueType.INT32),
        ],
        input=source,
        ttl=timedelta(hours=2),
    )
    return driver_stats_feature_view


def stage_customer_daily_profile_parquet_source(directory, df):
    customer_profile_path = os.path.join(directory, "customer_profile.parquet")
    df.to_parquet(path=customer_profile_path, allow_truncated_timestamps=True)
    return FileSource(
        path=customer_profile_path,
        event_timestamp_column="datetime",
        created_timestamp_column="created",
    )


def stage_customer_daily_profile_bigquery_source(df, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    df.reset_index(drop=True, inplace=True)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def create_customer_daily_profile_feature_view(source):
    customer_profile_feature_view = FeatureView(
        name="customer_profile",
        entities=["customer_id"],
        features=[
            Feature(name="current_balance", dtype=ValueType.FLOAT),
            Feature(name="avg_passenger_count", dtype=ValueType.FLOAT),
            Feature(name="lifetime_trip_count", dtype=ValueType.INT32),
        ],
        input=source,
        ttl=timedelta(days=2),
    )
    return customer_profile_feature_view


# Converts the given column of the pandas records to UTC timestamps
def convert_timestamp_records_to_utc(records, column):
    for record in records:
        record[column] = utils.make_tzaware(record[column]).astimezone(utc)
    return records


# Find the latest record in the given time range and filter
def find_asof_record(records, ts_key, ts_start, ts_end, filter_key, filter_value):
    found_record = {}
    for record in records:
        if record[filter_key] == filter_value and ts_start <= record[ts_key] <= ts_end:
            if not found_record or found_record[ts_key] < record[ts_key]:
                found_record = record
    return found_record


def get_expected_training_df(
    customer_df: pd.DataFrame,
    customer_fv: FeatureView,
    driver_df: pd.DataFrame,
    driver_fv: FeatureView,
    orders_df: pd.DataFrame,
    event_timestamp: str,
):
    # Convert all pandas dataframes into records with UTC timestamps
    order_records = convert_timestamp_records_to_utc(
        orders_df.to_dict("records"), event_timestamp
    )
    driver_records = convert_timestamp_records_to_utc(
        driver_df.to_dict("records"), driver_fv.input.event_timestamp_column
    )
    customer_records = convert_timestamp_records_to_utc(
        customer_df.to_dict("records"), customer_fv.input.event_timestamp_column
    )

    # Manually do point-in-time join of orders to drivers and customers records
    for order_record in order_records:
        driver_record = find_asof_record(
            driver_records,
            ts_key=driver_fv.input.event_timestamp_column,
            ts_start=order_record[event_timestamp] - driver_fv.ttl,
            ts_end=order_record[event_timestamp],
            filter_key="driver_id",
            filter_value=order_record["driver_id"],
        )
        customer_record = find_asof_record(
            customer_records,
            ts_key=customer_fv.input.event_timestamp_column,
            ts_start=order_record[event_timestamp] - customer_fv.ttl,
            ts_end=order_record[event_timestamp],
            filter_key="customer_id",
            filter_value=order_record["customer_id"],
        )
        order_record.update(
            {
                f"driver_stats__{k}": driver_record.get(k, None)
                for k in ("conv_rate", "avg_daily_trips")
            }
        )
        order_record.update(
            {
                f"customer_profile__{k}": customer_record.get(k, None)
                for k in (
                    "current_balance",
                    "avg_passenger_count",
                    "lifetime_trip_count",
                )
            }
        )

    # Convert records back to pandas dataframe
    expected_df = pd.DataFrame(order_records)

    # Move "datetime" column to front
    current_cols = expected_df.columns.tolist()
    current_cols.remove(event_timestamp)
    expected_df = expected_df[[event_timestamp] + current_cols]

    # Cast some columns to expected types, since we lose information when converting pandas DFs into Python objects.
    expected_df["order_is_success"] = expected_df["order_is_success"].astype("int32")
    expected_df["customer_profile__current_balance"] = expected_df[
        "customer_profile__current_balance"
    ].astype("float32")
    expected_df["customer_profile__avg_passenger_count"] = expected_df[
        "customer_profile__avg_passenger_count"
    ].astype("float32")

    return expected_df


def stage_orders_bigquery(df, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    df.reset_index(drop=True, inplace=True)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


class BigQueryDataSet:
    def __init__(self, dataset_name):
        self.name = dataset_name

    def __enter__(self):
        client = bigquery.Client()
        dataset = bigquery.Dataset(f"{client.project}.{self.name}")
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)
        return dataset

    def __exit__(self, exc_type, exc_value, exc_traceback):
        print("Tearing down BigQuery dataset")
        client = bigquery.Client()
        dataset_id = f"{client.project}.{self.name}"

        client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
        print(f"Deleted dataset '{dataset_id}'")
        if exc_type:
            print(
                "***Logging exception {}***".format(
                    (exc_type, exc_value, exc_traceback)
                )
            )


@pytest.mark.parametrize(
    "infer_event_timestamp_col", [False, True],
)
def test_historical_features_from_parquet_sources(infer_event_timestamp_col):
    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (
        customer_entities,
        driver_entities,
        end_date,
        orders_df,
        start_date,
    ) = generate_entities(start_date, infer_event_timestamp_col)

    with TemporaryDirectory() as temp_dir:
        driver_df = driver_data.create_driver_hourly_stats_df(
            driver_entities, start_date, end_date
        )
        driver_source = stage_driver_hourly_stats_parquet_source(temp_dir, driver_df)
        driver_fv = create_driver_hourly_stats_feature_view(driver_source)
        customer_df = driver_data.create_customer_daily_profile_df(
            customer_entities, start_date, end_date
        )
        customer_source = stage_customer_daily_profile_parquet_source(
            temp_dir, customer_df
        )
        customer_fv = create_customer_daily_profile_feature_view(customer_source)
        driver = Entity(name="driver", join_key="driver_id", value_type=ValueType.INT64)
        customer = Entity(name="customer_id", value_type=ValueType.INT64)

        store = FeatureStore(
            config=RepoConfig(
                registry=os.path.join(temp_dir, "registry.db"),
                project="default",
                provider="local",
                online_store=SqliteOnlineStoreConfig(
                    path=os.path.join(temp_dir, "online_store.db")
                ),
            )
        )

        store.apply([driver, customer, driver_fv, customer_fv])

        job = store.get_historical_features(
            entity_df=orders_df,
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
        )

        actual_df = job.to_df()
        event_timestamp = (
            DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
            if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in orders_df.columns
            else "e_ts"
        )
        expected_df = get_expected_training_df(
            customer_df, customer_fv, driver_df, driver_fv, orders_df, event_timestamp,
        )
        assert_frame_equal(
            expected_df.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            actual_df.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "provider_type", ["local", "gcp", "gcp_custom_offline_config"],
)
@pytest.mark.parametrize(
    "infer_event_timestamp_col", [False, True],
)
def test_historical_features_from_bigquery_sources(
    provider_type, infer_event_timestamp_col
):
    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (
        customer_entities,
        driver_entities,
        end_date,
        orders_df,
        start_date,
    ) = generate_entities(start_date, infer_event_timestamp_col)

    # bigquery_dataset = "test_hist_retrieval_static"
    bigquery_dataset = f"test_hist_retrieval_{int(time.time())}"

    with BigQueryDataSet(bigquery_dataset), TemporaryDirectory() as temp_dir:
        gcp_project = bigquery.Client().project

        # Orders Query
        table_id = f"{bigquery_dataset}.orders"
        stage_orders_bigquery(orders_df, table_id)
        entity_df_query = f"SELECT * FROM {gcp_project}.{table_id}"

        # Driver Feature View
        driver_df = driver_data.create_driver_hourly_stats_df(
            driver_entities, start_date, end_date
        )
        driver_table_id = f"{gcp_project}.{bigquery_dataset}.driver_hourly"
        stage_driver_hourly_stats_bigquery_source(driver_df, driver_table_id)
        driver_source = BigQuerySource(
            table_ref=driver_table_id,
            event_timestamp_column="datetime",
            created_timestamp_column="created",
        )
        driver_fv = create_driver_hourly_stats_feature_view(driver_source)

        # Customer Feature View
        customer_df = driver_data.create_customer_daily_profile_df(
            customer_entities, start_date, end_date
        )
        customer_table_id = f"{gcp_project}.{bigquery_dataset}.customer_profile"

        stage_customer_daily_profile_bigquery_source(customer_df, customer_table_id)
        customer_source = BigQuerySource(
            table_ref=customer_table_id,
            event_timestamp_column="datetime",
            created_timestamp_column="",
        )
        customer_fv = create_customer_daily_profile_feature_view(customer_source)

        driver = Entity(name="driver", join_key="driver_id", value_type=ValueType.INT64)
        customer = Entity(name="customer_id", value_type=ValueType.INT64)

        if provider_type == "local":
            store = FeatureStore(
                config=RepoConfig(
                    registry=os.path.join(temp_dir, "registry.db"),
                    project="default",
                    provider="local",
                    online_store=SqliteOnlineStoreConfig(
                        path=os.path.join(temp_dir, "online_store.db"),
                    ),
                    offline_store=BigQueryOfflineStoreConfig(type="bigquery",),
                )
            )
        elif provider_type == "gcp":
            store = FeatureStore(
                config=RepoConfig(
                    registry=os.path.join(temp_dir, "registry.db"),
                    project="".join(
                        random.choices(string.ascii_uppercase + string.digits, k=10)
                    ),
                    provider="gcp",
                    offline_store=BigQueryOfflineStoreConfig(type="bigquery",),
                )
            )
        elif provider_type == "gcp_custom_offline_config":
            store = FeatureStore(
                config=RepoConfig(
                    registry=os.path.join(temp_dir, "registry.db"),
                    project="".join(
                        random.choices(string.ascii_uppercase + string.digits, k=10)
                    ),
                    provider="gcp",
                    offline_store=BigQueryOfflineStoreConfig(
                        type="bigquery", dataset="foo"
                    ),
                )
            )
        else:
            raise Exception("Invalid provider used as part of test configuration")

        store.apply([driver, customer, driver_fv, customer_fv])

        event_timestamp = (
            DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
            if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in orders_df.columns
            else "e_ts"
        )
        expected_df = get_expected_training_df(
            customer_df, customer_fv, driver_df, driver_fv, orders_df, event_timestamp,
        )

        job_from_sql = store.get_historical_features(
            entity_df=entity_df_query,
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
        )

        actual_df_from_sql_entities = job_from_sql.to_df()

        assert_frame_equal(
            expected_df.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            actual_df_from_sql_entities.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            check_dtype=False,
        )

        job_from_df = store.get_historical_features(
            entity_df=orders_df,
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
        )

        if provider_type == "gcp_custom_offline_config":
            # Make sure that custom dataset name is being used from the offline_store config
            assertpy.assert_that(job_from_df.query).contains("foo.entity_df")
        else:
            # If the custom dataset name isn't provided in the config, use default `feast` name
            assertpy.assert_that(job_from_df.query).contains("feast.entity_df")

        actual_df_from_df_entities = job_from_df.to_df()

        assert_frame_equal(
            expected_df.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            actual_df_from_df_entities.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            check_dtype=False,
        )
