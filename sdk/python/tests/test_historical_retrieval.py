import os
import random
import string
import time
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import pytest
from google.cloud import bigquery
from pandas.testing import assert_frame_equal

import feast.driver_test_data as driver_data
from feast.data_source import BigQuerySource, FileSource
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView
from feast.infra.provider import ENTITY_DF_EVENT_TIMESTAMP_COL
from feast.repo_config import RepoConfig, SqliteOnlineStoreConfig
from feast.value_type import ValueType

np.random.seed(0)

PROJECT_NAME = "default"


def generate_entities(date):
    end_date = date
    before_start_date = end_date - timedelta(days=14)
    start_date = end_date - timedelta(days=7)
    after_end_date = end_date + timedelta(days=7)
    customer_entities = [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]
    driver_entities = [5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010]
    orders_df = driver_data.create_orders_df(
        customer_entities, driver_entities, before_start_date, after_end_date, 20
    )
    return customer_entities, driver_entities, end_date, orders_df, start_date


def stage_driver_hourly_stats_parquet_source(directory, df):
    # Write to disk
    driver_stats_path = os.path.join(directory, "driver_stats.parquet")
    df.to_parquet(path=driver_stats_path, allow_truncated_timestamps=True)
    return FileSource(path=driver_stats_path, event_timestamp_column="datetime")


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
    return FileSource(path=customer_profile_path, event_timestamp_column="datetime")


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


def get_expected_training_df(
    customer_df: pd.DataFrame,
    customer_fv: FeatureView,
    driver_df: pd.DataFrame,
    driver_fv: FeatureView,
    orders_df: pd.DataFrame,
):
    expected_orders_df = orders_df.copy().sort_values(ENTITY_DF_EVENT_TIMESTAMP_COL)
    expected_drivers_df = driver_df.copy().sort_values(
        driver_fv.input.event_timestamp_column
    )
    expected_orders_with_drivers = pd.merge_asof(
        expected_orders_df,
        expected_drivers_df[
            [
                driver_fv.input.event_timestamp_column,
                "driver_id",
                "conv_rate",
                "avg_daily_trips",
            ]
        ],
        left_on=ENTITY_DF_EVENT_TIMESTAMP_COL,
        right_on=driver_fv.input.event_timestamp_column,
        by=["driver_id"],
        tolerance=driver_fv.ttl,
    )

    expected_orders_with_drivers.drop(
        columns=[driver_fv.input.event_timestamp_column], inplace=True
    )

    expected_customers_df = customer_df.copy().sort_values(
        [customer_fv.input.event_timestamp_column]
    )
    expected_df = pd.merge_asof(
        expected_orders_with_drivers,
        expected_customers_df[
            [
                customer_fv.input.event_timestamp_column,
                "customer_id",
                "current_balance",
                "avg_passenger_count",
                "lifetime_trip_count",
            ]
        ],
        left_on=ENTITY_DF_EVENT_TIMESTAMP_COL,
        right_on=customer_fv.input.event_timestamp_column,
        by=["customer_id"],
        tolerance=customer_fv.ttl,
    )
    expected_df.drop(columns=[driver_fv.input.event_timestamp_column], inplace=True)

    # Move "datetime" column to front
    current_cols = expected_df.columns.tolist()
    current_cols.remove(ENTITY_DF_EVENT_TIMESTAMP_COL)
    expected_df = expected_df[[ENTITY_DF_EVENT_TIMESTAMP_COL] + current_cols]

    # Rename columns to have double underscore
    expected_df.rename(
        inplace=True,
        columns={
            "conv_rate": "driver_stats__conv_rate",
            "avg_daily_trips": "driver_stats__avg_daily_trips",
            "current_balance": "customer_profile__current_balance",
            "avg_passenger_count": "customer_profile__avg_passenger_count",
            "lifetime_trip_count": "customer_profile__lifetime_trip_count",
        },
    )
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


def test_historical_features_from_parquet_sources():
    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (
        customer_entities,
        driver_entities,
        end_date,
        orders_df,
        start_date,
    ) = generate_entities(start_date)

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
        expected_df = get_expected_training_df(
            customer_df, customer_fv, driver_df, driver_fv, orders_df,
        )
        assert_frame_equal(
            expected_df.sort_values(
                by=[
                    ENTITY_DF_EVENT_TIMESTAMP_COL,
                    "order_id",
                    "driver_id",
                    "customer_id",
                ]
            ).reset_index(drop=True),
            actual_df.sort_values(
                by=[
                    ENTITY_DF_EVENT_TIMESTAMP_COL,
                    "order_id",
                    "driver_id",
                    "customer_id",
                ]
            ).reset_index(drop=True),
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "provider_type", ["local", "gcp"],
)
def test_historical_features_from_bigquery_sources(provider_type):
    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (
        customer_entities,
        driver_entities,
        end_date,
        orders_df,
        start_date,
    ) = generate_entities(start_date)

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
            created_timestamp_column="created",
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
                )
            )
        else:
            raise Exception("Invalid provider used as part of test configuration")

        store.apply([driver, customer, driver_fv, customer_fv])

        expected_df = get_expected_training_df(
            customer_df, customer_fv, driver_df, driver_fv, orders_df,
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
                by=[
                    ENTITY_DF_EVENT_TIMESTAMP_COL,
                    "order_id",
                    "driver_id",
                    "customer_id",
                ]
            ).reset_index(drop=True),
            actual_df_from_sql_entities.sort_values(
                by=[
                    ENTITY_DF_EVENT_TIMESTAMP_COL,
                    "order_id",
                    "driver_id",
                    "customer_id",
                ]
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
        actual_df_from_df_entities = job_from_df.to_df()

        assert_frame_equal(
            expected_df.sort_values(
                by=[
                    ENTITY_DF_EVENT_TIMESTAMP_COL,
                    "order_id",
                    "driver_id",
                    "customer_id",
                ]
            ).reset_index(drop=True),
            actual_df_from_df_entities.sort_values(
                by=[
                    ENTITY_DF_EVENT_TIMESTAMP_COL,
                    "order_id",
                    "driver_id",
                    "customer_id",
                ]
            ).reset_index(drop=True),
            check_dtype=False,
        )
