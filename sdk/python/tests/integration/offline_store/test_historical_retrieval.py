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
from pytz import utc

import feast.driver_test_data as driver_data
from feast import BigQuerySource, FeatureService, FileSource, RepoConfig, utils
from feast.entity import Entity
from feast.errors import FeatureNameCollisionError
from feast.feature import Feature
from feast.feature_store import FeatureStore, _validate_feature_refs
from feast.feature_view import FeatureView
from feast.infra.offline_stores.bigquery import (
    BigQueryOfflineStoreConfig,
    _write_df_to_bq,
)
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.value_type import ValueType

np.random.seed(0)

PROJECT_NAME = "default"


def generate_entities(date, infer_event_timestamp_col, order_count: int = 1000):
    end_date = date
    before_start_date = end_date - timedelta(days=365)
    start_date = end_date - timedelta(days=7)
    after_end_date = end_date + timedelta(days=365)
    customer_entities = list(range(1001, 1110))
    driver_entities = list(range(5001, 5110))
    orders_df = driver_data.create_orders_df(
        customers=customer_entities,
        drivers=driver_entities,
        start_date=before_start_date,
        end_date=after_end_date,
        order_count=order_count,
        infer_event_timestamp_col=infer_event_timestamp_col,
    )
    return customer_entities, driver_entities, end_date, orders_df, start_date


def stage_driver_hourly_stats_parquet_source(directory, df):
    # Write to disk
    driver_stats_path = os.path.join(directory, "driver_stats.parquet")
    df.to_parquet(path=driver_stats_path, allow_truncated_timestamps=True)
    return FileSource(
        path=driver_stats_path,
        event_timestamp_column="event_timestamp",
        created_timestamp_column="",
    )


def stage_driver_hourly_stats_bigquery_source(df, table_id):
    client = bigquery.Client()
    df.reset_index(drop=True, inplace=True)
    job = _write_df_to_bq(client, df, table_id)
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
        batch_source=source,
        ttl=timedelta(hours=2),
    )
    return driver_stats_feature_view


def stage_customer_daily_profile_parquet_source(directory, df):
    customer_profile_path = os.path.join(directory, "customer_profile.parquet")
    df.to_parquet(path=customer_profile_path, allow_truncated_timestamps=True)
    return FileSource(
        path=customer_profile_path,
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )


def feature_service(name: str, views) -> FeatureService:
    return FeatureService(name, views)


def stage_customer_daily_profile_bigquery_source(df, table_id):
    client = bigquery.Client()
    df.reset_index(drop=True, inplace=True)
    job = _write_df_to_bq(client, df, table_id)
    job.result()


def create_customer_daily_profile_feature_view(source):
    customer_profile_feature_view = FeatureView(
        name="customer_profile",
        entities=["customer_id"],
        features=[
            Feature(name="current_balance", dtype=ValueType.FLOAT),
            Feature(name="avg_passenger_count", dtype=ValueType.FLOAT),
            Feature(name="lifetime_trip_count", dtype=ValueType.INT32),
            Feature(name="avg_daily_trips", dtype=ValueType.INT32),
        ],
        batch_source=source,
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
    full_feature_names: bool = False,
):
    # Convert all pandas dataframes into records with UTC timestamps
    order_records = convert_timestamp_records_to_utc(
        orders_df.to_dict("records"), event_timestamp
    )
    driver_records = convert_timestamp_records_to_utc(
        driver_df.to_dict("records"), driver_fv.batch_source.event_timestamp_column
    )
    customer_records = convert_timestamp_records_to_utc(
        customer_df.to_dict("records"), customer_fv.batch_source.event_timestamp_column
    )

    # Manually do point-in-time join of orders to drivers and customers records
    for order_record in order_records:
        driver_record = find_asof_record(
            driver_records,
            ts_key=driver_fv.batch_source.event_timestamp_column,
            ts_start=order_record[event_timestamp] - driver_fv.ttl,
            ts_end=order_record[event_timestamp],
            filter_key="driver_id",
            filter_value=order_record["driver_id"],
        )
        customer_record = find_asof_record(
            customer_records,
            ts_key=customer_fv.batch_source.event_timestamp_column,
            ts_start=order_record[event_timestamp] - customer_fv.ttl,
            ts_end=order_record[event_timestamp],
            filter_key="customer_id",
            filter_value=order_record["customer_id"],
        )

        order_record.update(
            {
                (f"driver_stats__{k}" if full_feature_names else k): driver_record.get(
                    k, None
                )
                for k in ("conv_rate", "avg_daily_trips")
            }
        )

        order_record.update(
            {
                (
                    f"customer_profile__{k}" if full_feature_names else k
                ): customer_record.get(k, None)
                for k in (
                    "current_balance",
                    "avg_passenger_count",
                    "lifetime_trip_count",
                )
            }
        )

    # Convert records back to pandas dataframe
    expected_df = pd.DataFrame(order_records)

    # Move "event_timestamp" column to front
    current_cols = expected_df.columns.tolist()
    current_cols.remove(event_timestamp)
    expected_df = expected_df[[event_timestamp] + current_cols]

    # Cast some columns to expected types, since we lose information when converting pandas DFs into Python objects.
    if full_feature_names:
        expected_column_types = {
            "order_is_success": "int32",
            "driver_stats__conv_rate": "float32",
            "customer_profile__current_balance": "float32",
            "customer_profile__avg_passenger_count": "float32",
        }
    else:
        expected_column_types = {
            "order_is_success": "int32",
            "conv_rate": "float32",
            "current_balance": "float32",
            "avg_passenger_count": "float32",
        }

    for col, typ in expected_column_types.items():
        expected_df[col] = expected_df[col].astype(typ)

    return expected_df


def stage_orders_bigquery(df, table_id):
    client = bigquery.Client()
    df.reset_index(drop=True, inplace=True)
    job = _write_df_to_bq(client, df, table_id)
    job.result()


class BigQueryDataSet:
    def __init__(self, dataset_name):
        self.name = dataset_name

    def __enter__(self):
        client = bigquery.Client()
        dataset = bigquery.Dataset(f"{client.project}.{self.name}")
        dataset.location = "US"
        print(f"Creating dataset: {dataset}")
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
@pytest.mark.parametrize(
    "full_feature_names", [False, True],
)
def test_historical_features_from_parquet_sources(
    infer_event_timestamp_col, full_feature_names
):
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

        customer_fs = feature_service(
            "customer_feature_service",
            [
                customer_fv[
                    ["current_balance", "avg_passenger_count", "lifetime_trip_count"]
                ],
                driver_fv[["conv_rate", "avg_daily_trips"]],
            ],
        )
        print(f"Customer fs features: {customer_fs.features}")

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

        store.apply([driver, customer, driver_fv, customer_fv, customer_fs])

        job = store.get_historical_features(
            entity_df=orders_df,
            features=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
            full_feature_names=full_feature_names,
        )

        actual_df = job.to_df()
        event_timestamp = (
            DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
            if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in orders_df.columns
            else "e_ts"
        )
        expected_df = get_expected_training_df(
            customer_df,
            customer_fv,
            driver_df,
            driver_fv,
            orders_df,
            event_timestamp,
            full_feature_names=full_feature_names,
        )

        expected_df.sort_values(
            by=[event_timestamp, "order_id", "driver_id", "customer_id"]
        ).reset_index(drop=True)
        expected_df = expected_df.reindex(sorted(expected_df.columns), axis=1)

        actual_df = actual_df.sort_values(
            by=[event_timestamp, "order_id", "driver_id", "customer_id"]
        ).reset_index(drop=True)
        actual_df = actual_df.reindex(sorted(actual_df.columns), axis=1)

        assert_frame_equal(
            expected_df, actual_df,
        )

        feature_service_job = store.get_historical_features(
            entity_df=orders_df,
            features=customer_fs,
            full_feature_names=full_feature_names,
        )
        feature_service_df = feature_service_job.to_df()
        feature_service_df = feature_service_df.sort_values(
            by=[event_timestamp, "order_id", "driver_id", "customer_id"]
        ).reset_index(drop=True)
        feature_service_df = feature_service_df.reindex(
            sorted(feature_service_df.columns), axis=1
        )

        assert_frame_equal(expected_df, feature_service_df)
        store.teardown()


def test_feature_name_collision_on_historical_retrieval():

    # _validate_feature_refs is the function that checks for colliding feature names
    # check when feature names collide and 'full_feature_names=False'
    with pytest.raises(FeatureNameCollisionError) as error:
        _validate_feature_refs(
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
                "customer_profile:avg_daily_trips",
            ],
            full_feature_names=False,
        )

    expected_error_message = (
        "Duplicate features named avg_daily_trips found.\n"
        "To resolve this collision, either use the full feature name by setting "
        "'full_feature_names=True', or ensure that the features in question have different names."
    )

    assert str(error.value) == expected_error_message

    # check when feature names collide and 'full_feature_names=True'
    with pytest.raises(FeatureNameCollisionError) as error:
        _validate_feature_refs(
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
                "customer_profile:avg_daily_trips",
            ],
            full_feature_names=True,
        )

    expected_error_message = (
        "Duplicate features named driver_stats__avg_daily_trips found.\n"
        "To resolve this collision, please ensure that the features in question "
        "have different names."
    )
    assert str(error.value) == expected_error_message


@pytest.mark.integration
def test_historical_features_from_bigquery_sources_containing_backfills(capsys):
    now = datetime.now().replace(microsecond=0, second=0, minute=0)
    tomorrow = now + timedelta(days=1)

    entity_dataframe = pd.DataFrame(
        data=[
            {"driver_id": 1001, "event_timestamp": now + timedelta(days=2)},
            {"driver_id": 1002, "event_timestamp": now + timedelta(days=2)},
        ]
    )

    driver_stats_df = pd.DataFrame(
        data=[
            # Duplicated rows simple case
            {
                "driver_id": 1001,
                "avg_daily_trips": 10,
                "event_timestamp": now,
                "created": tomorrow,
            },
            {
                "driver_id": 1001,
                "avg_daily_trips": 20,
                "event_timestamp": tomorrow,
                "created": tomorrow,
            },
            # Duplicated rows after a backfill
            {
                "driver_id": 1002,
                "avg_daily_trips": 30,
                "event_timestamp": now,
                "created": tomorrow,
            },
            {
                "driver_id": 1002,
                "avg_daily_trips": 40,
                "event_timestamp": tomorrow,
                "created": now,
            },
        ]
    )

    expected_df = pd.DataFrame(
        data=[
            {
                "driver_id": 1001,
                "event_timestamp": now + timedelta(days=2),
                "avg_daily_trips": 20,
            },
            {
                "driver_id": 1002,
                "event_timestamp": now + timedelta(days=2),
                "avg_daily_trips": 40,
            },
        ]
    )

    bigquery_dataset = (
        f"test_hist_retrieval_{int(time.time_ns())}_{random.randint(1000, 9999)}"
    )

    with BigQueryDataSet(bigquery_dataset), TemporaryDirectory() as temp_dir:
        gcp_project = bigquery.Client().project

        # Entity Dataframe SQL query
        table_id = f"{bigquery_dataset}.orders"
        stage_orders_bigquery(entity_dataframe, table_id)
        entity_df_query = f"SELECT * FROM {gcp_project}.{table_id}"

        # Driver Feature View
        driver_table_id = f"{gcp_project}.{bigquery_dataset}.driver_hourly"
        stage_driver_hourly_stats_bigquery_source(driver_stats_df, driver_table_id)

        store = FeatureStore(
            config=RepoConfig(
                registry=os.path.join(temp_dir, "registry.db"),
                project="".join(
                    random.choices(string.ascii_uppercase + string.digits, k=10)
                ),
                provider="gcp",
                offline_store=BigQueryOfflineStoreConfig(
                    type="bigquery", dataset=bigquery_dataset
                ),
            )
        )

        driver = Entity(name="driver", join_key="driver_id", value_type=ValueType.INT64)
        driver_fv = FeatureView(
            name="driver_stats",
            entities=["driver"],
            features=[Feature(name="avg_daily_trips", dtype=ValueType.INT32)],
            batch_source=BigQuerySource(
                table_ref=driver_table_id,
                event_timestamp_column="event_timestamp",
                created_timestamp_column="created",
            ),
            ttl=None,
        )

        store.apply([driver, driver_fv])

        try:
            job_from_sql = store.get_historical_features(
                entity_df=entity_df_query,
                features=["driver_stats:avg_daily_trips"],
                full_feature_names=False,
            )

            start_time = datetime.utcnow()
            actual_df_from_sql_entities = job_from_sql.to_df()
            end_time = datetime.utcnow()
            with capsys.disabled():
                print(
                    str(
                        f"\nTime to execute job_from_sql.to_df() = '{(end_time - start_time)}'"
                    )
                )

            assert sorted(expected_df.columns) == sorted(
                actual_df_from_sql_entities.columns
            )
            assert_frame_equal(
                expected_df.sort_values(by=["driver_id"]).reset_index(drop=True),
                actual_df_from_sql_entities[expected_df.columns]
                .sort_values(by=["driver_id"])
                .reset_index(drop=True),
                check_dtype=False,
            )

        finally:
            store.teardown()
