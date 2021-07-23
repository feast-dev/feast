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
from feast import (
    BigQuerySource,
    FeatureService,
    FileSource,
    RedshiftSource,
    RepoConfig,
    errors,
    utils,
)
from feast.entity import Entity
from feast.errors import FeatureNameCollisionError
from feast.feature import Feature
from feast.feature_store import FeatureStore, _validate_feature_refs
from feast.feature_view import FeatureView
from feast.infra.offline_stores.bigquery import BigQueryOfflineStoreConfig
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)
from feast.infra.offline_stores.redshift import RedshiftOfflineStoreConfig
from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.infra.utils import aws_utils
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


def feature_service(name: str, views) -> FeatureService:
    return FeatureService(name, views)


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
            Feature(name="avg_daily_trips", dtype=ValueType.INT32),
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
    full_feature_names: bool = False,
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

    # Move "datetime" column to front
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


@pytest.mark.integration
@pytest.mark.parametrize(
    "provider_type", ["local", "gcp", "gcp_custom_offline_config"],
)
@pytest.mark.parametrize(
    "infer_event_timestamp_col", [False, True],
)
@pytest.mark.parametrize(
    "full_feature_names", [False, True],
)
def test_historical_features_from_bigquery_sources(
    provider_type, infer_event_timestamp_col, capsys, full_feature_names
):
    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (
        customer_entities,
        driver_entities,
        end_date,
        orders_df,
        start_date,
    ) = generate_entities(start_date, infer_event_timestamp_col)

    bigquery_dataset = (
        f"test_hist_retrieval_{int(time.time_ns())}_{random.randint(1000, 9999)}"
    )

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
                    offline_store=BigQueryOfflineStoreConfig(
                        type="bigquery", dataset=bigquery_dataset
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
                    offline_store=BigQueryOfflineStoreConfig(
                        type="bigquery", dataset=bigquery_dataset
                    ),
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
            customer_df,
            customer_fv,
            driver_df,
            driver_fv,
            orders_df,
            event_timestamp,
            full_feature_names,
        )

        job_from_sql = store.get_historical_features(
            entity_df=entity_df_query,
            features=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
            full_feature_names=full_feature_names,
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
            expected_df.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            actual_df_from_sql_entities[expected_df.columns]
            .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
            .reset_index(drop=True),
            check_dtype=False,
        )

        table_from_sql_entities = job_from_sql.to_arrow()
        assert_frame_equal(
            actual_df_from_sql_entities, table_from_sql_entities.to_pandas()
        )

        timestamp_column = (
            "e_ts"
            if infer_event_timestamp_col
            else DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
        )

        entity_df_query_with_invalid_join_key = (
            f"select order_id, driver_id, customer_id as customer, "
            f"order_is_success, {timestamp_column}, FROM {gcp_project}.{table_id}"
        )
        # Rename the join key; this should now raise an error.
        assertpy.assert_that(store.get_historical_features).raises(
            errors.FeastEntityDFMissingColumnsError
        ).when_called_with(
            entity_df=entity_df_query_with_invalid_join_key,
            features=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
        )

        job_from_df = store.get_historical_features(
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

        # Rename the join key; this should now raise an error.
        orders_df_with_invalid_join_key = orders_df.rename(
            {"customer_id": "customer"}, axis="columns"
        )
        assertpy.assert_that(store.get_historical_features).raises(
            errors.FeastEntityDFMissingColumnsError
        ).when_called_with(
            entity_df=orders_df_with_invalid_join_key,
            features=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
        )

        # Make sure that custom dataset name is being used from the offline_store config
        if provider_type == "gcp_custom_offline_config":
            assertpy.assert_that(job_from_df.query).contains("foo.feast_entity_df")
        else:
            assertpy.assert_that(job_from_df.query).contains(
                f"{bigquery_dataset}.feast_entity_df"
            )

        start_time = datetime.utcnow()
        actual_df_from_df_entities = job_from_df.to_df()
        end_time = datetime.utcnow()
        with capsys.disabled():
            print(
                str(
                    f"Time to execute job_from_df.to_df() = '{(end_time - start_time)}'\n"
                )
            )

        assert sorted(expected_df.columns) == sorted(actual_df_from_df_entities.columns)
        assert_frame_equal(
            expected_df.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            actual_df_from_df_entities[expected_df.columns]
            .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
            .reset_index(drop=True),
            check_dtype=False,
        )

        table_from_df_entities = job_from_df.to_arrow()
        assert_frame_equal(
            actual_df_from_df_entities, table_from_df_entities.to_pandas()
        )

        store.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "provider_type", ["local", "aws"],
)
@pytest.mark.parametrize(
    "infer_event_timestamp_col", [False, True],
)
@pytest.mark.parametrize(
    "full_feature_names", [False, True],
)
def test_historical_features_from_redshift_sources(
    provider_type, infer_event_timestamp_col, capsys, full_feature_names
):
    client = aws_utils.get_redshift_data_client("us-west-2")
    s3 = aws_utils.get_s3_resource("us-west-2")

    offline_store = RedshiftOfflineStoreConfig(
        cluster_id="feast-integration-tests",
        region="us-west-2",
        user="admin",
        database="feast",
        s3_staging_location="s3://feast-integration-tests/redshift/tests/ingestion",
        iam_role="arn:aws:iam::402087665549:role/redshift_s3_access_role",
    )

    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (
        customer_entities,
        driver_entities,
        end_date,
        orders_df,
        start_date,
    ) = generate_entities(start_date, infer_event_timestamp_col)

    redshift_table_prefix = (
        f"test_hist_retrieval_{int(time.time_ns())}_{random.randint(1000, 9999)}"
    )

    # Stage orders_df to Redshift
    table_name = f"{redshift_table_prefix}_orders"
    entity_df_query = f"SELECT * FROM {table_name}"
    orders_context = aws_utils.temporarily_upload_df_to_redshift(
        client,
        offline_store.cluster_id,
        offline_store.database,
        offline_store.user,
        s3,
        f"{offline_store.s3_staging_location}/copy/{table_name}.parquet",
        offline_store.iam_role,
        table_name,
        orders_df,
    )

    # Stage driver_df to Redshift
    driver_df = driver_data.create_driver_hourly_stats_df(
        driver_entities, start_date, end_date
    )
    driver_table_name = f"{redshift_table_prefix}_driver_hourly"
    driver_context = aws_utils.temporarily_upload_df_to_redshift(
        client,
        offline_store.cluster_id,
        offline_store.database,
        offline_store.user,
        s3,
        f"{offline_store.s3_staging_location}/copy/{driver_table_name}.parquet",
        offline_store.iam_role,
        driver_table_name,
        driver_df,
    )

    # Stage customer_df to Redshift
    customer_df = driver_data.create_customer_daily_profile_df(
        customer_entities, start_date, end_date
    )
    customer_table_name = f"{redshift_table_prefix}_customer_profile"
    customer_context = aws_utils.temporarily_upload_df_to_redshift(
        client,
        offline_store.cluster_id,
        offline_store.database,
        offline_store.user,
        s3,
        f"{offline_store.s3_staging_location}/copy/{customer_table_name}.parquet",
        offline_store.iam_role,
        customer_table_name,
        customer_df,
    )

    with orders_context, driver_context, customer_context, TemporaryDirectory() as temp_dir:
        driver_source = RedshiftSource(
            table=driver_table_name,
            event_timestamp_column="datetime",
            created_timestamp_column="created",
        )
        driver_fv = create_driver_hourly_stats_feature_view(driver_source)

        customer_source = RedshiftSource(
            table=customer_table_name,
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
                    offline_store=offline_store,
                )
            )
        elif provider_type == "aws":
            store = FeatureStore(
                config=RepoConfig(
                    registry=os.path.join(temp_dir, "registry.db"),
                    project="".join(
                        random.choices(string.ascii_uppercase + string.digits, k=10)
                    ),
                    provider="aws",
                    online_store=DynamoDBOnlineStoreConfig(region="us-west-2"),
                    offline_store=offline_store,
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
            customer_df,
            customer_fv,
            driver_df,
            driver_fv,
            orders_df,
            event_timestamp,
            full_feature_names,
        )

        job_from_sql = store.get_historical_features(
            entity_df=entity_df_query,
            features=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
            full_feature_names=full_feature_names,
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
            expected_df.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            actual_df_from_sql_entities[expected_df.columns]
            .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
            .reset_index(drop=True),
            check_dtype=False,
        )

        table_from_sql_entities = job_from_sql.to_arrow()
        assert_frame_equal(
            actual_df_from_sql_entities.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            table_from_sql_entities.to_pandas()
            .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
            .reset_index(drop=True),
        )

        timestamp_column = (
            "e_ts"
            if infer_event_timestamp_col
            else DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
        )

        entity_df_query_with_invalid_join_key = (
            f"select order_id, driver_id, customer_id as customer, "
            f"order_is_success, {timestamp_column} FROM {table_name}"
        )
        # Rename the join key; this should now raise an error.
        assertpy.assert_that(store.get_historical_features).raises(
            errors.FeastEntityDFMissingColumnsError
        ).when_called_with(
            entity_df=entity_df_query_with_invalid_join_key,
            features=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
        )

        job_from_df = store.get_historical_features(
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

        # Rename the join key; this should now raise an error.
        orders_df_with_invalid_join_key = orders_df.rename(
            {"customer_id": "customer"}, axis="columns"
        )
        assertpy.assert_that(store.get_historical_features).raises(
            errors.FeastEntityDFMissingColumnsError
        ).when_called_with(
            entity_df=orders_df_with_invalid_join_key,
            features=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
        )

        start_time = datetime.utcnow()
        actual_df_from_df_entities = job_from_df.to_df()
        end_time = datetime.utcnow()
        with capsys.disabled():
            print(
                str(
                    f"Time to execute job_from_df.to_df() = '{(end_time - start_time)}'\n"
                )
            )

        assert sorted(expected_df.columns) == sorted(actual_df_from_df_entities.columns)
        assert_frame_equal(
            expected_df.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            actual_df_from_df_entities[expected_df.columns]
            .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
            .reset_index(drop=True),
            check_dtype=False,
        )

        table_from_df_entities = job_from_df.to_arrow()
        assert_frame_equal(
            actual_df_from_df_entities.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            table_from_df_entities.to_pandas()
            .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
            .reset_index(drop=True),
        )


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
