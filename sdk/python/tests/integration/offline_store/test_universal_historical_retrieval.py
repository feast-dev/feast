from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pytz import utc

from feast import utils
from feast.errors import RequestDataNotFoundInEntityDfException
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
    table_name_from_data_source,
)
from tests.integration.feature_repos.universal.entities import customer, driver

np.random.seed(0)


def convert_timestamp_records_to_utc(
    records: List[Dict[str, Any]], column: str
) -> List[Dict[str, Any]]:
    for record in records:
        record[column] = utils.make_tzaware(record[column]).astimezone(utc)
    return records


# Find the latest record in the given time range and filter
def find_asof_record(
    records: List[Dict[str, Any]],
    ts_key: str,
    ts_start: datetime,
    ts_end: datetime,
    filter_keys: Optional[List[str]] = None,
    filter_values: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    filter_keys = filter_keys or []
    filter_values = filter_values or []
    assert len(filter_keys) == len(filter_values)
    found_record = {}
    for record in records:
        if (
            all(
                [
                    record[filter_key] == filter_value
                    for filter_key, filter_value in zip(filter_keys, filter_values)
                ]
            )
            and ts_start <= record[ts_key] <= ts_end
        ):
            if not found_record or found_record[ts_key] < record[ts_key]:
                found_record = record
    return found_record


def get_expected_training_df(
    customer_df: pd.DataFrame,
    customer_fv: FeatureView,
    driver_df: pd.DataFrame,
    driver_fv: FeatureView,
    orders_df: pd.DataFrame,
    order_fv: FeatureView,
    global_df: pd.DataFrame,
    global_fv: FeatureView,
    entity_df: pd.DataFrame,
    event_timestamp: str,
    full_feature_names: bool = False,
):
    # Convert all pandas dataframes into records with UTC timestamps
    customer_records = convert_timestamp_records_to_utc(
        customer_df.to_dict("records"), customer_fv.batch_source.event_timestamp_column
    )
    driver_records = convert_timestamp_records_to_utc(
        driver_df.to_dict("records"), driver_fv.batch_source.event_timestamp_column
    )
    order_records = convert_timestamp_records_to_utc(
        orders_df.to_dict("records"), event_timestamp
    )
    global_records = convert_timestamp_records_to_utc(
        global_df.to_dict("records"), global_fv.batch_source.event_timestamp_column
    )
    entity_rows = convert_timestamp_records_to_utc(
        entity_df.to_dict("records"), event_timestamp
    )

    # Manually do point-in-time join of driver, customer, and order records against
    # the entity df
    for entity_row in entity_rows:
        customer_record = find_asof_record(
            customer_records,
            ts_key=customer_fv.batch_source.event_timestamp_column,
            ts_start=entity_row[event_timestamp] - customer_fv.ttl,
            ts_end=entity_row[event_timestamp],
            filter_keys=["customer_id"],
            filter_values=[entity_row["customer_id"]],
        )
        driver_record = find_asof_record(
            driver_records,
            ts_key=driver_fv.batch_source.event_timestamp_column,
            ts_start=entity_row[event_timestamp] - driver_fv.ttl,
            ts_end=entity_row[event_timestamp],
            filter_keys=["driver_id"],
            filter_values=[entity_row["driver_id"]],
        )
        order_record = find_asof_record(
            order_records,
            ts_key=customer_fv.batch_source.event_timestamp_column,
            ts_start=entity_row[event_timestamp] - order_fv.ttl,
            ts_end=entity_row[event_timestamp],
            filter_keys=["customer_id", "driver_id"],
            filter_values=[entity_row["customer_id"], entity_row["driver_id"]],
        )
        global_record = find_asof_record(
            global_records,
            ts_key=global_fv.batch_source.event_timestamp_column,
            ts_start=order_record[event_timestamp] - global_fv.ttl,
            ts_end=order_record[event_timestamp],
        )

        entity_row.update(
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
        entity_row.update(
            {
                (f"driver_stats__{k}" if full_feature_names else k): driver_record.get(
                    k, None
                )
                for k in ("conv_rate", "avg_daily_trips")
            }
        )
        entity_row.update(
            {
                (f"order__{k}" if full_feature_names else k): order_record.get(k, None)
                for k in ("order_is_success",)
            }
        )
        entity_row.update(
            {
                (f"global_stats__{k}" if full_feature_names else k): global_record.get(
                    k, None
                )
                for k in ("num_rides", "avg_ride_length",)
            }
        )

    # Convert records back to pandas dataframe
    expected_df = pd.DataFrame(entity_rows)

    # Move "event_timestamp" column to front
    current_cols = expected_df.columns.tolist()
    current_cols.remove(event_timestamp)
    expected_df = expected_df[[event_timestamp] + current_cols]

    # Cast some columns to expected types, since we lose information when converting pandas DFs into Python objects.
    if full_feature_names:
        expected_column_types = {
            "order__order_is_success": "int32",
            "driver_stats__conv_rate": "float32",
            "customer_profile__current_balance": "float32",
            "customer_profile__avg_passenger_count": "float32",
            "global_stats__avg_ride_length": "float32",
        }
    else:
        expected_column_types = {
            "order_is_success": "int32",
            "conv_rate": "float32",
            "current_balance": "float32",
            "avg_passenger_count": "float32",
            "avg_ride_length": "float32",
        }

    for col, typ in expected_column_types.items():
        expected_df[col] = expected_df[col].astype(typ)

    conv_feature_name = "driver_stats__conv_rate" if full_feature_names else "conv_rate"
    expected_df["conv_rate_plus_100"] = expected_df[conv_feature_name] + 100
    expected_df["conv_rate_plus_val_to_add"] = (
        expected_df[conv_feature_name] + expected_df["val_to_add"]
    )

    return expected_df


@pytest.mark.integration
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_historical_features(environment, universal_data_sources, full_feature_names):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    customer_df, driver_df, orders_df, global_df, entity_df = (
        datasets["customer"],
        datasets["driver"],
        datasets["orders"],
        datasets["global"],
        datasets["entity"],
    )
    entity_df_with_request_data = entity_df.copy(deep=True)
    entity_df_with_request_data["val_to_add"] = [
        i for i in range(len(entity_df_with_request_data))
    ]

    customer_fv, driver_fv, driver_odfv, order_fv, global_fv = (
        feature_views["customer"],
        feature_views["driver"],
        feature_views["driver_odfv"],
        feature_views["order"],
        feature_views["global"],
    )

    feature_service = FeatureService(
        "convrate_plus100",
        features=[feature_views["driver"][["conv_rate"]], feature_views["driver_odfv"]],
    )

    feast_objects = []
    feast_objects.extend(
        [
            customer_fv,
            driver_fv,
            driver_odfv,
            order_fv,
            global_fv,
            driver(),
            customer(),
            feature_service,
        ]
    )
    store.apply(feast_objects)

    entity_df_query = None
    orders_table = table_name_from_data_source(data_sources["orders"])
    if orders_table:
        entity_df_query = f"SELECT customer_id, driver_id, order_id, event_timestamp FROM {orders_table}"

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
        order_fv,
        global_df,
        global_fv,
        entity_df_with_request_data,
        event_timestamp,
        full_feature_names,
    )

    if entity_df_query:
        job_from_sql = store.get_historical_features(
            entity_df=entity_df_query,
            features=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
                "order:order_is_success",
                "global_stats:num_rides",
                "global_stats:avg_ride_length",
            ],
            full_feature_names=full_feature_names,
        )

        start_time = datetime.utcnow()
        actual_df_from_sql_entities = job_from_sql.to_df()
        end_time = datetime.utcnow()
        print(
            str(f"\nTime to execute job_from_sql.to_df() = '{(end_time - start_time)}'")
        )

        # Not requesting the on demand transform with an entity_df query (can't add request data in them)
        expected_df_query = expected_df.drop(
            columns=["conv_rate_plus_100", "val_to_add", "conv_rate_plus_val_to_add"]
        )
        assert sorted(expected_df_query.columns) == sorted(
            actual_df_from_sql_entities.columns
        )

        actual_df_from_sql_entities = (
            actual_df_from_sql_entities[expected_df_query.columns]
            .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
            .drop_duplicates()
            .reset_index(drop=True)
        )
        expected_df_query = (
            expected_df_query.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )

        assert_frame_equal(
            actual_df_from_sql_entities, expected_df_query, check_dtype=False,
        )

        table_from_sql_entities = job_from_sql.to_arrow()
        df_from_sql_entities = (
            table_from_sql_entities.to_pandas()[expected_df_query.columns]
            .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
            .drop_duplicates()
            .reset_index(drop=True)
        )

        for col in df_from_sql_entities.columns:
            expected_df_query[col] = expected_df_query[col].astype(
                df_from_sql_entities[col].dtype
            )

        assert_frame_equal(expected_df_query, df_from_sql_entities)

    job_from_df = store.get_historical_features(
        entity_df=entity_df_with_request_data,
        features=[
            "driver_stats:conv_rate",
            "driver_stats:avg_daily_trips",
            "customer_profile:current_balance",
            "customer_profile:avg_passenger_count",
            "customer_profile:lifetime_trip_count",
            "conv_rate_plus_100:conv_rate_plus_100",
            "conv_rate_plus_100:conv_rate_plus_val_to_add",
            "order:order_is_success",
            "global_stats:num_rides",
            "global_stats:avg_ride_length",
        ],
        full_feature_names=full_feature_names,
    )

    start_time = datetime.utcnow()
    actual_df_from_df_entities = job_from_df.to_df()

    print(f"actual_df_from_df_entities shape: {actual_df_from_df_entities.shape}")
    end_time = datetime.utcnow()
    print(str(f"Time to execute job_from_df.to_df() = '{(end_time - start_time)}'\n"))

    assert sorted(expected_df.columns) == sorted(actual_df_from_df_entities.columns)
    expected_df: pd.DataFrame = (
        expected_df.sort_values(
            by=[event_timestamp, "order_id", "driver_id", "customer_id"]
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )
    actual_df_from_df_entities = (
        actual_df_from_df_entities[expected_df.columns]
        .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    assert_frame_equal(
        expected_df, actual_df_from_df_entities, check_dtype=False,
    )
    assert_feature_service_correctness(
        store,
        feature_service,
        full_feature_names,
        entity_df_with_request_data,
        expected_df,
        event_timestamp,
    )

    table_from_df_entities: pd.DataFrame = job_from_df.to_arrow().to_pandas()

    columns_expected_in_table = expected_df.columns.tolist()

    table_from_df_entities = (
        table_from_df_entities[columns_expected_in_table]
        .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
        .drop_duplicates()
        .reset_index(drop=True)
    )
    assert_frame_equal(actual_df_from_df_entities, table_from_df_entities)

    # If request data is missing that's needed for on demand transform, throw an error
    with pytest.raises(RequestDataNotFoundInEntityDfException):
        store.get_historical_features(
            entity_df=entity_df,
            features=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
                "conv_rate_plus_100:conv_rate_plus_100",
                "conv_rate_plus_100:conv_rate_plus_val_to_add",
                "global_stats:num_rides",
                "global_stats:avg_ride_length",
            ],
            full_feature_names=full_feature_names,
        )


def response_feature_name(feature: str, full_feature_names: bool) -> str:
    if feature in {"conv_rate", "avg_daily_trips"} and full_feature_names:
        return f"driver_stats__{feature}"

    if feature in {"conv_rate_plus_100"} and full_feature_names:
        return f"conv_rate_plus_100__{feature}"

    return feature


def assert_feature_service_correctness(
    store, feature_service, full_feature_names, entity_df, expected_df, event_timestamp
):

    job_from_df = store.get_historical_features(
        entity_df=entity_df,
        features=feature_service,
        full_feature_names=full_feature_names,
    )

    actual_df_from_df_entities = job_from_df.to_df()

    expected_df: pd.DataFrame = (
        expected_df.sort_values(
            by=[event_timestamp, "order_id", "driver_id", "customer_id"]
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )
    expected_df = expected_df[
        [
            event_timestamp,
            "order_id",
            "driver_id",
            "customer_id",
            response_feature_name("conv_rate", full_feature_names),
            "conv_rate_plus_100",
        ]
    ]
    actual_df_from_df_entities = (
        actual_df_from_df_entities[expected_df.columns]
        .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    assert_frame_equal(
        expected_df, actual_df_from_df_entities, check_dtype=False,
    )
