from datetime import datetime
from typing import Dict, Any, List

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pytz import utc

from feast import utils
from feast.errors import FeatureNameCollisionError
from feast.feature_store import _validate_feature_refs
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)
from tests.integration.feature_repos.test_repo_configuration import (
    Environment,
    parametrize_offline_retrieval_test,
)

np.random.seed(0)


def convert_timestamp_records_to_utc(records: List[Dict[str, Any]], column: str) -> List[Dict[str, Any]]:
    for record in records:
        record[column] = utils.make_tzaware(record[column]).astimezone(utc)
    return records


# Find the latest record in the given time range and filter
def find_asof_record(records: List[Dict[str, Any]],
                     ts_key: str,
                     ts_start: datetime,
                     ts_end: datetime,
                     filter_key: str,
                     filter_value: Any) -> Dict[str, Any]:
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


@parametrize_offline_retrieval_test
def test_historical_features_from_bigquery_sources(environment: Environment):
    store = environment.feature_store

    customer_df, customer_fv = environment.customer_df, environment.customer_feature_view()
    driver_df, driver_fv = environment.driver_df, environment.driver_stats_feature_view()
    orders_df = environment.orders_df
    full_feature_names = environment.test_repo_config.full_feature_names
    entity_df_query = environment.orders_sql_fixtures()

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

    if entity_df_query:
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
        print(
            str(f"\nTime to execute job_from_sql.to_df() = '{(end_time - start_time)}'")
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

    # timestamp_column = (
    #     "e_ts"
    #     if infer_event_timestamp_col
    #     else DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
    # )

    # entity_df_query_with_invalid_join_key = (
    #     f"select order_id, driver_id, customer_id as customer, "
    #     f"order_is_success, {timestamp_column}, FROM {gcp_project}.{table_id}"
    # )
    # # Rename the join key; this should now raise an error.
    # assertpy.assert_that(store.get_historical_features).raises(
    #     errors.FeastEntityDFMissingColumnsError
    # ).when_called_with(
    #     entity_df=entity_df_query_with_invalid_join_key,
    #     features=[
    #         "driver_stats:conv_rate",
    #         "driver_stats:avg_daily_trips",
    #         "customer_profile:current_balance",
    #         "customer_profile:avg_passenger_count",
    #         "customer_profile:lifetime_trip_count",
    #     ],
    # )

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
    # orders_df_with_invalid_join_key = orders_df.rename(
    #     {"customer_id": "customer"}, axis="columns"
    # )
    # assertpy.assert_that(store.get_historical_features).raises(
    #     errors.FeastEntityDFMissingColumnsError
    # ).when_called_with(
    #     entity_df=orders_df_with_invalid_join_key,
    #     features=[
    #         "driver_stats:conv_rate",
    #         "driver_stats:avg_daily_trips",
    #         "customer_profile:current_balance",
    #         "customer_profile:avg_passenger_count",
    #         "customer_profile:lifetime_trip_count",
    #     ],
    # )

    # Make sure that custom dataset name is being used from the offline_store config
    #
    # if provider_type == "gcp_custom_offline_config":
    #     assertpy.assert_that(job_from_df.query).contains("foo.feast_entity_df")
    # else:
    #     assertpy.assert_that(job_from_df.query).contains(
    #         f"{name}.feast_entity_df"
    #     )

    start_time = datetime.utcnow()
    actual_df_from_df_entities = job_from_df.to_df()
    end_time = datetime.utcnow()
    print(str(f"Time to execute job_from_df.to_df() = '{(end_time - start_time)}'\n"))

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
    assert_frame_equal(actual_df_from_df_entities, table_from_df_entities.to_pandas())


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
