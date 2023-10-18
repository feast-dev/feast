from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from pytz import utc

from feast import FeatureService, FeatureStore, utils
from feast.errors import FeatureNameCollisionError
from feast.feature_view import FeatureView


def convert_timestamp_records_to_utc(
    records: List[Dict[str, Any]], column: str
) -> List[Dict[str, Any]]:
    for record in records:
        record[column] = utils.make_tzaware(record[column]).astimezone(utc)
    return records


# Find the latest record in the given time range and filter
def find_latest_record(
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
    found_record: Dict[str, Any] = {}
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
    location_df: pd.DataFrame,
    location_fv: FeatureView,
    global_df: pd.DataFrame,
    global_fv: FeatureView,
    field_mapping_df: pd.DataFrame,
    field_mapping_fv: FeatureView,
    entity_df: pd.DataFrame,
    event_timestamp: str,
    full_feature_names: bool = False,
):
    # Convert all pandas dataframes into records with UTC timestamps
    customer_records = convert_timestamp_records_to_utc(
        customer_df.to_dict("records"), customer_fv.batch_source.timestamp_field
    )
    driver_records = convert_timestamp_records_to_utc(
        driver_df.to_dict("records"), driver_fv.batch_source.timestamp_field
    )
    order_records = convert_timestamp_records_to_utc(
        orders_df.to_dict("records"), event_timestamp
    )
    location_records = convert_timestamp_records_to_utc(
        location_df.to_dict("records"), location_fv.batch_source.timestamp_field
    )
    global_records = convert_timestamp_records_to_utc(
        global_df.to_dict("records"), global_fv.batch_source.timestamp_field
    )
    field_mapping_records = convert_timestamp_records_to_utc(
        field_mapping_df.to_dict("records"),
        field_mapping_fv.batch_source.timestamp_field,
    )
    entity_rows = convert_timestamp_records_to_utc(
        entity_df.to_dict("records"), event_timestamp
    )

    # Set sufficiently large ttl that it effectively functions as infinite for the calculations below.
    default_ttl = timedelta(weeks=52)

    # Manually do point-in-time join of driver, customer, and order records against
    # the entity df
    for entity_row in entity_rows:
        customer_record = find_latest_record(
            customer_records,
            ts_key=customer_fv.batch_source.timestamp_field,
            ts_start=entity_row[event_timestamp]
            - _get_feature_view_ttl(customer_fv, default_ttl),
            ts_end=entity_row[event_timestamp],
            filter_keys=["customer_id"],
            filter_values=[entity_row["customer_id"]],
        )
        driver_record = find_latest_record(
            driver_records,
            ts_key=driver_fv.batch_source.timestamp_field,
            ts_start=entity_row[event_timestamp]
            - _get_feature_view_ttl(driver_fv, default_ttl),
            ts_end=entity_row[event_timestamp],
            filter_keys=["driver_id"],
            filter_values=[entity_row["driver_id"]],
        )
        order_record = find_latest_record(
            order_records,
            ts_key=customer_fv.batch_source.timestamp_field,
            ts_start=entity_row[event_timestamp]
            - _get_feature_view_ttl(order_fv, default_ttl),
            ts_end=entity_row[event_timestamp],
            filter_keys=["customer_id", "driver_id"],
            filter_values=[entity_row["customer_id"], entity_row["driver_id"]],
        )
        origin_record = find_latest_record(
            location_records,
            ts_key=location_fv.batch_source.timestamp_field,
            ts_start=order_record[event_timestamp]
            - _get_feature_view_ttl(location_fv, default_ttl),
            ts_end=order_record[event_timestamp],
            filter_keys=["location_id"],
            filter_values=[order_record["origin_id"]],
        )
        destination_record = find_latest_record(
            location_records,
            ts_key=location_fv.batch_source.timestamp_field,
            ts_start=order_record[event_timestamp]
            - _get_feature_view_ttl(location_fv, default_ttl),
            ts_end=order_record[event_timestamp],
            filter_keys=["location_id"],
            filter_values=[order_record["destination_id"]],
        )
        global_record = find_latest_record(
            global_records,
            ts_key=global_fv.batch_source.timestamp_field,
            ts_start=order_record[event_timestamp]
            - _get_feature_view_ttl(global_fv, default_ttl),
            ts_end=order_record[event_timestamp],
        )

        field_mapping_record = find_latest_record(
            field_mapping_records,
            ts_key=field_mapping_fv.batch_source.timestamp_field,
            ts_start=order_record[event_timestamp]
            - _get_feature_view_ttl(field_mapping_fv, default_ttl),
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
                "origin__temperature": origin_record.get("temperature", None),
                "destination__temperature": destination_record.get("temperature", None),
            }
        )
        entity_row.update(
            {
                (f"global_stats__{k}" if full_feature_names else k): global_record.get(
                    k, None
                )
                for k in (
                    "num_rides",
                    "avg_ride_length",
                )
            }
        )

        # get field_mapping_record by column name, but label by feature name
        entity_row.update(
            {
                (
                    f"field_mapping__{feature}" if full_feature_names else feature
                ): field_mapping_record.get(column, None)
                for (
                    column,
                    feature,
                ) in field_mapping_fv.batch_source.field_mapping.items()
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
            "field_mapping__feature_name": "int32",
        }
    else:
        expected_column_types = {
            "order_is_success": "int32",
            "conv_rate": "float32",
            "current_balance": "float32",
            "avg_passenger_count": "float32",
            "avg_ride_length": "float32",
            "feature_name": "int32",
        }

    for col, typ in expected_column_types.items():
        expected_df[col] = expected_df[col].astype(typ)

    conv_feature_name = "driver_stats__conv_rate" if full_feature_names else "conv_rate"
    conv_plus_feature_name = get_response_feature_name(
        "conv_rate_plus_100", full_feature_names
    )
    expected_df[conv_plus_feature_name] = expected_df[conv_feature_name] + 100
    expected_df[
        get_response_feature_name("conv_rate_plus_100_rounded", full_feature_names)
    ] = (
        expected_df[conv_plus_feature_name]
        .astype("float")
        .round()
        .astype(pd.Int32Dtype())
    )
    if "val_to_add" in expected_df.columns:
        expected_df[
            get_response_feature_name("conv_rate_plus_val_to_add", full_feature_names)
        ] = (expected_df[conv_feature_name] + expected_df["val_to_add"])

    return expected_df


def get_response_feature_name(feature: str, full_feature_names: bool) -> str:
    if feature in {"conv_rate", "avg_daily_trips"} and full_feature_names:
        return f"driver_stats__{feature}"

    if (
        feature
        in {
            "conv_rate_plus_100",
            "conv_rate_plus_100_rounded",
            "conv_rate_plus_val_to_add",
        }
        and full_feature_names
    ):
        return f"conv_rate_plus_100__{feature}"

    return feature


def assert_feature_service_correctness(
    store: FeatureStore,
    feature_service: FeatureService,
    full_feature_names: bool,
    entity_df,
    expected_df,
    event_timestamp,
):

    job_from_df = store.get_historical_features(
        entity_df=entity_df,
        features=store.get_feature_service(feature_service.name),
        full_feature_names=full_feature_names,
    )

    actual_df_from_df_entities = job_from_df.to_df()

    expected_df = expected_df[
        [
            event_timestamp,
            "order_id",
            "driver_id",
            "customer_id",
            get_response_feature_name("conv_rate", full_feature_names),
            get_response_feature_name("conv_rate_plus_100", full_feature_names),
            "driver_age",
        ]
    ]

    validate_dataframes(
        expected_df,
        actual_df_from_df_entities,
        sort_by=[event_timestamp, "order_id", "driver_id", "customer_id"],
        event_timestamp_column=event_timestamp,
        timestamp_precision=timedelta(milliseconds=1),
    )


def assert_feature_service_entity_mapping_correctness(
    store, feature_service, full_feature_names, entity_df, expected_df, event_timestamp
):
    if full_feature_names:
        job_from_df = store.get_historical_features(
            entity_df=entity_df,
            features=feature_service,
            full_feature_names=full_feature_names,
        )
        actual_df_from_df_entities = job_from_df.to_df()

        expected_df: pd.DataFrame = (
            expected_df.sort_values(
                by=[
                    event_timestamp,
                    "order_id",
                    "driver_id",
                    "customer_id",
                    "origin_id",
                    "destination_id",
                ]
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
                "origin_id",
                "destination_id",
                "origin__temperature",
                "destination__temperature",
            ]
        ]

        validate_dataframes(
            expected_df,
            actual_df_from_df_entities,
            sort_by=[
                event_timestamp,
                "order_id",
                "driver_id",
                "customer_id",
                "origin_id",
                "destination_id",
            ],
            event_timestamp_column=event_timestamp,
            timestamp_precision=timedelta(milliseconds=1),
        )
    else:
        # using 2 of the same FeatureView without full_feature_names=True will result in collision
        with pytest.raises(FeatureNameCollisionError):
            job_from_df = store.get_historical_features(
                entity_df=entity_df,
                features=feature_service,
                full_feature_names=full_feature_names,
            )


# Specify timestamp_precision to relax timestamp equality constraints
def validate_dataframes(
    expected_df: pd.DataFrame,
    actual_df: pd.DataFrame,
    sort_by: List[str],
    event_timestamp_column: Optional[str] = None,
    timestamp_precision: timedelta = timedelta(seconds=0),
):
    expected_df = (
        expected_df.sort_values(by=sort_by).drop_duplicates().reset_index(drop=True)
    )

    actual_df = (
        actual_df[expected_df.columns]
        .sort_values(by=sort_by)
        .drop_duplicates()
        .reset_index(drop=True)
    )
    if event_timestamp_column:
        expected_timestamp_col = expected_df[event_timestamp_column].to_frame()
        actual_timestamp_col = expected_df[event_timestamp_column].to_frame()
        expected_df = expected_df.drop(event_timestamp_column, axis=1)
        actual_df = actual_df.drop(event_timestamp_column, axis=1)
        if event_timestamp_column in sort_by:
            sort_by.remove(event_timestamp_column)

        diffs = expected_timestamp_col.to_numpy() - actual_timestamp_col.to_numpy()
        for diff in diffs:
            if isinstance(diff, np.ndarray):
                diff = diff[0]
            if isinstance(diff, np.timedelta64):
                assert abs(diff) <= timestamp_precision.seconds
            else:
                assert abs(diff) <= timestamp_precision
    pd_assert_frame_equal(
        expected_df,
        actual_df,
        check_dtype=False,
    )


def _get_feature_view_ttl(
    feature_view: FeatureView, default_ttl: timedelta
) -> timedelta:
    """Returns the ttl of a feature view if it is non-zero. Otherwise returns the specified default."""
    return feature_view.ttl if feature_view.ttl else default_ttl


def validate_online_features(
    store: FeatureStore, driver_df: pd.DataFrame, max_date: datetime
):
    """Assert that features in online store are up to date with `max_date` date."""
    # Read features back
    response = store.get_online_features(
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:avg_daily_trips",
            "global_daily_stats:num_rides",
            "global_daily_stats:avg_ride_length",
        ],
        entity_rows=[{"driver_id": 1001}],
        full_feature_names=True,
    )

    # Float features should still be floats.
    assert (
        response.proto.results[
            list(response.proto.metadata.feature_names.val).index(
                "driver_hourly_stats__conv_rate"
            )
        ]
        .values[0]
        .float_val
        > 0
    ), response.to_dict()

    result = response.to_dict()
    assert len(result) == 5
    assert "driver_hourly_stats__avg_daily_trips" in result
    assert "driver_hourly_stats__conv_rate" in result
    assert (
        abs(
            result["driver_hourly_stats__conv_rate"][0]
            - get_last_feature_row(driver_df, 1001, max_date)["conv_rate"]
        )
        < 0.01
    )
    assert "global_daily_stats__num_rides" in result
    assert "global_daily_stats__avg_ride_length" in result

    # Test the ODFV if it exists.
    odfvs = store.list_on_demand_feature_views()
    if odfvs and odfvs[0].name == "conv_rate_plus_100":
        response = store.get_online_features(
            features=[
                "conv_rate_plus_100:conv_rate_plus_100",
                "conv_rate_plus_100:conv_rate_plus_val_to_add",
            ],
            entity_rows=[{"driver_id": 1001, "val_to_add": 100}],
            full_feature_names=True,
        )

        # Check that float64 feature is stored correctly in proto format.
        assert (
            response.proto.results[
                list(response.proto.metadata.feature_names.val).index(
                    "conv_rate_plus_100__conv_rate_plus_100"
                )
            ]
            .values[0]
            .double_val
            > 0
        )

        result = response.to_dict()
        assert len(result) == 3
        assert "conv_rate_plus_100__conv_rate_plus_100" in result
        assert "conv_rate_plus_100__conv_rate_plus_val_to_add" in result
        assert (
            abs(
                result["conv_rate_plus_100__conv_rate_plus_100"][0]
                - (get_last_feature_row(driver_df, 1001, max_date)["conv_rate"] + 100)
            )
            < 0.01
        )
        assert (
            abs(
                result["conv_rate_plus_100__conv_rate_plus_val_to_add"][0]
                - (get_last_feature_row(driver_df, 1001, max_date)["conv_rate"] + 100)
            )
            < 0.01
        )


def get_last_feature_row(df: pd.DataFrame, driver_id, max_date: datetime):
    """Manually extract last feature value from a dataframe for a given driver_id with up to `max_date` date"""
    filtered = df[
        (df["driver_id"] == driver_id)
        & (df["event_timestamp"] < max_date.replace(tzinfo=utc))
    ]
    max_ts = filtered.loc[filtered["event_timestamp"].idxmax()]["event_timestamp"]
    filtered_by_ts = filtered[filtered["event_timestamp"] == max_ts]
    return filtered_by_ts.loc[filtered_by_ts["created"].idxmax()]
