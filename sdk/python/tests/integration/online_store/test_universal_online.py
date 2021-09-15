import unittest
from datetime import timedelta

import pandas as pd
import pytest

from feast import FeatureService
from feast.errors import RequestDataNotFoundInEntityRowsException
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import customer, driver


@pytest.mark.integration
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_online_retrieval(environment, universal_data_sources, full_feature_names):

    fs = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    feature_service = FeatureService(
        "convrate_plus100",
        features=[feature_views["driver"][["conv_rate"]], feature_views["driver_odfv"]],
    )

    feast_objects = []
    feast_objects.extend(feature_views.values())
    feast_objects.extend([driver(), customer(), feature_service])
    fs.apply(feast_objects)
    fs.materialize(
        environment.start_date - timedelta(days=1),
        environment.end_date + timedelta(days=1),
    )

    entity_sample = datasets["orders"].sample(10)[
        ["customer_id", "driver_id", "order_id", "event_timestamp"]
    ]
    orders_df = datasets["orders"][
        (
            datasets["orders"]["customer_id"].isin(entity_sample["customer_id"])
            & datasets["orders"]["driver_id"].isin(entity_sample["driver_id"])
        )
    ]

    sample_drivers = entity_sample["driver_id"]
    drivers_df = datasets["driver"][
        datasets["driver"]["driver_id"].isin(sample_drivers)
    ]

    sample_customers = entity_sample["customer_id"]
    customers_df = datasets["customer"][
        datasets["customer"]["customer_id"].isin(sample_customers)
    ]

    global_df = datasets["global"]

    entity_rows = [
        {"driver": d, "customer_id": c, "val_to_add": 50}
        for (d, c) in zip(sample_drivers, sample_customers)
    ]

    feature_refs = [
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
    ]
    unprefixed_feature_refs = [f.rsplit(":", 1)[-1] for f in feature_refs if ":" in f]
    # Remove the on demand feature view output features, since they're not present in the source dataframe
    unprefixed_feature_refs.remove("conv_rate_plus_100")
    unprefixed_feature_refs.remove("conv_rate_plus_val_to_add")

    online_features = fs.get_online_features(
        features=feature_refs,
        entity_rows=entity_rows,
        full_feature_names=full_feature_names,
    )
    assert online_features is not None

    online_features_dict = online_features.to_dict()
    keys = online_features_dict.keys()
    assert (
        len(keys) == len(feature_refs) + 3
    )  # Add three for the driver id and the customer id entity keys + val_to_add request data.
    for feature in feature_refs:
        if full_feature_names:
            assert feature.replace(":", "__") in keys
        else:
            assert feature.rsplit(":", 1)[-1] in keys
            assert (
                "driver_stats" not in keys
                and "customer_profile" not in keys
                and "order" not in keys
                and "global_stats" not in keys
            )

    tc = unittest.TestCase()
    for i, entity_row in enumerate(entity_rows):
        df_features = get_latest_feature_values_from_dataframes(
            drivers_df, customers_df, orders_df, global_df, entity_row
        )

        assert df_features["customer_id"] == online_features_dict["customer_id"][i]
        assert df_features["driver_id"] == online_features_dict["driver_id"][i]
        assert (
            online_features_dict[
                response_feature_name("conv_rate_plus_100", full_feature_names)
            ][i]
            == df_features["conv_rate"] + 100
        )
        assert (
            online_features_dict[
                response_feature_name("conv_rate_plus_val_to_add", full_feature_names)
            ][i]
            == df_features["conv_rate"] + df_features["val_to_add"]
        )
        for unprefixed_feature_ref in unprefixed_feature_refs:
            tc.assertEqual(
                df_features[unprefixed_feature_ref],
                online_features_dict[
                    response_feature_name(unprefixed_feature_ref, full_feature_names)
                ][i],
            )

    # Check what happens for missing values
    missing_responses_dict = fs.get_online_features(
        features=feature_refs,
        entity_rows=[{"driver": 0, "customer_id": 0, "val_to_add": 100}],
        full_feature_names=full_feature_names,
    ).to_dict()
    assert missing_responses_dict is not None
    for unprefixed_feature_ref in unprefixed_feature_refs:
        if unprefixed_feature_ref not in {"num_rides", "avg_ride_length"}:
            tc.assertIsNone(
                missing_responses_dict[
                    response_feature_name(unprefixed_feature_ref, full_feature_names)
                ][0]
            )

    # Check what happens for missing request data
    with pytest.raises(RequestDataNotFoundInEntityRowsException):
        fs.get_online_features(
            features=feature_refs,
            entity_rows=[{"driver": 0, "customer_id": 0}],
            full_feature_names=full_feature_names,
        ).to_dict()

    assert_feature_service_correctness(
        fs,
        feature_service,
        entity_rows,
        full_feature_names,
        drivers_df,
        customers_df,
        orders_df,
        global_df,
    )


def response_feature_name(feature: str, full_feature_names: bool) -> str:
    if (
        feature in {"current_balance", "avg_passenger_count", "lifetime_trip_count"}
        and full_feature_names
    ):
        return f"customer_profile__{feature}"

    if feature in {"conv_rate", "avg_daily_trips"} and full_feature_names:
        return f"driver_stats__{feature}"

    if (
        feature in {"conv_rate_plus_100", "conv_rate_plus_val_to_add"}
        and full_feature_names
    ):
        return f"conv_rate_plus_100__{feature}"

    if feature in {"order_is_success"} and full_feature_names:
        return f"order__{feature}"

    if feature in {"num_rides", "avg_ride_length"} and full_feature_names:
        return f"global_stats__{feature}"

    return feature


def get_latest_feature_values_from_dataframes(
    driver_df, customer_df, orders_df, global_df, entity_row
):
    driver_rows = driver_df[driver_df["driver_id"] == entity_row["driver"]]
    latest_driver_row: pd.DataFrame = driver_rows.loc[
        driver_rows["event_timestamp"].idxmax()
    ].to_dict()
    customer_rows = customer_df[customer_df["customer_id"] == entity_row["customer_id"]]
    latest_customer_row = customer_rows.loc[
        customer_rows["event_timestamp"].idxmax()
    ].to_dict()

    # Since the event timestamp columns may contain timestamps of different timezones,
    # we must first convert the timestamps to UTC before we can compare them.
    order_rows = orders_df[
        (orders_df["driver_id"] == entity_row["driver"])
        & (orders_df["customer_id"] == entity_row["customer_id"])
    ]
    timestamps = order_rows[["event_timestamp"]]
    timestamps["event_timestamp"] = pd.to_datetime(
        timestamps["event_timestamp"], utc=True
    )
    max_index = timestamps["event_timestamp"].idxmax()
    latest_orders_row = order_rows.loc[max_index]

    latest_global_row = global_df.loc[global_df["event_timestamp"].idxmax()].to_dict()
    request_data_features = entity_row.copy()
    request_data_features.pop("driver")
    request_data_features.pop("customer_id")
    return {
        **latest_customer_row,
        **latest_driver_row,
        **latest_global_row,
        **latest_orders_row,
        **request_data_features,
    }


def assert_feature_service_correctness(
    fs,
    feature_service,
    entity_rows,
    full_feature_names,
    drivers_df,
    customers_df,
    orders_df,
    global_df,
):
    feature_service_response = fs.get_online_features(
        features=feature_service,
        entity_rows=entity_rows,
        full_feature_names=full_feature_names,
    )
    assert feature_service_response is not None

    feature_service_online_features_dict = feature_service_response.to_dict()
    feature_service_keys = feature_service_online_features_dict.keys()

    assert (
        len(feature_service_keys)
        == sum([len(projection.features) for projection in feature_service.features])
        + 3
    )  # Add two for the driver id and the customer id entity keys and val_to_add request data

    for i, entity_row in enumerate(entity_rows):
        df_features = get_latest_feature_values_from_dataframes(
            drivers_df, customers_df, orders_df, global_df, entity_row
        )
        assert (
            feature_service_online_features_dict[
                response_feature_name("conv_rate_plus_100", full_feature_names)
            ][i]
            == df_features["conv_rate"] + 100
        )
