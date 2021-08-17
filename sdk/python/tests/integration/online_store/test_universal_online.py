import random
import unittest

import pandas as pd

from tests.integration.feature_repos.test_repo_configuration import (
    Environment,
    parametrize_online_test,
)


@parametrize_online_test
def test_online_retrieval(environment: Environment):
    fs = environment.feature_store
    full_feature_names = environment.test_repo_config.full_feature_names

    sample_drivers = random.sample(environment.driver_entities, 10)
    drivers_df = environment.driver_df[
        environment.driver_df["driver_id"].isin(sample_drivers)
    ]

    sample_customers = random.sample(environment.customer_entities, 10)
    customers_df = environment.customer_df[
        environment.customer_df["customer_id"].isin(sample_customers)
    ]

    entity_rows = [
        {"driver": d, "customer_id": c}
        for (d, c) in zip(sample_drivers, sample_customers)
    ]

    feature_refs = [
        "driver_stats:conv_rate",
        "driver_stats:avg_daily_trips",
        "customer_profile:current_balance",
        "customer_profile:avg_passenger_count",
        "customer_profile:lifetime_trip_count",
    ]
    unprefixed_feature_refs = [f.rsplit(":", 1)[-1] for f in feature_refs]

    online_features = fs.get_online_features(
        features=feature_refs,
        entity_rows=entity_rows,
        full_feature_names=full_feature_names,
    )
    assert online_features is not None

    keys = online_features.to_dict().keys()
    assert (
        len(keys) == len(feature_refs) + 2
    )  # Add two for the driver id and the customer id entity keys.
    for feature in feature_refs:
        if full_feature_names:
            assert feature.replace(":", "__") in keys
        else:
            assert feature.rsplit(":", 1)[-1] in keys
            assert "driver_stats" not in keys and "customer_profile" not in keys

    online_features_dict = online_features.to_dict()
    tc = unittest.TestCase()
    for i, entity_row in enumerate(entity_rows):
        df_features = get_latest_feature_values_from_dataframes(
            drivers_df, customers_df, entity_row
        )

        assert df_features["customer_id"] == online_features_dict["customer_id"][i]
        assert df_features["driver_id"] == online_features_dict["driver_id"][i]
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
        entity_rows=[{"driver": 0, "customer_id": 0}],
        full_feature_names=full_feature_names,
    ).to_dict()
    assert missing_responses_dict is not None
    for unprefixed_feature_ref in unprefixed_feature_refs:
        tc.assertIsNone(
            missing_responses_dict[
                response_feature_name(unprefixed_feature_ref, full_feature_names)
            ][0]
        )


def response_feature_name(feature: str, full_feature_names: bool) -> str:
    if (
        feature in {"current_balance", "avg_passenger_count", "lifetime_trip_count"}
        and full_feature_names
    ):
        return f"customer_profile__{feature}"

    if feature in {"conv_rate", "avg_daily_trips"} and full_feature_names:
        return f"driver_stats__{feature}"

    return feature


def get_latest_feature_values_from_dataframes(driver_df, customer_df, entity_row):
    driver_rows = driver_df[driver_df["driver_id"] == entity_row["driver"]]
    latest_driver_row: pd.DataFrame = driver_rows.loc[
        driver_rows["event_timestamp"].idxmax()
    ].to_dict()
    customer_rows = customer_df[customer_df["customer_id"] == entity_row["customer_id"]]
    latest_customer_row = customer_rows.loc[
        customer_rows["event_timestamp"].idxmax()
    ].to_dict()

    latest_customer_row.update(latest_driver_row)
    return latest_customer_row
