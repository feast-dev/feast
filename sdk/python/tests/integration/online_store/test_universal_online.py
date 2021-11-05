import datetime
import itertools
import os
import unittest
from datetime import timedelta

import numpy as np
import pandas as pd
import pytest

from feast import Entity, Feature, FeatureService, FeatureView, ValueType
from feast.errors import (
    FeatureNameCollisionError,
    RequestDataNotFoundInEntityRowsException,
)
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import (
    customer,
    driver,
    location,
)
from tests.integration.feature_repos.universal.feature_views import (
    create_driver_hourly_stats_feature_view,
)
from tests.utils.data_source_utils import prep_file_source


# TODO: make this work with all universal (all online store types)
@pytest.mark.integration
def test_write_to_online_store_event_check(local_redis_environment):
    if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "True":
        return
    fs = local_redis_environment.feature_store

    # write same data points 3 with different timestamps
    now = pd.Timestamp(datetime.datetime.utcnow()).round("ms")
    hour_ago = pd.Timestamp(datetime.datetime.utcnow() - timedelta(hours=1)).round("ms")
    latest = pd.Timestamp(datetime.datetime.utcnow() + timedelta(seconds=1)).round("ms")

    data = {
        "id": [123, 567, 890],
        "string_col": ["OLD_FEATURE", "LATEST_VALUE2", "LATEST_VALUE3"],
        "ts_1": [hour_ago, now, now],
    }
    dataframe_source = pd.DataFrame(data)
    with prep_file_source(
        df=dataframe_source, event_timestamp_column="ts_1"
    ) as file_source:
        e = Entity(name="id", value_type=ValueType.STRING)

        # Create Feature View
        fv1 = FeatureView(
            name="feature_view_123",
            features=[Feature(name="string_col", dtype=ValueType.STRING)],
            entities=["id"],
            batch_source=file_source,
            ttl=timedelta(minutes=5),
        )
        # Register Feature View and Entity
        fs.apply([fv1, e])

        #  data to ingest into Online Store (recent)
        data = {
            "id": [123],
            "string_col": ["hi_123"],
            "ts_1": [now],
        }
        df_data = pd.DataFrame(data)

        # directly ingest data into the Online Store
        fs.write_to_online_store("feature_view_123", df_data)

        df = fs.get_online_features(
            features=["feature_view_123:string_col"], entity_rows=[{"id": 123}]
        ).to_df()
        assert df["string_col"].iloc[0] == "hi_123"

        # data to ingest into Online Store (1 hour delayed data)
        # should now overwrite features for id=123 because it's less recent data
        data = {
            "id": [123, 567, 890],
            "string_col": ["bye_321", "hello_123", "greetings_321"],
            "ts_1": [hour_ago, hour_ago, hour_ago],
        }
        df_data = pd.DataFrame(data)

        # directly ingest data into the Online Store
        fs.write_to_online_store("feature_view_123", df_data)

        df = fs.get_online_features(
            features=["feature_view_123:string_col"],
            entity_rows=[{"id": 123}, {"id": 567}, {"id": 890}],
        ).to_df()
        assert df["string_col"].iloc[0] == "hi_123"
        assert df["string_col"].iloc[1] == "hello_123"
        assert df["string_col"].iloc[2] == "greetings_321"

        # should overwrite string_col for id=123 because it's most recent based on event_timestamp
        data = {
            "id": [123],
            "string_col": ["LATEST_VALUE"],
            "ts_1": [latest],
        }
        df_data = pd.DataFrame(data)

        fs.write_to_online_store("feature_view_123", df_data)

        df = fs.get_online_features(
            features=["feature_view_123:string_col"],
            entity_rows=[{"id": 123}, {"id": 567}, {"id": 890}],
        ).to_df()
        assert df["string_col"].iloc[0] == "LATEST_VALUE"
        assert df["string_col"].iloc[1] == "hello_123"
        assert df["string_col"].iloc[2] == "greetings_321"

        # writes to online store via datasource (dataframe_source) materialization
        fs.materialize(
            start_date=datetime.datetime.now() - timedelta(hours=12),
            end_date=datetime.datetime.utcnow(),
        )

        df = fs.get_online_features(
            features=["feature_view_123:string_col"],
            entity_rows=[{"id": 123}, {"id": 567}, {"id": 890}],
        ).to_df()
        assert df["string_col"].iloc[0] == "LATEST_VALUE"
        assert df["string_col"].iloc[1] == "LATEST_VALUE2"
        assert df["string_col"].iloc[2] == "LATEST_VALUE3"


@pytest.mark.integration
def test_write_to_online_store(environment, universal_data_sources):
    fs = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    driver_hourly_stats = create_driver_hourly_stats_feature_view(
        data_sources["driver"]
    )
    driver_entity = driver()

    # Register Feature View and Entity
    fs.apply([driver_hourly_stats, driver_entity])

    # fake data to ingest into Online Store
    data = {
        "driver_id": [123],
        "conv_rate": [0.85],
        "acc_rate": [0.91],
        "avg_daily_trips": [14],
        "event_timestamp": [pd.Timestamp(datetime.datetime.utcnow()).round("ms")],
        "created": [pd.Timestamp(datetime.datetime.utcnow()).round("ms")],
    }
    df_data = pd.DataFrame(data)

    # directly ingest data into the Online Store
    fs.write_to_online_store("driver_stats", df_data)

    # assert the right data is in the Online Store
    df = fs.get_online_features(
        features=[
            "driver_stats:avg_daily_trips",
            "driver_stats:acc_rate",
            "driver_stats:conv_rate",
        ],
        entity_rows=[{"driver": 123}],
    ).to_df()
    assert df["avg_daily_trips"].iloc[0] == 14
    assert df["acc_rate"].iloc[0] == 0.91
    assert df["conv_rate"].iloc[0] == 0.85


@pytest.mark.integration
@pytest.mark.universal
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_online_retrieval(environment, universal_data_sources, full_feature_names):

    fs = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    feature_service = FeatureService(
        "convrate_plus100",
        features=[
            feature_views["driver"][["conv_rate"]],
            feature_views["driver_odfv"],
            feature_views["driver_age_request_fv"],
        ],
    )
    feature_service_entity_mapping = FeatureService(
        name="entity_mapping",
        features=[
            feature_views["location"]
            .with_name("origin")
            .with_join_key_map({"location_id": "origin_id"}),
            feature_views["location"]
            .with_name("destination")
            .with_join_key_map({"location_id": "destination_id"}),
        ],
    )

    feast_objects = []
    feast_objects.extend(feature_views.values())
    feast_objects.extend(
        [
            driver(),
            customer(),
            location(),
            feature_service,
            feature_service_entity_mapping,
        ]
    )
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

    location_pairs = np.array(list(itertools.permutations(entities["location"], 2)))
    sample_location_pairs = location_pairs[np.random.choice(len(location_pairs), 10)].T
    origins_df = datasets["location"][
        datasets["location"]["location_id"].isin(sample_location_pairs[0])
    ]
    destinations_df = datasets["location"][
        datasets["location"]["location_id"].isin(sample_location_pairs[1])
    ]

    global_df = datasets["global"]

    entity_rows = [
        {"driver": d, "customer_id": c, "val_to_add": 50, "driver_age": 25}
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
        "driver_age:driver_age",
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

    # Test that the on demand feature views compute properly even if the dependent conv_rate
    # feature isn't requested.
    online_features_no_conv_rate = fs.get_online_features(
        features=[ref for ref in feature_refs if ref != "driver_stats:conv_rate"],
        entity_rows=entity_rows,
        full_feature_names=full_feature_names,
    )
    assert online_features_no_conv_rate is not None

    online_features_dict = online_features.to_dict()
    keys = online_features_dict.keys()
    assert (
        len(keys) == len(feature_refs) + 2
    )  # Add two for the driver id and the customer id entity keys
    for feature in feature_refs:
        # full_feature_names does not apply to request feature views
        if full_feature_names and feature != "driver_age:driver_age":
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
            driver_df=drivers_df,
            customer_df=customers_df,
            orders_df=orders_df,
            global_df=global_df,
            entity_row=entity_row,
        )

        assert df_features["customer_id"] == online_features_dict["customer_id"][i]
        assert df_features["driver_id"] == online_features_dict["driver_id"][i]
        tc.assertAlmostEqual(
            online_features_dict[
                response_feature_name("conv_rate_plus_100", full_feature_names)
            ][i],
            df_features["conv_rate"] + 100,
            delta=0.0001,
        )
        tc.assertAlmostEqual(
            online_features_dict[
                response_feature_name("conv_rate_plus_val_to_add", full_feature_names)
            ][i],
            df_features["conv_rate"] + df_features["val_to_add"],
            delta=0.0001,
        )
        for unprefixed_feature_ref in unprefixed_feature_refs:
            tc.assertAlmostEqual(
                df_features[unprefixed_feature_ref],
                online_features_dict[
                    response_feature_name(unprefixed_feature_ref, full_feature_names)
                ][i],
                delta=0.0001,
            )

    # Check what happens for missing values
    missing_responses_dict = fs.get_online_features(
        features=feature_refs,
        entity_rows=[
            {"driver": 0, "customer_id": 0, "val_to_add": 100, "driver_age": 125}
        ],
        full_feature_names=full_feature_names,
    ).to_dict()
    assert missing_responses_dict is not None
    for unprefixed_feature_ref in unprefixed_feature_refs:
        if unprefixed_feature_ref not in {"num_rides", "avg_ride_length", "driver_age"}:
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

    # Also with request data
    with pytest.raises(RequestDataNotFoundInEntityRowsException):
        fs.get_online_features(
            features=feature_refs,
            entity_rows=[{"driver": 0, "customer_id": 0, "val_to_add": 20}],
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

    entity_rows = [
        {
            "driver": driver,
            "customer_id": customer,
            "origin_id": origin,
            "destination_id": destination,
        }
        for (driver, customer, origin, destination) in zip(
            sample_drivers, sample_customers, *sample_location_pairs
        )
    ]
    assert_feature_service_entity_mapping_correctness(
        fs,
        feature_service_entity_mapping,
        entity_rows,
        full_feature_names,
        drivers_df,
        customers_df,
        orders_df,
        origins_df,
        destinations_df,
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


def get_latest_row(entity_row, df, join_key, entity_key):
    rows = df[df[join_key] == entity_row[entity_key]]
    return rows.loc[rows["event_timestamp"].idxmax()].to_dict()


def get_latest_feature_values_from_dataframes(
    driver_df,
    customer_df,
    orders_df,
    entity_row,
    global_df=None,
    origin_df=None,
    destination_df=None,
):
    latest_driver_row = get_latest_row(entity_row, driver_df, "driver_id", "driver")
    latest_customer_row = get_latest_row(
        entity_row, customer_df, "customer_id", "customer_id"
    )

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

    if global_df is not None:
        latest_global_row = global_df.loc[
            global_df["event_timestamp"].idxmax()
        ].to_dict()
    if origin_df is not None:
        latest_origin_row = get_latest_row(
            entity_row, origin_df, "location_id", "origin_id"
        )
        latest_destination_row = get_latest_row(
            entity_row, destination_df, "location_id", "destination_id"
        )
        # Need full feature names for shadow entities
        latest_origin_row["origin__temperature"] = latest_origin_row.pop("temperature")
        latest_destination_row["destination__temperature"] = latest_destination_row.pop(
            "temperature"
        )
    request_data_features = entity_row.copy()
    request_data_features.pop("driver")
    request_data_features.pop("customer_id")
    if global_df is not None:
        return {
            **latest_customer_row,
            **latest_driver_row,
            **latest_orders_row,
            **latest_global_row,
            **request_data_features,
        }
    if origin_df is not None:
        request_data_features.pop("origin_id")
        request_data_features.pop("destination_id")
        return {
            **latest_customer_row,
            **latest_driver_row,
            **latest_orders_row,
            **latest_origin_row,
            **latest_destination_row,
            **request_data_features,
        }
    return {
        **latest_customer_row,
        **latest_driver_row,
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
        == sum(
            [
                len(projection.features)
                for projection in feature_service.feature_view_projections
            ]
        )
        + 2
    )  # Add two for the driver id and the customer id entity keys

    tc = unittest.TestCase()
    for i, entity_row in enumerate(entity_rows):
        df_features = get_latest_feature_values_from_dataframes(
            driver_df=drivers_df,
            customer_df=customers_df,
            orders_df=orders_df,
            global_df=global_df,
            entity_row=entity_row,
        )
        tc.assertAlmostEqual(
            feature_service_online_features_dict[
                response_feature_name("conv_rate_plus_100", full_feature_names)
            ][i],
            df_features["conv_rate"] + 100,
            delta=0.0001,
        )


def assert_feature_service_entity_mapping_correctness(
    fs,
    feature_service,
    entity_rows,
    full_feature_names,
    drivers_df,
    customers_df,
    orders_df,
    origins_df,
    destinations_df,
):
    if full_feature_names:
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
            == sum(
                [
                    len(projection.features)
                    for projection in feature_service.feature_view_projections
                ]
            )
            + 4
        )  # Add 4 for the driver_id, customer_id, origin_id, and destination_id entity keys

        for i, entity_row in enumerate(entity_rows):
            df_features = get_latest_feature_values_from_dataframes(
                driver_df=drivers_df,
                customer_df=customers_df,
                orders_df=orders_df,
                origin_df=origins_df,
                destination_df=destinations_df,
                entity_row=entity_row,
            )
            for feature_name in ["origin__temperature", "destination__temperature"]:
                assert (
                    feature_service_online_features_dict[feature_name][i]
                    == df_features[feature_name]
                )
    else:
        # using 2 of the same FeatureView without full_feature_names=True will result in collision
        with pytest.raises(FeatureNameCollisionError):
            feature_service_response = fs.get_online_features(
                features=feature_service,
                entity_rows=entity_rows,
                full_feature_names=full_feature_names,
            )
