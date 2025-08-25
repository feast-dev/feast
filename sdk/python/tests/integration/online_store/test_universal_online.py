import os
import random
import time
import unittest
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Union

import assertpy
import numpy as np
import pandas as pd
import pandas.api.types as ptypes
import pytest
import requests
from botocore.exceptions import BotoCoreError

from feast import FeatureStore
from feast.entity import Entity
from feast.errors import FeatureNameCollisionError
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.utils.postgres.postgres_config import ConnectionType
from feast.online_response import TIMESTAMP_POSTFIX
from feast.types import Array, Float32, Int32, Int64, String, ValueType
from feast.utils import _utc_now
from feast.wait import wait_retry_backoff
from tests.integration.feature_repos.repo_configuration import (
    Environment,
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import driver, item
from tests.integration.feature_repos.universal.feature_views import (
    TAGS,
    create_driver_hourly_stats_feature_view,
    create_item_embeddings_feature_view,
    create_vector_feature_view,
    driver_feature_view,
)
from tests.utils.data_source_test_creator import prep_file_source


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["postgres"])
@pytest.mark.parametrize(
    "conn_type",
    [ConnectionType.singleton, ConnectionType.pool],
    ids=lambda v: f"conn_type:{v}",
)
def test_connection_pool_online_stores(
    environment, universal_data_sources, fake_ingest_data, conn_type
):
    if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "True":
        return
    fs = environment.feature_store
    fs.config.online_store.conn_type = conn_type
    fs.config.online_store.min_conn = 1
    fs.config.online_store.max_conn = 10

    entities, datasets, data_sources = universal_data_sources
    driver_hourly_stats = create_driver_hourly_stats_feature_view(data_sources.driver)
    driver_entity = driver()

    # Register Feature View and Entity
    fs.apply([driver_hourly_stats, driver_entity])

    # directly ingest data into the Online Store
    fs.write_to_online_store("driver_stats", fake_ingest_data)

    # assert the right data is in the Online Store
    df = fs.get_online_features(
        features=[
            "driver_stats:avg_daily_trips",
            "driver_stats:acc_rate",
            "driver_stats:conv_rate",
        ],
        entity_rows=[{"driver_id": 1}],
    ).to_df()
    assertpy.assert_that(df["avg_daily_trips"].iloc[0]).is_equal_to(4)
    assertpy.assert_that(df["acc_rate"].iloc[0]).is_close_to(0.6, 1e-6)
    assertpy.assert_that(df["conv_rate"].iloc[0]).is_close_to(0.5, 1e-6)


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["redis"])
def test_entity_ttl_online_store(environment, universal_data_sources, fake_ingest_data):
    if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "True":
        return
    fs = environment.feature_store
    # setting ttl setting in online store to 1 second
    fs.config.online_store.key_ttl_seconds = 1
    entities, datasets, data_sources = universal_data_sources
    driver_hourly_stats = create_driver_hourly_stats_feature_view(data_sources.driver)
    driver_entity = driver()

    # Register Feature View and Entity
    fs.apply([driver_hourly_stats, driver_entity])

    # directly ingest data into the Online Store
    fs.write_to_online_store("driver_stats", fake_ingest_data)

    # assert the right data is in the Online Store
    df = fs.get_online_features(
        features=[
            "driver_stats:avg_daily_trips",
            "driver_stats:acc_rate",
            "driver_stats:conv_rate",
        ],
        entity_rows=[{"driver_id": 1}],
    ).to_df()
    assertpy.assert_that(df["avg_daily_trips"].iloc[0]).is_equal_to(4)
    assertpy.assert_that(df["acc_rate"].iloc[0]).is_close_to(0.6, 1e-6)
    assertpy.assert_that(df["conv_rate"].iloc[0]).is_close_to(0.5, 1e-6)

    # simulate time passing for testing ttl
    time.sleep(1)

    # retrieve the same entity again
    df = fs.get_online_features(
        features=[
            "driver_stats:avg_daily_trips",
            "driver_stats:acc_rate",
            "driver_stats:conv_rate",
        ],
        entity_rows=[{"driver_id": 1}],
    ).to_df()
    # assert that the entity features expired in the online store
    assertpy.assert_that(df["avg_daily_trips"].iloc[0]).is_none()
    assertpy.assert_that(df["acc_rate"].iloc[0]).is_none()
    assertpy.assert_that(df["conv_rate"].iloc[0]).is_none()


# TODO: make this work with all universal (all online store types)
@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["redis"])
def test_write_to_online_store_event_check(environment):
    if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "True":
        return
    fs = environment.feature_store

    # write same data points 3 with different timestamps
    now = pd.Timestamp(_utc_now()).round("ms")
    hour_ago = pd.Timestamp(_utc_now() - timedelta(hours=1)).round("ms")
    latest = pd.Timestamp(_utc_now() + timedelta(seconds=1)).round("ms")

    data = {
        "id": [123, 567, 890],
        "string_col": ["OLD_FEATURE", "LATEST_VALUE2", "LATEST_VALUE3"],
        "ts_1": [hour_ago, now, now],
    }
    dataframe_source = pd.DataFrame(data)
    with prep_file_source(df=dataframe_source, timestamp_field="ts_1") as file_source:
        e = Entity(name="id")

        # Create Feature View
        fv1 = FeatureView(
            name="feature_view_123",
            schema=[Field(name="string_col", dtype=String)],
            entities=[e],
            source=file_source,
            ttl=timedelta(minutes=5),
            tags=TAGS,
        )
        # Register Feature View and Entity
        fs.apply([fv1, e])
        assert len(fs.list_all_feature_views(tags=TAGS)) == 1
        assert len(fs.list_feature_views(tags=TAGS)) == 1

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
            start_date=datetime.now() - timedelta(hours=12),
            end_date=_utc_now(),
        )

        df = fs.get_online_features(
            features=["feature_view_123:string_col"],
            entity_rows=[{"id": 123}, {"id": 567}, {"id": 890}],
        ).to_df()
        assert df["string_col"].iloc[0] == "LATEST_VALUE"
        assert df["string_col"].iloc[1] == "LATEST_VALUE2"
        assert df["string_col"].iloc[2] == "LATEST_VALUE3"


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_write_to_online_store(environment, universal_data_sources):
    fs = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    driver_hourly_stats = create_driver_hourly_stats_feature_view(data_sources.driver)
    driver_entity = driver()

    # Register Feature View and Entity
    fs.apply([driver_hourly_stats, driver_entity])

    # fake data to ingest into Online Store
    data = {
        "driver_id": [123],
        "conv_rate": [0.85],
        "acc_rate": [0.91],
        "avg_daily_trips": [14],
        "event_timestamp": [pd.Timestamp(_utc_now()).round("ms")],
        "created": [pd.Timestamp(_utc_now()).round("ms")],
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
        entity_rows=[{"driver_id": 123}],
    ).to_df()
    assertpy.assert_that(df["avg_daily_trips"].iloc[0]).is_equal_to(14)
    assertpy.assert_that(df["acc_rate"].iloc[0]).is_close_to(0.91, 1e-6)
    assertpy.assert_that(df["conv_rate"].iloc[0]).is_close_to(0.85, 1e-6)


def _get_online_features_dict_remotely(
    endpoint: str,
    features: Union[List[str], FeatureService],
    entity_rows: List[Dict[str, Any]],
    full_feature_names: bool = False,
) -> Dict[str, List[Any]]:
    """Sends the online feature request to a remote feature server (through endpoint) and returns the feature dict.

    The output should be identical to:

    fs.get_online_features(features=features, entity_rows=entity_rows, full_feature_names=full_feature_names).to_dict()

    This makes it easy to test the remote feature server by comparing the output to the local method.

    """
    request = {
        # Convert list of dicts (entity_rows) into dict of lists (entities) for json request
        "entities": {key: [row[key] for row in entity_rows] for key in entity_rows[0]},
        "full_feature_names": full_feature_names,
    }
    # Either set features of feature_service depending on the parameter
    if isinstance(features, list):
        request["features"] = features
    else:
        request["feature_service"] = features.name
    for _ in range(25):
        # Send the request to the remote feature server and get the response in JSON format
        response = requests.post(
            f"{endpoint}/get-online-features", json=request, timeout=30
        ).json()
        # Retry if the response is internal server error, which can happen when lambda is being restarted
        if response.get("message") != "Internal Server Error":
            break
        # Sleep between retries to give the server some time to start
        time.sleep(15)
    else:
        raise Exception("Failed to get online features from remote feature server")
    if "metadata" not in response:
        raise Exception(
            f"Failed to get online features from remote feature server {response}"
        )
    keys = response["metadata"]["feature_names"]
    # Get rid of unnecessary structure in the response, leaving list of dicts
    values = [row["values"] for row in response["results"]]
    # Convert list of dicts (response) into dict of lists which is the format of the return value
    return {key: feature_vector for key, feature_vector in zip(keys, values)}


def get_online_features_dict(
    environment: Environment,
    endpoint: str,
    features: Union[List[str], FeatureService],
    entity_rows: List[Dict[str, Any]],
    full_feature_names: bool = False,
) -> Dict[str, List[Any]]:
    """Get the online feature values from both SDK and remote feature servers, assert equality and return values.

    Always use this method instead of fs.get_online_features(...) in this test file.

    """
    online_features = environment.feature_store.get_online_features(
        features=features,
        entity_rows=entity_rows,
        full_feature_names=full_feature_names,
    )
    assertpy.assert_that(online_features).is_not_none()
    dict1 = online_features.to_dict()

    # If endpoint is None, it means that a local / remote feature server aren't configured
    if endpoint is not None:
        dict2 = _get_online_features_dict_remotely(
            endpoint=endpoint,
            features=features,
            entity_rows=entity_rows,
            full_feature_names=full_feature_names,
        )

        # Make sure that the two dicts are equal
        assertpy.assert_that(dict1).is_equal_to(dict2)
    elif environment.python_feature_server:
        raise ValueError(
            "feature_store.get_feature_server_endpoint() is None while python feature server is enabled"
        )
    return dict1


@pytest.mark.integration
def test_online_retrieval_with_shared_batch_source(environment, universal_data_sources):
    # Addresses https://github.com/feast-dev/feast/issues/2576

    fs = environment.feature_store

    entities, datasets, data_sources = universal_data_sources
    driver_entity = driver()
    driver_stats_v1 = FeatureView(
        name="driver_stats_v1",
        entities=[driver_entity],
        schema=[Field(name="avg_daily_trips", dtype=Int32)],
        source=data_sources.driver,
    )
    driver_stats_v2 = FeatureView(
        name="driver_stats_v2",
        entities=[driver_entity],
        schema=[
            Field(name="avg_daily_trips", dtype=Int32),
            Field(name="conv_rate", dtype=Float32),
        ],
        source=data_sources.driver,
    )

    fs.apply([driver_entity, driver_stats_v1, driver_stats_v2])

    data = pd.DataFrame(
        {
            "driver_id": [1, 2],
            "avg_daily_trips": [4, 5],
            "conv_rate": [0.5, 0.3],
            "event_timestamp": [
                pd.to_datetime(1646263500, utc=True, unit="s"),
                pd.to_datetime(1646263600, utc=True, unit="s"),
            ],
            "created": [
                pd.to_datetime(1646263500, unit="s"),
                pd.to_datetime(1646263600, unit="s"),
            ],
        }
    )
    fs.write_to_online_store("driver_stats_v1", data.drop("conv_rate", axis=1))
    fs.write_to_online_store("driver_stats_v2", data)

    with pytest.raises(KeyError):
        fs.get_online_features(
            features=[
                # `driver_stats_v1` does not have `conv_rate`
                "driver_stats_v1:conv_rate",
            ],
            entity_rows=[{"driver_id": 1}, {"driver_id": 2}],
        )


def setup_feature_store_universal_feature_views(
    environment, universal_data_sources
) -> FeatureStore:
    fs: FeatureStore = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    fs.apply([driver(), feature_views.driver, feature_views.global_fv])
    assert len(fs.list_all_feature_views(TAGS)) == 2

    data = {
        "driver_id": [1, 2],
        "conv_rate": [0.5, 0.3],
        "acc_rate": [0.6, 0.4],
        "avg_daily_trips": [4, 5],
        "event_timestamp": [
            pd.to_datetime(1646263500, utc=True, unit="s"),
            pd.to_datetime(1646263600, utc=True, unit="s"),
        ],
        "created": [
            pd.to_datetime(1646263500, unit="s"),
            pd.to_datetime(1646263600, unit="s"),
        ],
    }
    df_ingest = pd.DataFrame(data)

    fs.write_to_online_store("driver_stats", df_ingest)
    return fs


def assert_feature_store_universal_feature_views_response(df: pd.DataFrame):
    assertpy.assert_that(len(df)).is_equal_to(2)
    assertpy.assert_that(df["driver_id"].iloc[0]).is_equal_to(1)
    assertpy.assert_that(df["driver_id"].iloc[1]).is_equal_to(2)
    assertpy.assert_that(df["avg_daily_trips" + TIMESTAMP_POSTFIX].iloc[0]).is_equal_to(
        1646263500
    )
    assertpy.assert_that(df["avg_daily_trips" + TIMESTAMP_POSTFIX].iloc[1]).is_equal_to(
        1646263600
    )
    assertpy.assert_that(df["acc_rate" + TIMESTAMP_POSTFIX].iloc[0]).is_equal_to(
        1646263500
    )
    assertpy.assert_that(df["acc_rate" + TIMESTAMP_POSTFIX].iloc[1]).is_equal_to(
        1646263600
    )
    assertpy.assert_that(df["conv_rate" + TIMESTAMP_POSTFIX].iloc[0]).is_equal_to(
        1646263500
    )
    assertpy.assert_that(df["conv_rate" + TIMESTAMP_POSTFIX].iloc[1]).is_equal_to(
        1646263600
    )


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_online_retrieval_with_event_timestamps(environment, universal_data_sources):
    fs = setup_feature_store_universal_feature_views(
        environment, universal_data_sources
    )

    response = fs.get_online_features(
        features=[
            "driver_stats:avg_daily_trips",
            "driver_stats:acc_rate",
            "driver_stats:conv_rate",
        ],
        entity_rows=[{"driver_id": 1}, {"driver_id": 2}],
    )
    df = response.to_df(True)

    assert_feature_store_universal_feature_views_response(df)


async def _do_async_retrieval_test(environment, universal_data_sources):
    fs = setup_feature_store_universal_feature_views(
        environment, universal_data_sources
    )
    await fs.initialize()

    response = await fs.get_online_features_async(
        features=[
            "driver_stats:avg_daily_trips",
            "driver_stats:acc_rate",
            "driver_stats:conv_rate",
        ],
        entity_rows=[{"driver_id": 1}, {"driver_id": 2}],
    )
    df = response.to_df(True)

    assert_feature_store_universal_feature_views_response(df)

    await fs.close()


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["redis", "postgres"])
async def test_async_online_retrieval_with_event_timestamps(
    environment, universal_data_sources
):
    await _do_async_retrieval_test(environment, universal_data_sources)


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["dynamodb"])
async def test_async_online_retrieval_with_event_timestamps_dynamo(
    environment, universal_data_sources
):
    await _do_async_retrieval_test(environment, universal_data_sources)


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_online_list_retrieval(environment, universal_data_sources):
    fs = setup_feature_store_universal_feature_views(
        environment, universal_data_sources
    )

    assert len(fs.list_all_feature_views(tags=TAGS)) == 2


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["redis"])
def test_online_store_cleanup(environment, universal_data_sources):
    """
    Some online store implementations (like Redis) keep features from different features views
    but with common entities together.
    This might end up with deletion of all features attached to the entity,
    when only one feature view was deletion target (see https://github.com/feast-dev/feast/issues/2150).

    Plan:
        1. Register two feature views with common entity "driver"
        2. Materialize data
        3. Check if features are available (via online retrieval)
        4. Delete one feature view
        5. Check that features for other are still available
        6. Delete another feature view (and create again)
        7. Verify that features for both feature view were deleted
    """
    fs = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    driver_stats_fv = construct_universal_feature_views(data_sources).driver

    driver_entities = entities.driver_vals
    df = pd.DataFrame(
        {
            "ts_1": [environment.end_date] * len(driver_entities),
            "created_ts": [environment.end_date] * len(driver_entities),
            "driver_id": driver_entities,
            "value": np.random.random(size=len(driver_entities)),
        }
    )

    ds = environment.data_source_creator.create_data_source(
        df, destination_name="simple_driver_dataset"
    )

    simple_driver_fv = driver_feature_view(
        data_source=ds, name="test_universal_online_simple_driver"
    )

    fs.apply([driver(), simple_driver_fv, driver_stats_fv])

    fs.materialize(
        environment.start_date - timedelta(days=1),
        environment.end_date + timedelta(days=1),
    )
    expected_values = df.sort_values(by="driver_id")

    features = [f"{simple_driver_fv.name}:value"]
    entity_rows = [{"driver_id": driver_id} for driver_id in sorted(driver_entities)]

    online_features = fs.get_online_features(
        features=features, entity_rows=entity_rows
    ).to_dict()
    assert np.allclose(expected_values["value"], online_features["value"])

    fs.apply(
        objects=[simple_driver_fv], objects_to_delete=[driver_stats_fv], partial=False
    )

    online_features = fs.get_online_features(
        features=features, entity_rows=entity_rows
    ).to_dict()
    assert np.allclose(expected_values["value"], online_features["value"])

    fs.apply(objects=[], objects_to_delete=[simple_driver_fv], partial=False)

    def eventually_apply() -> Tuple[None, bool]:
        try:
            fs.apply([simple_driver_fv])
        except BotoCoreError:
            return None, False

        return None, True

    # Online store backend might have eventual consistency in schema update
    # So recreating table that was just deleted might need some retries
    wait_retry_backoff(eventually_apply, timeout_secs=60)

    online_features = fs.get_online_features(
        features=features, entity_rows=entity_rows
    ).to_dict()

    # Debugging print statement
    print("Online features values:", online_features["value"])

    assert all(v is None for v in online_features["value"])


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_online_retrieval_success(feature_store_for_online_retrieval):
    """
    Tests that online retrieval executes successfully (i.e. without errors).

    Does not test for correctness of the results of online retrieval.
    """
    fs, feature_refs, entity_rows = feature_store_for_online_retrieval
    fs.get_online_features(
        features=feature_refs,
        entity_rows=entity_rows,
    )


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["eg-milvus"])
def test_write_vectors_to_online_store(environment, universal_data_sources):
    fs = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    driver_daily_stats = create_vector_feature_view(data_sources.customer)
    driver_entity = driver()

    # Register Feature View and Entity
    fs.apply([driver_daily_stats, driver_entity])

    # fake data to ingest into Online Store
    data = {
        "driver_id": [123],
        "profile_embedding": [np.random.default_rng().uniform(-100, 100, 50)],
        "lifetime_trip_count": [85],
        "event_timestamp": [pd.Timestamp(datetime.datetime.utcnow()).round("ms")],
        "created": [pd.Timestamp(datetime.datetime.utcnow()).round("ms")],
    }
    df_data = pd.DataFrame(data)

    # directly ingest data into the Online Store
    fs.write_to_online_store("driver_profile", df_data)

    # assert the right data is in the Online Store
    df = fs.get_online_features(
        features=[
            "driver_profile:profile_embedding",
            "driver_profile:lifetime_trip_count",
        ],
        entity_rows=[{"driver_id": 123}],
    ).to_df()

    assert ptypes.is_array_like(df["profile_embedding"])
    assertpy.assert_that(df["profile_embedding"].iloc[0]).is_length(50)
    assertpy.assert_that(df["lifetime_trip_count"].iloc[0]).is_equal_to(85)


def response_feature_name(
    feature: str, feature_refs: List[str], full_feature_names: bool
) -> str:
    if not full_feature_names:
        return feature

    for feature_ref in feature_refs:
        if feature_ref.endswith(feature):
            return feature_ref.replace(":", "__")

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
    location_df=None,
    origin_df=None,
    destination_df=None,
):
    latest_driver_row = get_latest_row(entity_row, driver_df, "driver_id", "driver_id")
    latest_customer_row = get_latest_row(
        entity_row, customer_df, "customer_id", "customer_id"
    )
    latest_location_row = get_latest_row(
        entity_row,
        location_df,
        "location_id",
        "location_id",
    )

    # Since the event timestamp columns may contain timestamps of different timezones,
    # we must first convert the timestamps to UTC before we can compare them.
    order_rows = orders_df[
        (orders_df["driver_id"] == entity_row["driver_id"])
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
        latest_location_aliased_row = get_latest_feature_values_for_location_df(
            entity_row, origin_df, destination_df
        )

    request_data_features = entity_row.copy()
    request_data_features.pop("driver_id")
    request_data_features.pop("customer_id")
    if global_df is not None:
        return {
            **latest_customer_row,
            **latest_driver_row,
            **latest_orders_row,
            **latest_global_row,
            **latest_location_row,
            **request_data_features,
        }
    if origin_df is not None:
        request_data_features.pop("origin_id")
        request_data_features.pop("destination_id")
        return {
            **latest_customer_row,
            **latest_driver_row,
            **latest_orders_row,
            **latest_location_row,
            **latest_location_aliased_row,
            **request_data_features,
        }
    return {
        **latest_customer_row,
        **latest_driver_row,
        **latest_orders_row,
        **latest_location_row,
        **request_data_features,
    }


def get_latest_feature_values_for_location_df(entity_row, origin_df, destination_df):
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

    return {
        **latest_origin_row,
        **latest_destination_row,
    }


def get_latest_feature_values_from_location_df(entity_row, location_df):
    return get_latest_row(entity_row, location_df, "location_id", "location_id")


def assert_feature_service_correctness(
    environment,
    endpoint,
    feature_service,
    entity_rows,
    full_feature_names,
    drivers_df,
    customers_df,
    orders_df,
    global_df,
    location_df,
):
    feature_service_online_features_dict = get_online_features_dict(
        environment=environment,
        endpoint=endpoint,
        features=feature_service,
        entity_rows=entity_rows,
        full_feature_names=full_feature_names,
    )
    feature_service_keys = feature_service_online_features_dict.keys()
    expected_feature_refs = [
        (
            f"{projection.name_to_use()}__{feature.name}"
            if full_feature_names
            else feature.name
        )
        for projection in feature_service.feature_view_projections
        for feature in projection.features
    ]
    assert set(feature_service_keys) == set(expected_feature_refs) | {
        "customer_id",
        "driver_id",
        "location_id",
    }

    tc = unittest.TestCase()
    for i, entity_row in enumerate(entity_rows):
        df_features = get_latest_feature_values_from_dataframes(
            driver_df=drivers_df,
            customer_df=customers_df,
            orders_df=orders_df,
            global_df=global_df,
            entity_row=entity_row,
            location_df=location_df,
        )
        tc.assertAlmostEqual(
            feature_service_online_features_dict[
                response_feature_name(
                    "conv_rate_plus_100", expected_feature_refs, full_feature_names
                )
            ][i],
            df_features["conv_rate"] + 100,
            delta=0.0001,
        )


def assert_feature_service_entity_mapping_correctness(
    environment,
    endpoint,
    feature_service,
    entity_rows,
    full_feature_names,
    origins_df,
    destinations_df,
):
    if full_feature_names:
        feature_service_online_features_dict = get_online_features_dict(
            environment=environment,
            endpoint=endpoint,
            features=feature_service,
            entity_rows=entity_rows,
            full_feature_names=full_feature_names,
        )
        feature_service_keys = feature_service_online_features_dict.keys()

        expected_features = [
            (
                f"{projection.name_to_use()}__{feature.name}"
                if full_feature_names
                else feature.name
            )
            for projection in feature_service.feature_view_projections
            for feature in projection.features
        ]
        assert set(feature_service_keys) == set(expected_features) | {
            "destination_id",
            "origin_id",
        }

        for i, entity_row in enumerate(entity_rows):
            df_features = get_latest_feature_values_for_location_df(
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
            get_online_features_dict(
                environment=environment,
                endpoint=endpoint,
                features=feature_service,
                entity_rows=entity_rows,
                full_feature_names=full_feature_names,
            )


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["pgvector"])
def test_retrieve_online_documents(environment, fake_document_data):
    fs = environment.feature_store
    df, data_source = fake_document_data
    item_embeddings_feature_view = create_item_embeddings_feature_view(data_source)
    fs.apply([item_embeddings_feature_view, item()])
    fs.write_to_online_store("item_embeddings", df)

    documents = fs.retrieve_online_documents(
        features=["item_embeddings:embedding_float", "item_embeddings:item_id"],
        query=[1.0, 2.0],
        top_k=2,
        distance_metric="L2",
    ).to_dict()
    assert len(documents["embedding_float"]) == 2

    # assert returned the entity_id
    assert len(documents["item_id"]) == 2

    documents = fs.retrieve_online_documents(
        features=["item_embeddings:embedding_float"],
        query=[1.0, 2.0],
        top_k=2,
        distance_metric="L1",
    ).to_dict()
    assert len(documents["embedding_float"]) == 2

    with pytest.raises(ValueError):
        fs.retrieve_online_documents(
            features=["item_embeddings:embedding_float"],
            query=[1.0, 2.0],
            top_k=2,
            distance_metric="wrong",
        ).to_dict()


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["milvus"])
def test_retrieve_online_milvus_documents(environment, fake_document_data):
    fs = environment.feature_store
    df, data_source = fake_document_data
    item_embeddings_feature_view = create_item_embeddings_feature_view(data_source)
    fs.apply([item_embeddings_feature_view, item()])
    fs.write_to_online_store("item_embeddings", df)
    documents = fs.retrieve_online_documents_v2(
        features=[
            "item_embeddings:embedding_float",
            "item_embeddings:item_id",
            "item_embeddings:string_feature",
        ],
        query=[1.0, 2.0],
        top_k=2,
        distance_metric="L2",
    ).to_dict()
    assert len(documents["embedding_float"]) == 2

    assert len(documents["item_id"]) == 2
    assert documents["item_id"] == [2, 3]


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["pgvector", "elasticsearch"])
def test_retrieve_online_documents_v2(environment, fake_document_data):
    """Test retrieval of documents using vector store capabilities."""
    fs = environment.feature_store
    fs.config.online_store.vector_enabled = True

    n_rows = 20
    vector_dim = 2
    random.seed(42)

    df = pd.DataFrame(
        {
            "item_id": list(range(n_rows)),
            "embedding": [list(np.random.random(vector_dim)) for _ in range(n_rows)],
            "text_field": [
                f"Document text content {i} with searchable keywords"
                for i in range(n_rows)
            ],
            "category": [f"Category-{i % 5}" for i in range(n_rows)],
            "event_timestamp": [datetime.now() for _ in range(n_rows)],
        }
    )

    data_source = FileSource(
        path="dummy_path.parquet", timestamp_field="event_timestamp"
    )

    item = Entity(
        name="item_id",
        join_keys=["item_id"],
        value_type=ValueType.INT64,
    )

    item_embeddings_fv = FeatureView(
        name="item_embeddings",
        entities=[item],
        schema=[
            Field(name="embedding", dtype=Array(Float32), vector_index=True),
            Field(name="text_field", dtype=String),
            Field(name="category", dtype=String),
            Field(name="item_id", dtype=Int64),
        ],
        source=data_source,
    )

    fs.apply([item_embeddings_fv, item])
    fs.write_to_online_store("item_embeddings", df)

    # Test 1: Vector similarity search
    query_embedding = list(np.random.random(vector_dim))
    vector_results = fs.retrieve_online_documents_v2(
        features=[
            "item_embeddings:embedding",
            "item_embeddings:text_field",
            "item_embeddings:category",
            "item_embeddings:item_id",
        ],
        query=query_embedding,
        top_k=5,
        distance_metric="L2",
    ).to_dict()

    assert len(vector_results["embedding"]) == 5
    assert len(vector_results["distance"]) == 5
    assert len(vector_results["text_field"]) == 5
    assert len(vector_results["category"]) == 5

    # Test 2: Vector similarity search with Cosine distance
    vector_results = fs.retrieve_online_documents_v2(
        features=[
            "item_embeddings:embedding",
            "item_embeddings:text_field",
            "item_embeddings:category",
            "item_embeddings:item_id",
        ],
        query=query_embedding,
        top_k=5,
        distance_metric="cosine",
    ).to_dict()

    assert len(vector_results["embedding"]) == 5
    assert len(vector_results["distance"]) == 5
    assert len(vector_results["text_field"]) == 5
    assert len(vector_results["category"]) == 5

    # Test 3: Full text search
    text_results = fs.retrieve_online_documents_v2(
        features=[
            "item_embeddings:embedding",
            "item_embeddings:text_field",
            "item_embeddings:category",
            "item_embeddings:item_id",
        ],
        query_string="searchable keywords",
        top_k=5,
    ).to_dict()

    # Verify text search results
    assert len(text_results["text_field"]) == 5
    assert len(text_results["text_rank"]) == 5
    assert len(text_results["category"]) == 5
    assert len(text_results["item_id"]) == 5

    # Verify text rank values are between 0 and 1
    assert all(0 <= rank <= 1 for rank in text_results["text_rank"])

    # Verify results are sorted by text rank in descending order
    text_ranks = text_results["text_rank"]
    assert all(text_ranks[i] >= text_ranks[i + 1] for i in range(len(text_ranks) - 1))

    # Test 4: Hybrid search (vector + text)
    hybrid_results = fs.retrieve_online_documents_v2(
        features=[
            "item_embeddings:embedding",
            "item_embeddings:text_field",
            "item_embeddings:category",
            "item_embeddings:item_id",
        ],
        query=query_embedding,
        query_string="searchable keywords",
        top_k=5,
        distance_metric="L2",
    ).to_dict()

    # Verify hybrid search results
    assert len(hybrid_results["embedding"]) == 5
    assert len(hybrid_results["distance"]) == 5
    assert len(hybrid_results["text_field"]) == 5
    assert len(hybrid_results["text_rank"]) == 5
    assert len(hybrid_results["category"]) == 5
    assert len(hybrid_results["item_id"]) == 5

    # Test 5: Hybrid search with different text query
    hybrid_results = fs.retrieve_online_documents_v2(
        features=[
            "item_embeddings:embedding",
            "item_embeddings:text_field",
            "item_embeddings:category",
            "item_embeddings:item_id",
        ],
        query=query_embedding,
        query_string="Category-1",
        top_k=5,
        distance_metric="L2",
    ).to_dict()

    # Verify results contain only documents from Category-1
    assert all(cat == "Category-1" for cat in hybrid_results["category"])

    # Test 6: Full text search with no matches
    no_match_results = fs.retrieve_online_documents_v2(
        features=[
            "item_embeddings:embedding",
            "item_embeddings:text_field",
            "item_embeddings:category",
            "item_embeddings:item_id",
        ],
        query_string="nonexistent keyword",
        top_k=5,
    ).to_dict()

    # Verify no results are returned for non-matching query
    assert "text_field" in no_match_results
    assert len(no_match_results["text_field"]) == 0
    assert "text_rank" in no_match_results
    assert len(no_match_results["text_rank"]) == 0
