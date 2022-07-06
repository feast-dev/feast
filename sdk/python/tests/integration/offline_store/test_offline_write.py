import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from feast import FeatureView, Field
from feast.types import Float32, Int32
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import driver

# TODO(felixwang9817): Add a unit test that checks that write_to_offline_store can reorder columns.
# This should only happen after https://github.com/feast-dev/feast/issues/2797 is fixed.


@pytest.mark.integration
@pytest.mark.universal_offline_stores
def test_writing_incorrect_schema_fails(environment, universal_data_sources):
    """Tests that writing a dataframe with an incorrect schema fails."""
    store = environment.feature_store
    _, _, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)
    driver_fv = feature_views.driver
    store.apply([driver(), driver_fv])

    now = datetime.utcnow()
    ts = pd.Timestamp(now).round("ms")

    expected_df = pd.DataFrame.from_dict(
        {
            "event_timestamp": [ts - timedelta(hours=3), ts],
            "driver_id": [1001, 1002],
            "conv_rate": [random.random(), random.random()],
            "incorrect_schema": [random.randint(0, 10), random.randint(0, 10)],
            "created": [ts, ts],
        },
    )
    with pytest.raises(ValueError):
        store.write_to_offline_store(
            driver_fv.name, expected_df, allow_registry_cache=False
        )


@pytest.mark.integration
@pytest.mark.universal_offline_stores
def test_writing_consecutively_to_offline_store(environment, universal_data_sources):
    store = environment.feature_store
    _, _, data_sources = universal_data_sources
    driver_entity = driver()
    driver_stats = FeatureView(
        name="driver_stats",
        entities=[driver_entity],
        schema=[
            Field(name="avg_daily_trips", dtype=Int32),
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
        ],
        source=data_sources.driver,
        ttl=timedelta(
            minutes=10
        ),  # This is to make sure all offline store data is out of date since get_historical_features() only searches backwards for a ttl window.
    )

    now = datetime.utcnow()
    ts = pd.Timestamp(now, unit="ns")

    entity_df = pd.DataFrame.from_dict(
        {
            "driver_id": [1001, 1001],
            "event_timestamp": [ts - timedelta(hours=4), ts - timedelta(hours=3)],
        }
    )

    store.apply([driver_entity, driver_stats])
    df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_stats:conv_rate",
            "driver_stats:acc_rate",
            "driver_stats:avg_daily_trips",
        ],
        full_feature_names=False,
    ).to_df()

    assert df["conv_rate"].isnull().all()
    assert df["acc_rate"].isnull().all()
    assert df["avg_daily_trips"].isnull().all()

    first_df = pd.DataFrame.from_dict(
        {
            "event_timestamp": [ts - timedelta(hours=4), ts - timedelta(hours=3)],
            "driver_id": [1001, 1001],
            "conv_rate": [random.random(), random.random()],
            "acc_rate": [random.random(), random.random()],
            "avg_daily_trips": [random.randint(0, 10), random.randint(0, 10)],
            "created": [ts, ts],
        },
    )
    store.write_to_offline_store(
        driver_stats.name, first_df, allow_registry_cache=False
    )

    after_write_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_stats:conv_rate",
            "driver_stats:acc_rate",
            "driver_stats:avg_daily_trips",
        ],
        full_feature_names=False,
    ).to_df()

    assert len(after_write_df) == len(first_df)
    assert np.where(
        after_write_df["conv_rate"].reset_index(drop=True)
        == first_df["conv_rate"].reset_index(drop=True)
    )
    assert np.where(
        after_write_df["acc_rate"].reset_index(drop=True)
        == first_df["acc_rate"].reset_index(drop=True)
    )
    assert np.where(
        after_write_df["avg_daily_trips"].reset_index(drop=True)
        == first_df["avg_daily_trips"].reset_index(drop=True)
    )

    second_df = pd.DataFrame.from_dict(
        {
            "event_timestamp": [ts - timedelta(hours=1), ts],
            "driver_id": [1001, 1001],
            "conv_rate": [random.random(), random.random()],
            "acc_rate": [random.random(), random.random()],
            "avg_daily_trips": [random.randint(0, 10), random.randint(0, 10)],
            "created": [ts, ts],
        },
    )

    store.write_to_offline_store(
        driver_stats.name, second_df, allow_registry_cache=False
    )

    entity_df = pd.DataFrame.from_dict(
        {
            "driver_id": [1001, 1001, 1001, 1001],
            "event_timestamp": [
                ts - timedelta(hours=4),
                ts - timedelta(hours=3),
                ts - timedelta(hours=1),
                ts,
            ],
        }
    )

    after_write_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_stats:conv_rate",
            "driver_stats:acc_rate",
            "driver_stats:avg_daily_trips",
        ],
        full_feature_names=False,
    ).to_df()

    expected_df = pd.concat([first_df, second_df])
    assert len(after_write_df) == len(expected_df)
    assert np.where(
        after_write_df["conv_rate"].reset_index(drop=True)
        == expected_df["conv_rate"].reset_index(drop=True)
    )
    assert np.where(
        after_write_df["acc_rate"].reset_index(drop=True)
        == expected_df["acc_rate"].reset_index(drop=True)
    )
    assert np.where(
        after_write_df["avg_daily_trips"].reset_index(drop=True)
        == expected_df["avg_daily_trips"].reset_index(drop=True)
    )
