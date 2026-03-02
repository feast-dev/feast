from datetime import timedelta

import pandas as pd
import pytest

from feast.utils import _utc_now
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import driver


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.ray_offline_stores_only
def test_non_entity_mode_basic(environment, universal_data_sources):
    """Test historical features retrieval without entity_df (non-entity mode).

    This tests the basic functionality where entity_df=None and start_date/end_date
    are provided to retrieve all features within the time range.
    """
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    store.apply(
        [
            driver(),
            feature_views.driver,
        ]
    )

    # Use the environment's start and end dates for the query
    start_date = environment.start_date
    end_date = environment.end_date

    # Non-entity mode: entity_df=None with start_date and end_date
    result_df = store.get_historical_features(
        entity_df=None,
        features=[
            "driver_stats:conv_rate",
            "driver_stats:acc_rate",
            "driver_stats:avg_daily_trips",
        ],
        full_feature_names=False,
        start_date=start_date,
        end_date=end_date,
    ).to_df()

    # Verify data was retrieved
    assert len(result_df) > 0, "Non-entity mode should return data"
    assert "conv_rate" in result_df.columns
    assert "acc_rate" in result_df.columns
    assert "avg_daily_trips" in result_df.columns
    assert "event_timestamp" in result_df.columns
    assert "driver_id" in result_df.columns

    # Verify timestamps are within the requested range
    result_df["event_timestamp"] = pd.to_datetime(
        result_df["event_timestamp"], utc=True
    )
    assert (result_df["event_timestamp"] >= start_date).all()
    assert (result_df["event_timestamp"] <= end_date).all()


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.ray_offline_stores_only
def test_non_entity_mode_preserves_multiple_timestamps(
    environment, universal_data_sources
):
    """Test that non-entity mode preserves multiple transactions per entity ID.

    This is a regression test for the fix that ensures distinct (entity_key, event_timestamp)
    combinations are preserved, not just distinct entity keys. This is critical for
    proper point-in-time joins when an entity has multiple transactions.
    """
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    store.apply(
        [
            driver(),
            feature_views.driver,
        ]
    )

    now = _utc_now()
    ts1 = pd.Timestamp(now - timedelta(hours=2)).round("ms")
    ts2 = pd.Timestamp(now - timedelta(hours=1)).round("ms")
    ts3 = pd.Timestamp(now).round("ms")

    # Write data with multiple timestamps for the same entity (driver_id=9001)
    df_to_write = pd.DataFrame.from_dict(
        {
            "event_timestamp": [ts1, ts2, ts3],
            "driver_id": [9001, 9001, 9001],  # Same entity, different timestamps
            "conv_rate": [0.1, 0.2, 0.3],
            "acc_rate": [0.9, 0.8, 0.7],
            "avg_daily_trips": [10, 20, 30],
            "driver_metadata": [None, None, None],
            "driver_config": [None, None, None],
            "driver_profile": [None, None, None],
            "created": [ts1, ts2, ts3],
        },
    )

    store.write_to_offline_store(
        feature_views.driver.name, df_to_write, allow_registry_cache=False
    )

    # Query without entity_df - should get all 3 rows for driver_id=9001
    result_df = store.get_historical_features(
        entity_df=None,
        features=[
            "driver_stats:conv_rate",
            "driver_stats:acc_rate",
        ],
        full_feature_names=False,
        start_date=ts1 - timedelta(minutes=1),
        end_date=ts3 + timedelta(minutes=1),
    ).to_df()

    # Filter to just our test entity
    result_df = result_df[result_df["driver_id"] == 9001]

    # Verify we got all 3 rows with different timestamps (not just 1 row)
    assert len(result_df) == 3, (
        f"Expected 3 rows for driver_id=9001 (one per timestamp), got {len(result_df)}"
    )

    # Verify the feature values are correct for each timestamp
    result_df = result_df.sort_values("event_timestamp").reset_index(drop=True)
    assert list(result_df["conv_rate"]) == [0.1, 0.2, 0.3]
    assert list(result_df["acc_rate"]) == [0.9, 0.8, 0.7]
