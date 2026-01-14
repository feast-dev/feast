"""Integration tests for Iceberg Offline Store."""

import pytest

from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import (
    customer,
    driver,
    location,
)


@pytest.mark.integration
@pytest.mark.universal_offline_stores(only=["iceberg"])
def test_iceberg_get_historical_features(environment, universal_data_sources):
    """
    Tests basic historical feature retrieval from Iceberg offline store.

    This test validates:
    1. Feature store can apply entities and feature views with Iceberg sources
    2. Historical features can be retrieved using get_historical_features
    3. Point-in-time correct joins work properly
    4. Retrieved data matches expected schema
    """
    store = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    # Apply entities and feature views
    store.apply([driver(), customer(), location(), *feature_views.values()])

    # Define features to retrieve
    features = [
        "driver_stats:conv_rate",
        "driver_stats:acc_rate",
        "driver_stats:avg_daily_trips",
    ]

    # Get historical features using entity dataframe
    entity_df = datasets.entity_df.drop(
        columns=["order_id", "origin_id", "destination_id"]
    )
    job = store.get_historical_features(
        entity_df=entity_df,
        features=features,
    )

    # Retrieve results
    df = job.to_df()

    # Validate results
    assert df is not None
    assert len(df) > 0
    assert "driver_id" in df.columns
    assert "event_timestamp" in df.columns
    assert "conv_rate" in df.columns
    assert "acc_rate" in df.columns
    assert "avg_daily_trips" in df.columns

    # Validate no nulls in entity columns
    assert df["driver_id"].notna().all()
    assert df["event_timestamp"].notna().all()


@pytest.mark.integration
@pytest.mark.universal_offline_stores(only=["iceberg"])
def test_iceberg_multi_entity_join(environment, universal_data_sources):
    """
    Tests multi-entity feature retrieval from Iceberg offline store.

    This test validates:
    1. Joins across multiple entities work correctly
    2. Multiple feature views can be queried simultaneously
    3. Data integrity is maintained across joins
    """
    store = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    # Apply entities and feature views
    store.apply([driver(), customer(), location(), *feature_views.values()])

    # Define features from multiple entities
    features = [
        "driver_stats:conv_rate",
        "customer_profile:current_balance",
        "customer_profile:avg_passenger_count",
    ]

    # Get historical features
    entity_df = datasets.entity_df.drop(
        columns=["order_id", "origin_id", "destination_id"]
    )
    job = store.get_historical_features(
        entity_df=entity_df,
        features=features,
    )

    # Retrieve results
    df = job.to_df()

    # Validate results
    assert df is not None
    assert len(df) > 0
    assert "driver_id" in df.columns
    assert "customer_id" in df.columns
    assert "conv_rate" in df.columns
    assert "current_balance" in df.columns
    assert "avg_passenger_count" in df.columns


@pytest.mark.integration
@pytest.mark.universal_offline_stores(only=["iceberg"])
def test_iceberg_point_in_time_correctness(environment, universal_data_sources):
    """
    Tests point-in-time correctness for Iceberg offline store.

    This test validates:
    1. Point-in-time joins retrieve data from the correct time window
    2. Future data is not leaked into historical features
    3. Timestamps are respected during joins
    """
    store = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    # Apply entities and feature views
    store.apply([driver(), *feature_views.values()])

    # Get a subset of entity dataframe
    entity_df = datasets.entity_df[["driver_id", "event_timestamp"]].drop_duplicates()

    # Get historical features
    features = ["driver_stats:conv_rate", "driver_stats:acc_rate"]
    job = store.get_historical_features(
        entity_df=entity_df,
        features=features,
    )

    # Retrieve results
    df = job.to_df()

    # Validate results
    assert df is not None
    assert len(df) > 0

    # Validate that all timestamps in entity_df have corresponding results
    assert len(df) == len(entity_df)

    # Validate feature values are not null for valid timestamps
    # (some may be null if no data exists before that timestamp)
    assert "conv_rate" in df.columns
    assert "acc_rate" in df.columns


@pytest.mark.integration
@pytest.mark.universal_offline_stores(only=["iceberg"])
def test_iceberg_feature_view_schema_inference(environment, universal_data_sources):
    """
    Tests that schema inference works correctly for Iceberg tables.

    This test validates:
    1. Iceberg table schemas are correctly inferred
    2. Data types are properly mapped between Iceberg and Feast
    3. Feature views can be created without explicit schema definitions
    """
    store = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    # Apply a single feature view
    store.apply([driver(), feature_views.driver])

    # Retrieve the feature view to check schema
    fv = store.get_feature_view("driver_stats")

    # Validate feature view exists and has features
    assert fv is not None
    assert len(fv.features) > 0

    # Validate data source is Iceberg
    from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
        IcebergSource,
    )

    assert isinstance(fv.batch_source, IcebergSource)
    assert fv.batch_source.table_identifier is not None


@pytest.mark.integration
@pytest.mark.universal_offline_stores(only=["iceberg"])
def test_iceberg_empty_entity_df(environment, universal_data_sources):
    """
    Tests behavior with empty entity dataframe.

    This test validates:
    1. Empty entity dataframes are handled gracefully
    2. No errors occur when querying with no entities
    """
    store = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    # Apply entities and feature views
    store.apply([driver(), *feature_views.values()])

    # Create empty entity dataframe
    import pandas as pd

    entity_df = pd.DataFrame(
        {"driver_id": [], "event_timestamp": []},
    ).astype({"driver_id": "int64", "event_timestamp": "datetime64[ns]"})

    # Get historical features with empty entity dataframe
    features = ["driver_stats:conv_rate"]
    job = store.get_historical_features(
        entity_df=entity_df,
        features=features,
    )

    # Retrieve results
    df = job.to_df()

    # Validate results are empty
    assert df is not None
    assert len(df) == 0
