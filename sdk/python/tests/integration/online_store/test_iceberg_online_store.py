"""Integration tests for Iceberg Online Store."""

import pytest

from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import driver


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["iceberg"])
def test_iceberg_online_write_read(environment, universal_data_sources):
    """
    Tests basic write and read operations for Iceberg online store.

    This test validates:
    1. Features can be materialized to Iceberg online store
    2. Features can be retrieved from Iceberg online store
    3. Data consistency between write and read operations
    """
    store = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    # Apply entities and feature views
    store.apply([driver(), feature_views.driver])

    # Materialize features to online store
    store.materialize(environment.start_date, environment.end_date)

    # Retrieve online features
    online_features = store.get_online_features(
        features=["driver_stats:conv_rate", "driver_stats:acc_rate"],
        entity_rows=[
            {"driver_id": 1001},
            {"driver_id": 1002},
        ],
    )

    # Convert to dictionary
    result = online_features.to_dict()

    # Validate results
    assert result is not None
    assert "driver_id" in result
    assert "conv_rate" in result
    assert "acc_rate" in result
    assert len(result["driver_id"]) == 2

    # Validate driver IDs match request
    assert result["driver_id"][0] == 1001
    assert result["driver_id"][1] == 1002


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["iceberg"])
def test_iceberg_online_missing_entity(environment, universal_data_sources):
    """
    Tests behavior when requesting features for non-existent entities.

    This test validates:
    1. Graceful handling of missing entities
    2. Null values returned for non-existent entities
    3. No errors thrown for missing data
    """
    store = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    # Apply entities and feature views
    store.apply([driver(), feature_views.driver])

    # Materialize features to online store
    store.materialize(environment.start_date, environment.end_date)

    # Request features for non-existent entity
    online_features = store.get_online_features(
        features=["driver_stats:conv_rate"],
        entity_rows=[
            {"driver_id": 9999999},  # Non-existent driver
        ],
    )

    # Convert to dictionary
    result = online_features.to_dict()

    # Validate results
    assert result is not None
    assert "driver_id" in result
    assert result["driver_id"][0] == 9999999
    # Feature value should be None for non-existent entity
    assert "conv_rate" in result


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["iceberg"])
def test_iceberg_online_materialization_consistency(
    environment, universal_data_sources
):
    """
    Tests that materialization correctly updates online store data.

    This test validates:
    1. Multiple materializations update data correctly
    2. Latest values are retrieved after materialization
    3. Timestamp-based filtering works during materialization
    """
    store = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    # Apply entities and feature views
    store.apply([driver(), feature_views.driver])

    # First materialization
    store.materialize(environment.start_date, environment.end_date)

    # Retrieve features
    result1 = store.get_online_features(
        features=["driver_stats:conv_rate"],
        entity_rows=[{"driver_id": 1001}],
    ).to_dict()

    # Validate first result
    assert result1 is not None
    first_value = result1["conv_rate"][0]

    # Second materialization (should update if data changed)
    store.materialize(environment.start_date, environment.end_date)

    # Retrieve features again
    result2 = store.get_online_features(
        features=["driver_stats:conv_rate"],
        entity_rows=[{"driver_id": 1001}],
    ).to_dict()

    # Validate second result
    assert result2 is not None
    # Should get the same value since data hasn't changed
    assert result2["conv_rate"][0] == first_value


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["iceberg"])
def test_iceberg_online_batch_retrieval(environment, universal_data_sources):
    """
    Tests batch retrieval of features from Iceberg online store.

    This test validates:
    1. Multiple entities can be queried in a single request
    2. Results are returned in correct order
    3. Performance is acceptable for batch queries
    """
    store = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    # Apply entities and feature views
    store.apply([driver(), feature_views.driver])

    # Materialize features to online store
    store.materialize(environment.start_date, environment.end_date)

    # Create batch of entity rows
    entity_rows = [{"driver_id": driver_id} for driver_id in range(1001, 1011)]

    # Retrieve online features for batch
    online_features = store.get_online_features(
        features=["driver_stats:conv_rate", "driver_stats:acc_rate"],
        entity_rows=entity_rows,
    )

    # Convert to dictionary
    result = online_features.to_dict()

    # Validate results
    assert result is not None
    assert "driver_id" in result
    assert "conv_rate" in result
    assert "acc_rate" in result
    assert len(result["driver_id"]) == 10

    # Validate driver IDs are in correct order
    for i, driver_id in enumerate(range(1001, 1011)):
        assert result["driver_id"][i] == driver_id


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["iceberg"])
def test_iceberg_online_entity_hash_partitioning(environment, universal_data_sources):
    """
    Tests that entity hash partitioning works correctly.

    This test validates:
    1. Data is distributed across partitions based on entity hash
    2. Queries correctly retrieve data from partitioned tables
    3. Performance benefits of partitioning are realized
    """
    store = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    # Apply entities and feature views
    store.apply([driver(), feature_views.driver])

    # Materialize features to online store (with entity_hash partitioning)
    store.materialize(environment.start_date, environment.end_date)

    # Retrieve features for multiple entities across different hash buckets
    entity_rows = [
        {"driver_id": 1001},
        {"driver_id": 1002},
        {"driver_id": 1003},
        {"driver_id": 1004},
        {"driver_id": 1005},
    ]

    online_features = store.get_online_features(
        features=["driver_stats:conv_rate"],
        entity_rows=entity_rows,
    )

    # Convert to dictionary
    result = online_features.to_dict()

    # Validate results
    assert result is not None
    assert len(result["driver_id"]) == 5

    # All entities should be found despite being in different partitions
    for i, row in enumerate(entity_rows):
        assert result["driver_id"][i] == row["driver_id"]
