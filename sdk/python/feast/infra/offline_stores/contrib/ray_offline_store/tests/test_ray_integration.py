import pandas as pd
import pytest

from feast.utils import _utc_now
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import driver


@pytest.mark.integration
@pytest.mark.universal_offline_stores
def test_ray_offline_store_basic_write_and_read(environment, universal_data_sources):
    """Test basic write and read functionality with Ray offline store."""
    store = environment.feature_store
    _, _, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)
    driver_fv = feature_views.driver
    store.apply([driver(), driver_fv])

    now = _utc_now()
    ts = pd.Timestamp(now).round("ms")

    # Write data to offline store
    df_to_write = pd.DataFrame.from_dict(
        {
            "event_timestamp": [ts, ts],
            "driver_id": [1001, 1002],
            "conv_rate": [0.1, 0.2],
            "acc_rate": [0.9, 0.8],
            "avg_daily_trips": [10, 20],
            "created": [ts, ts],
        },
    )

    store.write_to_offline_store(
        driver_fv.name, df_to_write, allow_registry_cache=False
    )

    # Read data back
    entity_df = pd.DataFrame({"driver_id": [1001, 1002], "event_timestamp": [ts, ts]})

    result_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_stats:conv_rate",
            "driver_stats:acc_rate",
            "driver_stats:avg_daily_trips",
        ],
        full_feature_names=False,
    ).to_df()

    assert len(result_df) == 2
    assert "conv_rate" in result_df.columns
    assert "acc_rate" in result_df.columns
    assert "avg_daily_trips" in result_df.columns


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: f"full:{v}")
def test_ray_offline_store_historical_features(
    environment, universal_data_sources, full_feature_names
):
    """Test historical features retrieval with Ray offline store."""
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    entity_df_with_request_data = datasets.entity_df.copy(deep=True)
    entity_df_with_request_data["val_to_add"] = [
        i for i in range(len(entity_df_with_request_data))
    ]

    store.apply(
        [
            driver(),
            *feature_views.values(),
        ]
    )

    job = store.get_historical_features(
        entity_df=entity_df_with_request_data,
        features=[
            "driver_stats:conv_rate",
            "driver_stats:avg_daily_trips",
            "customer_profile:current_balance",
            "conv_rate_plus_100:conv_rate_plus_100",
        ],
        full_feature_names=full_feature_names,
    )

    # Test DataFrame conversion
    result_df = job.to_df()
    assert len(result_df) > 0
    assert "event_timestamp" in result_df.columns

    # Test Arrow conversion
    result_table = job.to_arrow().to_pandas()
    assert len(result_table) > 0
    assert "event_timestamp" in result_table.columns


@pytest.mark.integration
@pytest.mark.universal_offline_stores
def test_ray_offline_store_persist(environment, universal_data_sources):
    """Test dataset persistence with Ray offline store."""
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    entity_df_with_request_data = datasets.entity_df.copy(deep=True)
    entity_df_with_request_data["val_to_add"] = [
        i for i in range(len(entity_df_with_request_data))
    ]

    store.apply(
        [
            driver(),
            *feature_views.values(),
        ]
    )

    job = store.get_historical_features(
        entity_df=entity_df_with_request_data,
        features=[
            "driver_stats:conv_rate",
            "customer_profile:current_balance",
        ],
        full_feature_names=False,
    )

    # Test persisting the dataset
    from feast.saved_dataset import SavedDatasetFileStorage

    storage = SavedDatasetFileStorage(path="data/test_saved_dataset.parquet")
    saved_path = job.persist(storage, allow_overwrite=True)

    assert saved_path == "data/test_saved_dataset.parquet"

    # Verify the saved dataset exists
    import os

    assert os.path.exists(saved_path)
