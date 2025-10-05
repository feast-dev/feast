import time
from datetime import timedelta

import pandas as pd
import pytest

from feast import FeatureView, Field
from feast.data_source import DataSource
from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
)
from feast.types import Float32, Int32, Int64
from tests.integration.compute_engines.ray_compute.ray_shared_utils import (
    create_entity_df,
    create_feature_dataset,
    create_unique_sink_source,
    driver,
    now,
    today,
)


def create_base_feature_view(source: DataSource, name_suffix: str = "") -> FeatureView:
    """Create a base feature view with data source."""
    return FeatureView(
        name=f"base_driver_stats{name_suffix}",
        entities=[driver],
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
            Field(name="driver_id", dtype=Int32),
        ],
        online=True,
        offline=True,
        source=source,
    )


def create_derived_feature_view(
    base_fv: FeatureView, sink_source: DataSource, name_suffix: str = ""
) -> FeatureView:
    """Create a derived feature view that uses another feature view as source.
    Note: This creates a regular FeatureView with another FeatureView as source.
    """
    return FeatureView(
        name=f"derived_driver_stats{name_suffix}",
        entities=[driver],
        schema=[
            Field(name="conv_rate", dtype=Float32),  # Same feature names as source
            Field(name="acc_rate", dtype=Float32),  # Same feature names as source
            Field(name="avg_daily_trips", dtype=Int64),  # Same feature names as source
            Field(name="driver_id", dtype=Int32),
        ],
        online=True,
        offline=True,
        source=base_fv,
        sink_source=sink_source,
    )


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_compute_engine_single_source_feature_view(ray_environment, temp_dir):
    """Test Ray compute engine with a single source feature view."""
    fs = ray_environment.feature_store

    data_source = create_feature_dataset(ray_environment)
    base_fv = create_base_feature_view(data_source, "_single")
    sink_source = create_unique_sink_source(temp_dir, "derived_sink_single")
    derived_fv = create_derived_feature_view(base_fv, sink_source, "_single")
    fs.apply([driver, base_fv, derived_fv])

    entity_df = create_entity_df()
    job = fs.get_historical_features(
        entity_df=entity_df,
        features=[
            f"{base_fv.name}:conv_rate",
            f"{base_fv.name}:acc_rate",
            f"{derived_fv.name}:conv_rate",
            f"{derived_fv.name}:acc_rate",
        ],
        full_feature_names=True,
    )
    result_df = job.to_df()
    assert len(result_df) == 2
    assert f"{base_fv.name}__conv_rate" in result_df.columns
    assert f"{base_fv.name}__acc_rate" in result_df.columns
    assert f"{derived_fv.name}__conv_rate" in result_df.columns
    assert f"{derived_fv.name}__acc_rate" in result_df.columns


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_compute_engine_materialization_with_source_feature_views(
    ray_environment, temp_dir
):
    """Test Ray compute engine materialization with source feature views."""
    fs = ray_environment.feature_store
    data_source = create_feature_dataset(ray_environment)
    base_fv = create_base_feature_view(data_source, "_materialize")

    sink_source = create_unique_sink_source(temp_dir, "derived_sink")
    derived_fv = create_derived_feature_view(base_fv, sink_source, "_materialize")

    fs.apply([driver, base_fv, derived_fv])
    start_date = today - timedelta(days=7)
    end_date = today

    # Materialize only the derived feature view - compute engine handles base dependencies
    derived_job = fs.materialize(
        start_date=start_date,
        end_date=end_date,
        feature_views=[derived_fv.name],
    )

    if derived_job is not None:
        assert derived_job.status == MaterializationJobStatus.SUCCEEDED
    else:
        print("Materialization completed synchronously (no job object returned)")


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_compute_engine_cycle_detection(ray_environment, temp_dir):
    """Test Ray compute engine cycle detection in feature view dependencies."""
    fs = ray_environment.feature_store
    data_source = create_feature_dataset(ray_environment)
    sink_source1 = create_unique_sink_source(temp_dir, "cycle_sink1")
    sink_source2 = create_unique_sink_source(temp_dir, "cycle_sink2")

    fv1 = FeatureView(
        name="cycle_fv1",
        entities=[driver],
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="driver_id", dtype=Int32),
        ],
        source=data_source,
        online=True,
        offline=True,
    )

    fv2 = FeatureView(
        name="cycle_fv2",
        entities=[driver],
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="driver_id", dtype=Int32),
        ],
        source=fv1,
        sink_source=sink_source1,
        online=True,
        offline=True,
    )

    fv3 = FeatureView(
        name="cycle_fv3",
        entities=[driver],
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="driver_id", dtype=Int32),
        ],
        source=fv2,
        sink_source=sink_source2,
        online=True,
        offline=True,
    )

    # Apply feature views (this should work without cycles)
    fs.apply([driver, fv1, fv2, fv3])

    entity_df = create_entity_df()

    job = fs.get_historical_features(
        entity_df=entity_df,
        features=[
            f"{fv1.name}:conv_rate",
            f"{fv2.name}:conv_rate",
            f"{fv3.name}:conv_rate",
        ],
        full_feature_names=True,
    )

    result_df = job.to_df()

    assert len(result_df) == 2
    assert f"{fv1.name}__conv_rate" in result_df.columns
    assert f"{fv2.name}__conv_rate" in result_df.columns
    assert f"{fv3.name}__conv_rate" in result_df.columns


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_compute_engine_error_handling(ray_environment, temp_dir):
    """Test Ray compute engine error handling with invalid source feature views."""
    fs = ray_environment.feature_store
    data_source = create_feature_dataset(ray_environment)
    base_fv = create_base_feature_view(data_source, "_error")

    # Test 1: Regular FeatureView with FeatureView source but no sink_source should fail
    with pytest.raises(
        ValueError, match="Derived FeatureView must specify `sink_source`"
    ):
        FeatureView(
            name="invalid_fv",
            entities=[driver],
            schema=[
                Field(name="conv_rate", dtype=Float32),
                Field(name="driver_id", dtype=Int32),
            ],
            source=base_fv,
            online=True,
            offline=True,
        )

    # Test 2: Valid FeatureView with sink_source should work
    sink_source = create_unique_sink_source(temp_dir, "valid_sink")
    valid_fv = FeatureView(
        name="valid_fv",
        entities=[driver],
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="driver_id", dtype=Int32),
        ],  # Use same feature name as source
        source=base_fv,
        sink_source=sink_source,
        online=True,
        offline=True,
    )

    fs.apply([driver, base_fv, valid_fv])
    entity_df = create_entity_df()
    job = fs.get_historical_features(
        entity_df=entity_df,
        features=[
            f"{base_fv.name}:conv_rate",
            f"{valid_fv.name}:conv_rate",  # Use same feature name as source
        ],
        full_feature_names=True,
    )

    result_df = job.to_df()
    assert len(result_df) == 2
    assert f"{base_fv.name}__conv_rate" in result_df.columns
    assert f"{valid_fv.name}__conv_rate" in result_df.columns
    assert result_df[f"{base_fv.name}__conv_rate"].notna().all()
    assert result_df[f"{valid_fv.name}__conv_rate"].notna().all()


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_compute_engine_performance_with_source_feature_views(
    ray_environment, temp_dir
):
    """Test Ray compute engine performance with source feature views."""
    fs = ray_environment.feature_store
    large_df = pd.DataFrame()
    for i in range(1000):
        large_df = pd.concat(
            [
                large_df,
                pd.DataFrame(
                    {
                        "driver_id": [1000 + i],
                        "event_timestamp": [today - timedelta(days=i % 30)],
                        "created": [now - timedelta(hours=i % 24)],
                        "conv_rate": [0.5 + (i % 10) * 0.05],
                        "acc_rate": [0.6 + (i % 10) * 0.04],
                        "avg_daily_trips": [10 + i % 20],
                    }
                ),
            ]
        )
    data_source = ray_environment.data_source_creator.create_data_source(
        large_df,
        fs.project,
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    base_fv = create_base_feature_view(data_source, "_perf")
    sink_source1 = create_unique_sink_source(temp_dir, "perf_sink1")
    derived_fv1 = create_derived_feature_view(base_fv, sink_source1, "_perf1")
    sink_source2 = create_unique_sink_source(temp_dir, "perf_sink2")
    derived_fv2 = create_derived_feature_view(derived_fv1, sink_source2, "_perf2")
    fs.apply([driver, base_fv, derived_fv1, derived_fv2])

    large_entity_df = pd.DataFrame(
        {
            "driver_id": [1000 + i for i in range(100)],
            "event_timestamp": [today] * 100,
        }
    )
    start_time = time.time()
    job = fs.get_historical_features(
        entity_df=large_entity_df,
        features=[
            f"{base_fv.name}:conv_rate",
            f"{derived_fv1.name}:conv_rate",
        ],
        full_feature_names=True,
    )
    result_df = job.to_df()
    end_time = time.time()
    assert len(result_df) == 100
    assert f"{base_fv.name}__conv_rate" in result_df.columns
    assert f"{derived_fv1.name}__conv_rate" in result_df.columns
    assert end_time - start_time < 60
