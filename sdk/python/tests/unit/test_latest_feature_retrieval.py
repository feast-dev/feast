
from feast.feature_store import FeatureStore


def test_feature_store_exposes_latest_features_api():
    """The public FeatureStore should expose latest-only retrieval."""
    assert hasattr(FeatureStore, "get_latest_features"), (
        "FeatureStore does not expose get_latest_features()"
    )


def test_get_latest_features_returns_latest_value_per_entity(tmp_path):
    from datetime import datetime, timedelta, timezone

    import pandas as pd

    from feast import Entity, FeatureView, Field, FileSource, RepoConfig
    from feast.types import Float64, ValueType

    source = pd.DataFrame(
        {
            "driver_id": [1, 1, 2, 2],
            "event_timestamp": pd.to_datetime(
                [
                    "2025-01-01T00:00:00Z",
                    "2025-01-03T00:00:00Z",
                    "2025-01-02T00:00:00Z",
                    "2025-01-04T00:00:00Z",
                ]
            ),
            "value": [10.0, 30.0, 20.0, 40.0],
        }
    )

    source_path = tmp_path / "driver_stats.parquet"
    source.to_parquet(source_path)

    store = FeatureStore(
        config=RepoConfig(
            project="latest_features_test",
            registry=str(tmp_path / "registry.db"),
            provider="local",
            offline_store={"type": "file"},
            online_store={
                "type": "sqlite",
                "path": str(tmp_path / "online.db"),
            },
        )
    )

    driver = Entity(
        name="driver",
        join_keys=["driver_id"],
        value_type=ValueType.INT64,
    )

    feature_view = FeatureView(
        name="driver_stats",
        entities=[driver],
        schema=[Field(name="value", dtype=Float64)],
        source=FileSource(
            path=str(source_path),
            timestamp_field="event_timestamp",
        ),
        ttl=timedelta(days=7),
    )

    store.apply([driver, feature_view])

    result = (
        store.get_latest_features(
            features=["driver_stats:value"],
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 1, 4, tzinfo=timezone.utc),
        )
        .to_df()
        .sort_values("driver_id")
        .reset_index(drop=True)
    )

    assert result["driver_id"].tolist() == [1, 2]
    assert result["value"].tolist() == [30.0, 40.0]


def test_get_latest_features_respects_end_date(tmp_path):
    """Return the latest value per entity on or before the cutoff."""
    from datetime import datetime, timedelta, timezone

    import pandas as pd

    from feast import Entity, FeatureView, Field, FileSource, RepoConfig
    from feast.types import Float64, ValueType

    source = pd.DataFrame(
        {
            "driver_id": [1, 1, 2, 2],
            "event_timestamp": pd.to_datetime(
                [
                    "2025-01-01T00:00:00Z",
                    "2025-01-03T00:00:00Z",
                    "2025-01-02T00:00:00Z",
                    "2025-01-04T00:00:00Z",
                ]
            ),
            "value": [10.0, 30.0, 20.0, 40.0],
        }
    )

    source_path = tmp_path / "driver_stats.parquet"
    source.to_parquet(source_path)

    store = FeatureStore(
        config=RepoConfig(
            project="latest_features_test",
            registry=str(tmp_path / "registry.db"),
            provider="local",
            offline_store={"type": "file"},
            online_store={
                "type": "sqlite",
                "path": str(tmp_path / "online.db"),
            },
        )
    )

    driver = Entity(
        name="driver",
        join_keys=["driver_id"],
        value_type=ValueType.INT64,
    )

    feature_view = FeatureView(
        name="driver_stats",
        entities=[driver],
        schema=[Field(name="value", dtype=Float64)],
        source=FileSource(
            path=str(source_path),
            timestamp_field="event_timestamp",
        ),
        ttl=timedelta(days=7),
    )

    store.apply([driver, feature_view])

    result = (
        store.get_latest_features(
            features=["driver_stats:value"],
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 1, 2, tzinfo=timezone.utc),
        )
        .to_df()
        .sort_values("driver_id")
        .reset_index(drop=True)
    )

    assert result["driver_id"].tolist() == [1, 2]
    assert result["value"].tolist() == [10.0, 20.0]
