from feast.feature_store import FeatureStore


def test_feature_store_exposes_latest_features_api():
    """The public FeatureStore should expose latest-only retrieval."""
    assert hasattr(FeatureStore, "get_latest_features"), (
        "FeatureStore does not expose get_latest_features()"
    )


def test_get_latest_features_returns_latest_value_per_entity(tmp_path, monkeypatch):
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

    # Check default dates passed to the offline store without querying data.
    provider = store._get_provider()
    captured = []

    def capture_retrieval(**kwargs):
        captured.append(kwargs)
        return None

    monkeypatch.setattr(
        provider.offline_store,
        "pull_latest_from_table_or_query",
        capture_retrieval,
    )
    monkeypatch.setattr(store, "_get_provider", lambda: provider)

    before = datetime.now(timezone.utc)
    store.get_latest_features(features=["driver_stats:value"])
    after = datetime.now(timezone.utc)

    defaults = captured[-1]
    assert before <= defaults["end_date"] <= after
    assert defaults["end_date"].tzinfo is not None
    assert defaults["start_date"] == defaults["end_date"] - timedelta(weeks=52)

    explicit_start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    explicit_end = datetime(2025, 1, 4, tzinfo=timezone.utc)

    store.get_latest_features(
        features=["driver_stats:value"],
        start_date=explicit_start,
        end_date=explicit_end,
    )

    assert captured[-1]["start_date"] == explicit_start
    assert captured[-1]["end_date"] == explicit_end


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


def test_get_latest_features_rejects_unknown_feature(tmp_path):
    """Reject requests for features absent from a registered feature view."""
    from datetime import timedelta

    import pandas as pd
    import pytest

    from feast import Entity, FeatureView, Field, FileSource, RepoConfig
    from feast.types import Float64, ValueType

    source_path = tmp_path / "driver_stats.parquet"
    pd.DataFrame(
        {
            "driver_id": [1],
            "event_timestamp": pd.to_datetime(["2025-01-01T00:00:00Z"]),
            "value": [10.0],
        }
    ).to_parquet(source_path)

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

    with pytest.raises(ValueError, match="Unknown features requested"):
        store.get_latest_features(features=["driver_stats:unknown_value"])


def test_get_latest_features_rejects_multiple_feature_views(tmp_path):
    """Reject multiple feature views until multi-view retrieval is supported."""
    from datetime import timedelta

    import pandas as pd
    import pytest

    from feast import Entity, FeatureView, Field, FileSource, RepoConfig
    from feast.types import Float64, ValueType

    source_path = tmp_path / "driver_stats.parquet"
    pd.DataFrame(
        {
            "driver_id": [1],
            "event_timestamp": pd.to_datetime(["2025-01-01T00:00:00Z"]),
            "value": [10.0],
            "extra_value": [20.0],
        }
    ).to_parquet(source_path)

    store = FeatureStore(
        config=RepoConfig(
            project="latest_features_test",
            registry=str(tmp_path / "registry.db"),
            provider="local",
            offline_store={"type": "file"},
            online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
        )
    )

    driver = Entity(
        name="driver",
        join_keys=["driver_id"],
        value_type=ValueType.INT64,
    )

    source = FileSource(
        path=str(source_path),
        timestamp_field="event_timestamp",
    )

    first_view = FeatureView(
        name="driver_stats",
        entities=[driver],
        schema=[Field(name="value", dtype=Float64)],
        source=source,
        ttl=timedelta(days=7),
    )

    second_view = FeatureView(
        name="driver_stats_extra",
        entities=[driver],
        schema=[Field(name="extra_value", dtype=Float64)],
        source=source,
        ttl=timedelta(days=7),
    )

    store.apply([driver, first_view, second_view])

    with pytest.raises(ValueError, match="exactly one regular feature view"):
        store.get_latest_features(
            features=[
                "driver_stats:value",
                "driver_stats_extra:extra_value",
            ]
        )
