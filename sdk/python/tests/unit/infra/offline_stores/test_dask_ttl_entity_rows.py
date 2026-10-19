from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import dask.dataframe as dd
import pandas as pd
import pytest

from feast import Entity, FeatureStore, FeatureView, Field, FileSource, RepoConfig
from feast.infra.offline_stores.dask import _filter_ttl
from feast.types import Float64, Int64, ValueType


def _store(tmp_path: Path) -> FeatureStore:
    return FeatureStore(
        config=RepoConfig(
            project="ttl_rows",
            registry=str(tmp_path / "registry.db"),
            provider="local",
            offline_store={"type": "file"},
            online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
        )
    )


def _view(
    tmp_path: Path,
    name: str,
    data: pd.DataFrame,
    ttl: Optional[timedelta],
) -> FeatureView:
    path = tmp_path / (name + ".parquet")
    data.to_parquet(path)
    return FeatureView(
        name=name,
        entities=[
            Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
        ],
        schema=[Field(name="value", dtype=Float64)],
        source=FileSource(
            path=str(path),
            timestamp_field="event_timestamp",
            created_timestamp_column="created_ts",
        ),
        ttl=ttl,
    )


def _source() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "driver_id": [1, 2, 3, 4, 1, 3],
            "event_timestamp": pd.to_datetime(
                [
                    "2025-01-01T00:00:00Z",  # Outside a one-day TTL.
                    "2025-01-04T00:00:00Z",  # After the request time.
                    "2025-01-02T12:00:00Z",
                    "2025-01-02T00:00:00Z",  # Exactly on the TTL boundary.
                    "2024-12-30T00:00:00Z",  # A second expired candidate.
                    "2025-01-05T00:00:00Z",  # A future candidate alongside a valid one.
                ]
            ),
            "created_ts": pd.to_datetime(["2025-01-01T00:00:00Z"] * 6),
            "value": [10.0, 20.0, 30.0, 40.0, 5.0, 99.0],
        }
    )


def _entities() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "driver_id": [1, 2, 3, 4, 5],
            "event_timestamp": pd.to_datetime(["2025-01-03T00:00:00Z"] * 5),
            "label": ["expired", "future", "valid", "boundary", "missing"],
        }
    )


@pytest.mark.parametrize("ttl", [timedelta(days=1), timedelta(0), None])
@pytest.mark.parametrize("full_feature_names", [False, True])
@pytest.mark.parametrize("filter_by_created_timestamp", [False, True])
def test_historical_retrieval_keeps_requests_without_eligible_features(
    tmp_path: Path,
    ttl: Optional[timedelta],
    full_feature_names: bool,
    filter_by_created_timestamp: bool,
) -> None:
    store = _store(tmp_path)
    view = _view(tmp_path, "stats", _source(), ttl)
    store.apply(
        [
            Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64),
            view,
        ]
    )
    entities = _entities()
    original = entities.copy(deep=True)

    result = (
        store.get_historical_features(
            entity_df=entities,
            features=["stats:value"],
            full_feature_names=full_feature_names,
            filter_by_created_timestamp=filter_by_created_timestamp,
        )
        .to_df()
        .sort_values("driver_id")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(result[list(entities.columns)], original)
    pd.testing.assert_frame_equal(entities, original)
    feature = "stats__value" if full_feature_names else "value"
    expected = [None if ttl else 10.0, None, 30.0, 40.0, None]
    pd.testing.assert_series_equal(
        result[feature], pd.Series(expected, name=feature), check_dtype=False
    )


def test_later_feature_view_preserves_previous_features_and_entity_columns(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    first = _view(tmp_path, "first", _source(), None)
    second_data = _source()
    second_data["event_timestamp"] = pd.to_datetime(["2025-01-01T00:00:00Z"] * 6)
    second = _view(tmp_path, "second", second_data, timedelta(days=1))
    store.apply(
        [
            Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64),
            first,
            second,
        ]
    )

    result = (
        store.get_historical_features(
            entity_df=_entities(),
            features=["first:value", "second:value"],
            full_feature_names=True,
        )
        .to_df()
        .sort_values("driver_id")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(result[list(_entities().columns)], _entities())
    pd.testing.assert_series_equal(
        result["first__value"],
        pd.Series([10.0, None, 30.0, 40.0, None], name="first__value"),
    )
    assert result["second__value"].isna().all()


def test_ttl_filter_does_not_widen_integer_feature_columns(tmp_path: Path) -> None:
    store = _store(tmp_path)
    data = _source()
    data["value"] = pd.Series(data["value"], dtype="int64")
    path = tmp_path / "integer_stats.parquet"
    data.to_parquet(path)
    view = FeatureView(
        name="integer_stats",
        entities=[
            Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
        ],
        schema=[Field(name="value", dtype=Int64)],
        source=FileSource(
            path=str(path),
            timestamp_field="event_timestamp",
            created_timestamp_column="created_ts",
        ),
        ttl=timedelta(days=1),
    )
    store.apply(
        [
            Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64),
            view,
        ]
    )

    result = (
        store.get_historical_features(
            entity_df=_entities(), features=["integer_stats:value"]
        )
        .to_df()
        .sort_values("driver_id")
        .reset_index(drop=True)
    )

    assert str(result["value"].dtype) == "Int64"
    assert result["value"].tolist() == [pd.NA, pd.NA, 30, 40, pd.NA]


def test_ttl_filter_preserves_plain_integer_dtype_when_all_requests_match(
    tmp_path: Path,
) -> None:
    data = _source().iloc[[2, 3]].copy()
    data["value"] = pd.Series(data["value"].to_numpy(), index=data.index, dtype="int64")
    data["entity_event_timestamp"] = pd.Timestamp("2025-01-03T00:00:00Z")
    view = FeatureView(
        name="all_integer_stats",
        entities=[
            Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
        ],
        schema=[Field(name="value", dtype=Int64)],
        source=FileSource(
            path=str(tmp_path / "unused.parquet"),
            timestamp_field="event_timestamp",
            created_timestamp_column="created_ts",
        ),
        ttl=timedelta(days=1),
    )
    result = _filter_ttl(
        dd.from_pandas(data, npartitions=1),
        view,
        "entity_event_timestamp",
        "event_timestamp",
        preserved_columns={"driver_id", "entity_event_timestamp"},
        request_columns=["driver_id", "entity_event_timestamp"],
    )
    result = result.compute().sort_values("driver_id").reset_index(drop=True)

    assert str(result["value"].dtype) == "int64"
    assert result["value"].tolist() == [30, 40]


def test_ttl_filter_ignores_unhashable_preserved_columns(tmp_path: Path) -> None:
    data = pd.DataFrame(
        {
            "driver_id": [1, 2],
            "entity_event_timestamp": pd.to_datetime(["2025-01-03T00:00:00Z"] * 2),
            "label": [["expired"], ["future"]],
            "event_timestamp": pd.to_datetime(
                ["2025-01-01T00:00:00Z", "2025-01-04T00:00:00Z"]
            ),
            "value": [10.0, 20.0],
        }
    )
    view = FeatureView(
        name="list_label_stats",
        entities=[
            Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
        ],
        schema=[Field(name="value", dtype=Float64)],
        source=FileSource(
            path=str(tmp_path / "unused.parquet"),
            timestamp_field="event_timestamp",
        ),
        ttl=timedelta(days=1),
    )

    result = _filter_ttl(
        dd.from_pandas(data, npartitions=1),
        view,
        "entity_event_timestamp",
        "event_timestamp",
        preserved_columns={"driver_id", "entity_event_timestamp", "label"},
        request_columns=["driver_id", "entity_event_timestamp"],
    ).compute()

    result = result.sort_values("driver_id").reset_index(drop=True)
    assert result["label"].tolist() == [["expired"], ["future"]]
    assert pd.isna(result["value"].iloc[0])
    assert pd.isna(result["value"].iloc[1])


def test_historical_retrieval_keeps_requests_from_multiple_parquet_files(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    data = _source()
    source_dir = tmp_path / "multi_file_stats"
    source_dir.mkdir()
    data.iloc[:3].to_parquet(source_dir / "part-0.parquet", index=False)
    data.iloc[3:].to_parquet(source_dir / "part-1.parquet", index=False)
    view = FeatureView(
        name="multi_file_stats",
        entities=[
            Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
        ],
        schema=[Field(name="value", dtype=Float64)],
        source=FileSource(
            path=str(source_dir),
            timestamp_field="event_timestamp",
            created_timestamp_column="created_ts",
        ),
        ttl=timedelta(days=1),
    )
    store.apply(
        [
            Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64),
            view,
        ]
    )

    result = (
        store.get_historical_features(
            entity_df=_entities(), features=["multi_file_stats:value"]
        )
        .to_df()
        .sort_values("driver_id")
        .reset_index(drop=True)
    )

    assert result["driver_id"].tolist() == [1, 2, 3, 4, 5]
    assert result["value"].isna().tolist() == [True, True, False, False, True]
    assert result["value"].iloc[2:4].tolist() == [30.0, 40.0]


def test_ttl_filter_handles_missing_requests_in_later_partitions(
    tmp_path: Path,
) -> None:
    data = pd.DataFrame(
        {
            "driver_id": [1, 2, 3, 4],
            "entity_event_timestamp": pd.to_datetime(["2025-01-03T00:00:00Z"] * 4),
            "event_timestamp": pd.to_datetime(
                [
                    "2025-01-01T00:00:00Z",
                    "2025-01-04T00:00:00Z",
                    "2025-01-02T12:00:00Z",
                    "2025-01-04T00:00:00Z",
                ]
            ),
            "created_ts": pd.to_datetime(["2025-01-01T00:00:00Z"] * 4),
            "value": [10.0, 20.0, 30.0, 40.0],
        }
    )
    view = FeatureView(
        name="partitioned_stats",
        entities=[
            Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
        ],
        schema=[Field(name="value", dtype=Float64)],
        source=FileSource(
            path=str(tmp_path / "unused.parquet"),
            timestamp_field="event_timestamp",
            created_timestamp_column="created_ts",
        ),
        ttl=timedelta(days=1),
    )

    result = _filter_ttl(
        dd.from_pandas(data, npartitions=2),
        view,
        "entity_event_timestamp",
        "event_timestamp",
        preserved_columns={"driver_id", "entity_event_timestamp"},
        request_columns=["driver_id", "entity_event_timestamp"],
    ).compute()

    result = result.sort_values("driver_id").reset_index(drop=True)
    assert result["driver_id"].tolist() == [1, 2, 3, 4]
    assert pd.isna(result["value"].iloc[[0, 1, 3]]).all()
    assert result["value"].iloc[2] == 30.0


def test_non_entity_snapshot_still_excludes_ineligible_entities(tmp_path: Path) -> None:
    store = _store(tmp_path)
    view = _view(tmp_path, "stats", _source(), timedelta(days=1))
    store.apply(
        [
            Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64),
            view,
        ]
    )

    result = store.get_historical_features(
        features=["stats:value"],
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2025, 1, 3, tzinfo=timezone.utc),
    ).to_df()

    assert set(result["driver_id"]) == {3, 4}
    assert set(result["value"]) == {30.0, 40.0}
