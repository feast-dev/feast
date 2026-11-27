from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from feast import Entity, FeatureStore, FeatureView, Field, FileSource, RepoConfig
from feast.types import Int64, ValueType


def _store(tmp_path: Path, timestamp_fields: dict[str, str]) -> FeatureStore:
    entity = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    feature_views = []
    for name, timestamp_field in timestamp_fields.items():
        source_path = tmp_path / f"{name}.parquet"
        pd.DataFrame(
            {
                "driver_id": [1, 1, 1],
                timestamp_field: pd.to_datetime(
                    [
                        "2025-01-01T00:00:00Z",
                        "2025-01-02T00:00:00Z",
                        "2025-01-03T00:00:00Z",
                    ]
                ),
                name: [10, 20, 30],
            }
        ).to_parquet(source_path, index=False)
        feature_views.append(
            FeatureView(
                name=f"{name}_view",
                entities=[entity],
                schema=[
                    Field(name="driver_id", dtype=Int64),
                    Field(name=name, dtype=Int64),
                ],
                source=FileSource(
                    path=str(source_path), timestamp_field=timestamp_field
                ),
                ttl=timedelta(days=7),
            )
        )
    store = FeatureStore(
        config=RepoConfig(
            project="timestamp_collision",
            provider="local",
            registry=str(tmp_path / "registry.db"),
            online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
            offline_store={"type": "file"},
            repo_path=tmp_path,
        )
    )
    store.apply([entity, *feature_views])
    return store


@pytest.mark.parametrize("full_feature_names", [False, True])
@pytest.mark.parametrize("filter_by_created_timestamp", [False, True])
@pytest.mark.parametrize(
    "request_value",
    [
        pd.Timestamp("2025-01-10T00:00:00Z"),
        pd.Timestamp("2024-12-25T00:00:00Z"),
        pd.Timestamp("2025-01-02T01:00:00Z"),
        "not a timestamp",
        7,
    ],
    ids=["later", "earlier", "between-rows", "string", "integer"],
)
def test_request_column_named_like_source_timestamp_field(
    tmp_path: Path,
    request_value: object,
    filter_by_created_timestamp: bool,
    full_feature_names: bool,
) -> None:
    store = _store(tmp_path, {"trips": "feature_timestamp"})
    requests = pd.DataFrame(
        {
            "driver_id": [1],
            "event_timestamp": pd.to_datetime(["2025-01-02T12:00:00Z"]),
            # Request data that shares the source timestamp name, plus the
            # names a temporary alias would take.
            "feature_timestamp": [request_value],
            "__feature_timestamp": ["request label"],
            "___feature_timestamp": ["another label"],
        }
    )
    original = requests.copy(deep=True)

    result = store.get_historical_features(
        entity_df=requests,
        features=["trips_view:trips"],
        filter_by_created_timestamp=filter_by_created_timestamp,
        full_feature_names=full_feature_names,
    ).to_df()

    feature_column = "trips_view__trips" if full_feature_names else "trips"
    assert result[feature_column].astype("float64").tolist() == [20]
    assert set(result.columns) == set(requests.columns) | {feature_column}
    assert_frame_equal(result[requests.columns], original)
    assert_frame_equal(requests, original)


def test_request_timestamp_alias_does_not_collide_with_request_data(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path, {"trips": "event_timestamp"})
    requests = pd.DataFrame(
        {
            "driver_id": [1],
            "event_timestamp": pd.to_datetime(["2025-01-02T12:00:00Z"]),
            "__event_timestamp": pd.to_datetime(["2025-01-10T00:00:00Z"]),
        }
    )
    original = requests.copy(deep=True)

    result = store.get_historical_features(
        entity_df=requests, features=["trips_view:trips"]
    ).to_df()

    assert result["trips"].astype("float64").tolist() == [20]
    assert set(result.columns) == set(requests.columns) | {"trips"}
    assert_frame_equal(result[requests.columns], original)


def test_feature_views_sharing_a_colliding_timestamp_field(tmp_path: Path) -> None:
    store = _store(tmp_path, {"trips": "ts", "rating": "ts"})
    requests = pd.DataFrame(
        {
            "driver_id": [1],
            "event_timestamp": pd.to_datetime(["2025-01-02T12:00:00Z"]),
            "ts": pd.to_datetime(["2025-01-10T00:00:00Z"]),
            "ts__": pd.to_datetime(["2024-12-25T00:00:00Z"]),
        }
    )
    original = requests.copy(deep=True)

    result = store.get_historical_features(
        entity_df=requests, features=["trips_view:trips", "rating_view:rating"]
    ).to_df()

    assert result["trips"].astype("float64").tolist() == [20]
    assert result["rating"].astype("float64").tolist() == [20]
    assert set(result.columns) == set(requests.columns) | {"trips", "rating"}
    assert_frame_equal(result[requests.columns], original)
