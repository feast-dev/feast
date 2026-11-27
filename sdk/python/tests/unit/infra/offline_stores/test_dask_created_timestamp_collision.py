from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from feast import Entity, FeatureStore, FeatureView, Field, FileSource, RepoConfig
from feast.types import Float64, Int64, ValueType


@pytest.mark.parametrize("full_feature_names", [False, True])
@pytest.mark.parametrize("filter_by_created_timestamp", [False, True])
@pytest.mark.parametrize(
    "created_column,include_request_created_column",
    [("created_ts", False), ("created_ts", True), ("event_timestamp", False)],
    ids=["no-collision", "request-data-collision", "request-timestamp-collision"],
)
def test_historical_retrieval_keeps_source_created_timestamp_separate(
    tmp_path: Path,
    created_column: str,
    include_request_created_column: bool,
    filter_by_created_timestamp: bool,
    full_feature_names: bool,
) -> None:
    source_path = tmp_path / "features.parquet"
    pd.DataFrame(
        {
            "driver_id": [1, 1],
            "feature_timestamp": pd.to_datetime(
                ["2025-01-01T10:00:00Z", "2025-01-01T10:00:00Z"]
            ),
            created_column: pd.to_datetime(
                ["2025-01-01T12:00:00Z", "2025-01-03T00:00:00Z"]
            ),
            "score": [0.4, 0.9],
        }
    ).to_parquet(source_path, index=False)
    entity = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    feature_view = FeatureView(
        name="driver_stats",
        entities=[entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="score", dtype=Float64),
        ],
        source=FileSource(
            path=str(source_path),
            timestamp_field="feature_timestamp",
            created_timestamp_column=created_column,
        ),
        ttl=timedelta(days=7),
    )
    store = FeatureStore(
        config=RepoConfig(
            project="created_collision",
            provider="local",
            registry=str(tmp_path / "registry.db"),
            online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
            offline_store={"type": "file"},
            repo_path=tmp_path,
        )
    )
    store.apply([entity, feature_view])
    requests = pd.DataFrame(
        {
            "driver_id": [1],
            "event_timestamp": pd.to_datetime(["2025-01-02T00:00:00Z"]),
            # Existing request fields must not be reused as temporary source columns.
            f"__{created_column}": ["request label"],
            f"___{created_column}": ["another label"],
        }
    )
    if include_request_created_column:
        requests[created_column] = pd.to_datetime(["2025-01-01T00:00:00Z"])
    original = requests.copy(deep=True)

    result = store.get_historical_features(
        entity_df=requests,
        features=["driver_stats:score"],
        filter_by_created_timestamp=filter_by_created_timestamp,
        full_feature_names=full_feature_names,
    ).to_df()

    feature_column = "driver_stats__score" if full_feature_names else "score"
    expected_score = 0.4 if filter_by_created_timestamp else 0.9
    assert result[feature_column].tolist() == [expected_score]
    assert set(result.columns) == set(requests.columns) | {feature_column}
    assert_frame_equal(result[requests.columns], original)
    assert_frame_equal(requests, original)


@pytest.mark.parametrize("filter_by_created_timestamp", [False, True])
@pytest.mark.parametrize("include_second_view", [False, True])
def test_created_timestamp_alias_does_not_escape_feature_view(
    tmp_path: Path,
    filter_by_created_timestamp: bool,
    include_second_view: bool,
) -> None:
    entity = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    first_path = tmp_path / "first.parquet"
    pd.DataFrame(
        {
            "driver_id": [1],
            "feature_timestamp": pd.to_datetime(["2025-01-01T10:00:00Z"]),
            "created_ts": pd.to_datetime(["2025-01-01T12:00:00Z"]),
            "__created_ts": [0.4],
        }
    ).to_parquet(first_path, index=False)
    first = FeatureView(
        name="first",
        entities=[entity],
        schema=[Field(name="__created_ts", dtype=Float64)],
        source=FileSource(
            path=str(first_path),
            timestamp_field="feature_timestamp",
            created_timestamp_column="created_ts",
        ),
        ttl=timedelta(days=7),
    )
    store = FeatureStore(
        config=RepoConfig(
            project="alias_collision",
            provider="local",
            registry=str(tmp_path / "registry.db"),
            online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
            offline_store={"type": "file"},
            repo_path=tmp_path,
        )
    )
    store.apply([entity, first])
    features = ["first:__created_ts"]
    expected_features = {"first____created_ts"}
    if include_second_view:
        second_path = tmp_path / "second.parquet"
        pd.DataFrame(
            {
                "driver_id": [1, 1],
                "__created_ts": pd.to_datetime(
                    ["2025-01-01T10:00:00Z", "2025-01-03T10:00:00Z"]
                ),
                "score": [0.2, 0.8],
            }
        ).to_parquet(second_path, index=False)
        second = FeatureView(
            name="second",
            entities=[entity],
            schema=[Field(name="score", dtype=Float64)],
            source=FileSource(path=str(second_path), timestamp_field="__created_ts"),
            ttl=timedelta(days=7),
        )
        store.apply([second])
        features.append("second:score")
        expected_features.add("second__score")
    requests = pd.DataFrame(
        {
            "driver_id": [1],
            "event_timestamp": pd.to_datetime(["2025-01-02T00:00:00Z"]),
            "created_ts": pd.to_datetime(["2025-01-01T00:00:00Z"]),
        }
    )
    original = requests.copy(deep=True)
    result = store.get_historical_features(
        entity_df=requests,
        features=features,
        full_feature_names=True,
        filter_by_created_timestamp=filter_by_created_timestamp,
    ).to_df()

    assert result["first____created_ts"].tolist() == [0.4]
    if include_second_view:
        assert result["second__score"].tolist() == [0.2]
    assert set(result.columns) == set(requests.columns) | expected_features
    assert_frame_equal(result[requests.columns], original)
    assert_frame_equal(requests, original)
