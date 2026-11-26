"""Ray offline store: request columns named like the source timestamp field.

The entity dataframe can hold request data in a column that has the same name
as a feature view's source timestamp field. That column is not the event
timestamp, so retrieval must not use it as the point-in-time cutoff, and it
must come back unchanged, whatever its type.
"""

from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

from feast import Entity, FeatureStore, FeatureView, Field, FileSource, RepoConfig
from feast.infra.ray_initializer import shutdown_ray
from feast.types import Int64
from feast.value_type import ValueType


@pytest.fixture(autouse=True)
def fresh_ray():
    shutdown_ray()
    yield
    shutdown_ray()


def _feature_store(repo: Path, with_driver_2: bool = True) -> FeatureStore:
    # Driver 1 has 10, 20 and 30 on 1, 2 and 3 Jan; driver 2 has 40 on 1 Jan.
    trips_df = pd.DataFrame(
        {
            "driver_id": [1, 1, 1, 2],
            "ts": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-01"], utc=True
            ),
            "trips": [10, 20, 30, 40],
        }
    )
    if not with_driver_2:
        # With one driver the source timestamps stay sorted after sorting by
        # driver, so the broadcast join uses merge_asof instead of falling
        # back to its row-by-row join.
        trips_df = trips_df[trips_df["driver_id"] == 1]
    trips_df.to_parquet(repo / "trips.parquet")
    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    trips = FeatureView(
        name="driver_trips",
        entities=[driver],
        schema=[Field(name="trips", dtype=Int64)],
        source=FileSource(path=str(repo / "trips.parquet"), timestamp_field="ts"),
        ttl=timedelta(days=7),
    )
    store = FeatureStore(
        config=RepoConfig(
            project="ray_timestamp_field_collision",
            provider="local",
            registry=str(repo / "registry.db"),
            offline_store={"type": "ray", "storage_path": str(repo / "ray_storage")},
            online_store={"type": "sqlite", "path": str(repo / "online.db")},
            entity_key_serialization_version=3,
            repo_path=str(repo),
        )
    )
    store.apply([driver, trips])
    return store


@pytest.mark.parametrize(
    "request_values",
    [
        pd.to_datetime(
            ["2024-01-10 00:00", "2023-12-25 00:00", "2024-01-02 06:00"], utc=True
        ),
        pd.Series([1, 2, 3], dtype="int64"),
        pd.Series(["a", "2024-01-05", ""], dtype=object),
    ],
    ids=["datetime", "int", "string"],
)
@pytest.mark.parametrize("full_feature_names", [False, True], ids=lambda v: str(v))
def test_request_column_named_like_timestamp_field(
    tmp_path, request_values, full_feature_names
):
    store = _feature_store(tmp_path)
    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": pd.to_datetime(
                ["2024-01-02 12:00", "2024-01-02 13:00", "2024-01-02 12:00"], utc=True
            ),
            "ts": list(request_values),
            "request_id": [1, 2, 3],
        }
    )

    result = store.get_historical_features(
        entity_df=entity_df,
        features=["driver_trips:trips"],
        full_feature_names=full_feature_names,
    ).to_df()

    feature = "driver_trips__trips" if full_feature_names else "trips"
    assert set(result.columns) == set(entity_df.columns) | {feature}
    result = result.sort_values("request_id").reset_index(drop=True)
    # The latest value at or before each event timestamp.
    assert result[feature].tolist() == [20, 20, 40]
    # The request column comes back unchanged.
    assert result["ts"].tolist() == entity_df["ts"].tolist()


@pytest.mark.parametrize(
    "with_colliding_column", [True, False], ids=["colliding-ts", "no-collision"]
)
def test_requests_sharing_entity_and_event_timestamp_are_all_kept(
    tmp_path, with_colliding_column
):
    store = _feature_store(tmp_path, with_driver_2=False)
    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 1],
            "event_timestamp": pd.to_datetime(
                ["2024-01-02 12:00", "2024-01-02 12:00"], utc=True
            ),
            "request_id": [101, 102],
        }
    )
    if with_colliding_column:
        # Different request values in the column named like the source timestamp.
        entity_df["ts"] = pd.to_datetime(["2024-01-02", "2024-01-04"], utc=True)

    result = store.get_historical_features(
        entity_df=entity_df, features=["driver_trips:trips"]
    ).to_df()

    result = result.sort_values("request_id").reset_index(drop=True)
    # Each row is its own request, so both come back, each with the latest value
    # at or before its event timestamp.
    assert result["request_id"].tolist() == [101, 102]
    assert result["trips"].tolist() == [20, 20]
    if with_colliding_column:
        assert result["ts"].tolist() == entity_df["ts"].tolist()
