from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from feast import ChrononSource, Entity, FeatureStore, FeatureView, Field, RepoConfig
from feast.types import Float32, Float64, Int64


def _store(tmp_path: Path, ttl: timedelta, partitioned: bool = False) -> FeatureStore:
    data_path = tmp_path / "materialization"
    pd.DataFrame(
        {
            "user_id": [1, 2],
            "event_timestamp": pd.to_datetime(["2024-01-01", "2024-01-01"], utc=True),
            "feature_a": [2.0, 4.0],
        }
    ).to_parquet(data_path, partition_cols=["user_id"] if partitioned else None)
    config = RepoConfig(
        project="test",
        registry=str(tmp_path / "registry.db"),
        provider="chronon",
        offline_store={"type": "chronon"},
        online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
    )
    config.repo_path = tmp_path
    store = FeatureStore(config=config)
    user = Entity(name="user", join_keys=["user_id"])
    view = FeatureView(
        name="profile",
        entities=[user],
        ttl=ttl,
        schema=[Field(name="feature_a", dtype=Float32)],
        source=ChrononSource(
            materialization_path=str(data_path), timestamp_field="event_timestamp"
        ),
    )
    store.apply([user, view])
    return store


@pytest.mark.parametrize("ttl", [timedelta(days=1), timedelta(0)])
def test_historical_retrieval_honors_ttl(tmp_path: Path, ttl: timedelta) -> None:
    store = _store(tmp_path, ttl)
    entity_df = pd.DataFrame(
        {
            "user_id": [1, 1, 1],
            "event_timestamp": pd.to_datetime(
                ["2024-01-02", "2024-01-02 00:00:01", "2024-01-10"],
                format="mixed",
                utc=True,
            ),
        }
    )
    result = store.get_historical_features(
        entity_df=entity_df, features=["profile:feature_a"]
    ).to_df()
    assert result["feature_a"].iloc[0] == 2.0
    if ttl:
        assert result["feature_a"].iloc[1:].isna().all()
    else:
        assert result["feature_a"].tolist() == [2.0, 2.0, 2.0]


def test_historical_retrieval_infers_entity_timestamp(tmp_path: Path) -> None:
    store = _store(tmp_path, timedelta(days=1))
    entity_df = pd.DataFrame(
        {"user_id": [1], "request_time": pd.to_datetime(["2024-01-02"], utc=True)}
    )
    result = store.get_historical_features(
        entity_df=entity_df, features=["profile:feature_a"]
    ).to_df()
    assert result["feature_a"].tolist() == [2.0]
    pd.testing.assert_series_equal(result["request_time"], entity_df["request_time"])


def test_historical_retrieval_infers_partitioned_entity(tmp_path: Path) -> None:
    store = _store(tmp_path, timedelta(days=1), partitioned=True)
    assert store.get_feature_view("profile").join_keys == ["user_id"]
    result = store.get_historical_features(
        entity_df=pd.DataFrame(
            {
                "user_id": [2, 1],
                "event_timestamp": pd.to_datetime(
                    ["2024-01-02", "2024-01-02"], utc=True
                ),
            }
        ),
        features=["profile:feature_a"],
    ).to_df()
    assert result["feature_a"].tolist() == [4.0, 2.0]


def _entities() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "user_id": [2, 1],
            "event_timestamp": pd.to_datetime(["2024-01-02", "2024-01-02"], utc=True),
        }
    )


@pytest.mark.parametrize("destination", ["saved/data.parquet", "saved/dataset"])
@pytest.mark.parametrize("timestamp", ["event_timestamp", "request_time"])
def test_saved_dataset_round_trip(
    tmp_path: Path, destination: str, timestamp: str
) -> None:
    from feast.errors import SavedDatasetLocationAlreadyExists
    from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

    store = _store(tmp_path, timedelta(days=1))
    job = store.get_historical_features(
        entity_df=_entities().rename(columns={"event_timestamp": timestamp}),
        features=["profile:feature_a"],
        full_feature_names=True,
    )
    storage = SavedDatasetFileStorage(path=destination)
    dataset = store.create_saved_dataset(from_=job, name="training", storage=storage)
    pd.testing.assert_frame_equal(dataset.to_df(), job.to_df())
    pd.testing.assert_frame_equal(
        store.get_saved_dataset("training").to_df(), job.to_df()
    )
    with pytest.raises(SavedDatasetLocationAlreadyExists):
        job.persist(storage)
    if not destination.endswith(".parquet"):
        pd.DataFrame({"stale": [1]}).to_parquet(tmp_path / destination / "old.parquet")
    smaller = store.get_historical_features(
        entity_df=_entities().iloc[:1].rename(columns={"event_timestamp": timestamp}),
        features=["profile:feature_a"],
        full_feature_names=True,
    )
    smaller.persist(storage, allow_overwrite=True)
    pd.testing.assert_frame_equal(
        pd.read_parquet(tmp_path / destination), smaller.to_df()
    )


@pytest.mark.parametrize("partitioned", [False, True])
def test_projection_reads_only_required_physical_columns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, partitioned: bool
) -> None:
    store = _store(tmp_path, timedelta(days=1))
    path = tmp_path / "mapped"
    pd.DataFrame(
        {
            "raw_user": [1, 1],
            "raw_time": pd.to_datetime(["2024-01-01"] * 2, utc=True),
            "raw_created": pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True),
            "raw_value": [2.0, 3.0],
            "unused": ["large payload"] * 2,
        }
    ).to_parquet(path, partition_cols=["raw_user"] if partitioned else None)
    user = store.get_entity("user")
    view = FeatureView(
        name="mapped",
        entities=[user],
        schema=[Field(name="user_id", dtype=Int64), Field(name="value", dtype=Float32)],
        source=ChrononSource(
            materialization_path=str(path),
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
            field_mapping={
                "raw_user": "user_id",
                "raw_time": "event_timestamp",
                "raw_created": "created",
                "raw_value": "value",
            },
        ),
    )
    store.apply(view)
    original = pd.read_parquet
    reads = []

    def read(*args: Any, **kwargs: Any) -> pd.DataFrame:
        reads.append(kwargs.get("columns"))
        return original(*args, **kwargs)

    monkeypatch.setattr(pd, "read_parquet", read)
    result = store.get_historical_features(
        entity_df=_entities(), features=["mapped:value"]
    ).to_df()
    assert result["value"].iloc[1] == 3.0
    assert reads == [["raw_user", "raw_time", "raw_created", "raw_value"]]


@pytest.mark.parametrize("full_names", [False, True])
def test_on_demand_features_and_persisted_results(
    tmp_path: Path, full_names: bool
) -> None:
    from feast import RequestSource
    from feast.errors import RequestDataNotFoundInEntityDfException
    from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
    from feast.on_demand_feature_view import on_demand_feature_view

    store = _store(tmp_path, timedelta(days=1))
    request = RequestSource(
        name="checkout", schema=[Field(name="amount", dtype=Float32)]
    )

    @on_demand_feature_view(
        sources=[store.get_feature_view("profile"), request],
        schema=[
            Field(name="risk", dtype=Float64),
            Field(name="unused_risk", dtype=Float64),
        ],
        mode="pandas",
    )
    def checkout_risk(inputs: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "risk": inputs["feature_a"] + inputs["amount"],
                "unused_risk": inputs["feature_a"] * 2,
            }
        )

    store.apply(checkout_risk)
    entities = _entities().assign(amount=[10.0, 20.0])
    job = store.get_historical_features(
        entity_df=entities,
        features=["checkout_risk:risk"],
        full_feature_names=full_names,
    )
    result = job.to_df()
    name = "checkout_risk__risk" if full_names else "risk"
    assert result[name].tolist() == [14.0, 22.0]
    assert not any("unused_risk" in col for col in result.columns)
    pd.testing.assert_frame_equal(job.to_arrow().to_pandas(), result)
    dataset = store.create_saved_dataset(
        from_=job,
        name="risk_training",
        storage=SavedDatasetFileStorage(path="risk.parquet"),
    )
    pd.testing.assert_frame_equal(dataset.to_df(), result)
    with pytest.raises(RequestDataNotFoundInEntityDfException, match="amount"):
        store.get_historical_features(
            entity_df=_entities(), features=["checkout_risk:risk"]
        )


@pytest.mark.parametrize("full_names", [False, True])
def test_empty_saved_dataset(tmp_path: Path, full_names: bool) -> None:
    from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

    store = _store(tmp_path, timedelta(days=1))
    job = store.get_historical_features(
        entity_df=_entities().iloc[:0],
        features=["profile:feature_a"],
        full_feature_names=full_names,
    )
    dataset = store.create_saved_dataset(
        from_=job,
        name="empty_training",
        storage=SavedDatasetFileStorage(path="empty.parquet"),
    )
    pd.testing.assert_frame_equal(dataset.to_df(), job.to_df())


def test_saved_dataset_checks_destination_filesystem(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pyarrow.fs as pafs

    from feast.errors import SavedDatasetLocationAlreadyExists
    from feast.infra.offline_stores.file_source import (
        FileSource,
        SavedDatasetFileStorage,
    )

    store = _store(tmp_path, timedelta(days=1))
    filesystem = pafs.SubTreeFileSystem(str(tmp_path), pafs.LocalFileSystem())

    def resolve(uri: str, endpoint: str) -> tuple[Any, str]:
        assert uri == "s3://bucket/saved.parquet"
        return filesystem, "bucket/saved.parquet"

    monkeypatch.setattr(FileSource, "create_filesystem_and_path", resolve)
    job = store.get_historical_features(
        entity_df=_entities(), features=["profile:feature_a"]
    )
    storage = SavedDatasetFileStorage(path="s3://bucket/saved.parquet")
    dataset = store.create_saved_dataset(
        from_=job, name="remote_training", storage=storage
    )
    pd.testing.assert_frame_equal(dataset.to_df(), job.to_df())
    with pytest.raises(SavedDatasetLocationAlreadyExists):
        job.persist(storage)


def test_request_only_on_demand_features(tmp_path: Path) -> None:
    from feast import RequestSource
    from feast.on_demand_feature_view import on_demand_feature_view

    store = _store(tmp_path, timedelta(days=1))
    request = RequestSource(
        name="checkout", schema=[Field(name="amount", dtype=Float64)]
    )

    @on_demand_feature_view(
        sources=[request], schema=[Field(name="doubled", dtype=Float64)], mode="pandas"
    )
    def request_risk(inputs: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"doubled": inputs["amount"] * 2})

    store.apply(request_risk)
    result = store.get_historical_features(
        entity_df=_entities().assign(amount=[10.0, 20.0]),
        features=["request_risk:doubled"],
    ).to_df()
    assert result["doubled"].tolist() == [20.0, 40.0]
    with pytest.raises(ValueError, match="request data.*entity_df"):
        store.get_historical_features(
            features=["request_risk:doubled"],
            start_date=pd.Timestamp("2024-01-01", tz="UTC"),
            end_date=pd.Timestamp("2024-01-02", tz="UTC"),
        )
