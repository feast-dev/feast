from datetime import timedelta
from unittest.mock import MagicMock

import dask.dataframe as dd
import pandas as pd

from feast.entity import Entity
from feast.feature_view import FeatureView, Field
from feast.infra.offline_stores import dask as dask_mod
from feast.infra.offline_stores.dask import DaskOfflineStore, DaskOfflineStoreConfig
from feast.infra.offline_stores.file_source import FileSource
from feast.repo_config import RepoConfig
from feast.types import Float32, Int64, ValueType

# Enough rows spread over several partitions that an unstable sort reorders
# rows sharing an event timestamp; small inputs tend to keep their order.
NUM_DRIVERS = 100
NUM_PARTITIONS = 4

EVENT_TS = pd.Timestamp("2025-01-01T10:00:00Z")
ENTITY_TS = pd.Timestamp("2025-01-02T00:00:00Z")


def _source_df() -> pd.DataFrame:
    # Each driver has two rows with the same event timestamp. The row created
    # later carries the correction and must win.
    driver_ids = list(range(NUM_DRIVERS))
    event_ts = [EVENT_TS + timedelta(minutes=i % 10) for i in driver_ids]
    return pd.DataFrame(
        {
            "driver_id": driver_ids + driver_ids,
            "event_timestamp": event_ts + event_ts,
            "created_ts": event_ts + [ts + timedelta(hours=1) for ts in event_ts],
            "conv_rate": [0.0] * NUM_DRIVERS + [1.0] * NUM_DRIVERS,
        }
    )


def _use_source_df(monkeypatch):
    ddf = dd.from_pandas(_source_df(), npartitions=NUM_PARTITIONS)
    monkeypatch.setattr(dask_mod, "_read_datasource", lambda ds, repo_path: ddf)


def _config() -> RepoConfig:
    return RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=DaskOfflineStoreConfig(type="dask"),
    )


def _file_source() -> FileSource:
    return FileSource(
        path="dummy.parquet",  # not read in this test
        timestamp_field="event_timestamp",
        created_timestamp_column="created_ts",
    )


def test_historical_retrieval_breaks_event_timestamp_ties_by_created_timestamp(
    monkeypatch,
):
    _use_source_df(monkeypatch)
    fv = FeatureView(
        name="driver_stats",
        entities=[
            Entity(
                name="driver_id",
                join_keys=["driver_id"],
                value_type=ValueType.INT64,
            )
        ],
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=_file_source(),
        ttl=timedelta(days=7),
    )
    fv.entity_columns = [Field(name="driver_id", dtype=Int64)]
    registry = MagicMock()
    registry.list_on_demand_feature_views.return_value = []
    entity_df = pd.DataFrame(
        {
            "driver_id": list(range(NUM_DRIVERS)),
            "event_timestamp": [ENTITY_TS] * NUM_DRIVERS,
        }
    )

    df = DaskOfflineStore.get_historical_features(
        config=_config(),
        feature_views=[fv],
        feature_refs=["driver_stats:conv_rate"],
        entity_df=entity_df,
        registry=registry,
        project="test_project",
        full_feature_names=False,
    ).to_df()

    assert len(df) == NUM_DRIVERS
    assert df["conv_rate"].tolist() == [1.0] * NUM_DRIVERS


def test_pull_latest_breaks_event_timestamp_ties_by_created_timestamp(monkeypatch):
    _use_source_df(monkeypatch)

    df = DaskOfflineStore.pull_latest_from_table_or_query(
        config=_config(),
        data_source=_file_source(),
        join_key_columns=["driver_id"],
        feature_name_columns=["conv_rate"],
        timestamp_field="event_timestamp",
        created_timestamp_column="created_ts",
    ).to_df()

    assert len(df) == NUM_DRIVERS
    assert df["conv_rate"].tolist() == [1.0] * NUM_DRIVERS
