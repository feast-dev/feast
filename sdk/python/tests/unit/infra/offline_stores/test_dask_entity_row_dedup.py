from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import dask.dataframe as dd
import pandas as pd

from feast.entity import Entity
from feast.feature_view import FeatureView, Field
from feast.infra.offline_stores import dask as dask_mod
from feast.infra.offline_stores.dask import DaskOfflineStore, DaskOfflineStoreConfig
from feast.infra.offline_stores.file_source import FileSource
from feast.repo_config import RepoConfig
from feast.types import Float32, ValueType


def _mock_entity():
    return [
        Entity(
            name="driver_id",
            join_keys=["driver_id"],
            value_type=ValueType.INT64,
        )
    ]


def _mock_feature_view():
    return FeatureView(
        name="driver_stats",
        entities=_mock_entity(),
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=FileSource(path="unused", timestamp_field="event_timestamp"),
        ttl=timedelta(days=1),
    )


def _mock_repo_config():
    return RepoConfig(
        project="proj",
        registry="unused",
        provider="local",
        offline_store=DaskOfflineStoreConfig(type="dask"),
    )


class TestEntityRowDeduplication:
    """
    get_historical_features must return exactly one output row per input
    entity_df row. Two distinct entity_df rows sharing a join key and event
    timestamp - e.g. two orders placed by the same customer in the same
    logged second - are not duplicates of each other, even though the
    feature-source join produces the same feature values for both.

    Regression test for a bug where such rows were collapsed by
    _drop_duplicates, since its dedup key (join keys + event timestamp) did
    not account for other, distinct entity_df columns.
    """

    def test_distinct_entity_rows_are_not_collapsed(self, monkeypatch):
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        src = pd.DataFrame(
            {
                "driver_id": [1],
                "event_timestamp": [ts - timedelta(days=1)],
                "conv_rate": [0.5],
            }
        )
        monkeypatch.setattr(
            dask_mod,
            "_read_datasource",
            lambda ds, repo_path: dd.from_pandas(src, npartitions=1),
        )

        # Three distinct requests: two share (driver_id=1, ts), one is a
        # different entity. Each carries a unique label that must survive.
        entity_df = pd.DataFrame(
            {
                "driver_id": [1, 1, 2],
                "event_timestamp": [ts, ts, ts],
                "request_id": ["req-a", "req-b", "req-c"],
            }
        )

        job = DaskOfflineStore.get_historical_features(
            config=_mock_repo_config(),
            feature_views=[_mock_feature_view()],
            feature_refs=["driver_stats:conv_rate"],
            entity_df=entity_df,
            registry=MagicMock(),
            project="proj",
            full_feature_names=False,
        )
        result = job.to_df()

        assert len(result) == len(entity_df)
        assert sorted(result["request_id"]) == ["req-a", "req-b", "req-c"]
        # driver_id=1's two distinct requests both get the feature value.
        assert set(result.loc[result["driver_id"] == 1, "conv_rate"]) == {0.5}

    def test_row_count_matches_input_with_duplicated_join_key_and_timestamp(
        self, monkeypatch
    ):
        """Same as above, phrased as a direct row-count invariant."""
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        src = pd.DataFrame(
            {
                "driver_id": [1, 2],
                "event_timestamp": [ts - timedelta(days=1)] * 2,
                "conv_rate": [0.1, 0.2],
            }
        )
        monkeypatch.setattr(
            dask_mod,
            "_read_datasource",
            lambda ds, repo_path: dd.from_pandas(src, npartitions=1),
        )

        entity_df = pd.DataFrame(
            {
                "driver_id": [1, 1, 1, 2],
                "event_timestamp": [ts, ts, ts, ts],
                "label": ["a", "b", "c", "d"],
            }
        )

        job = DaskOfflineStore.get_historical_features(
            config=_mock_repo_config(),
            feature_views=[_mock_feature_view()],
            feature_refs=["driver_stats:conv_rate"],
            entity_df=entity_df,
            registry=MagicMock(),
            project="proj",
            full_feature_names=False,
        )
        result = job.to_df()

        assert len(result) == 4
        assert sorted(result["label"]) == ["a", "b", "c", "d"]
