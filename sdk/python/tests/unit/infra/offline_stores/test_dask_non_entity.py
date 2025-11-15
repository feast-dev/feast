from datetime import datetime, timezone
from unittest.mock import MagicMock

from feast.entity import Entity
from feast.feature_view import FeatureView, Field
from feast.infra.offline_stores.dask import (
    DaskOfflineStore,
    DaskOfflineStoreConfig,
)
from feast.infra.offline_stores.file_source import FileSource
from feast.repo_config import RepoConfig
from feast.types import Float32, ValueType


def _mock_dask_offline_store_config():
    return DaskOfflineStoreConfig(type="dask")


def _mock_entity():
    return [
        Entity(
            name="driver_id",
            join_keys=["driver_id"],
            description="Driver ID",
            value_type=ValueType.INT64,
        )
    ]


def _mock_feature_view():
    return FeatureView(
        name="driver_stats",
        entities=_mock_entity(),
        schema=[
            Field(name="conv_rate", dtype=Float32),
        ],
        source=FileSource(
            path="dummy.parquet",  # not read in this test
            timestamp_field="event_timestamp",
        ),
    )


def test_dask_non_entity_historical_retrieval_accepts_dates():
    repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=_mock_dask_offline_store_config(),
    )

    fv = _mock_feature_view()

    # Expect this to work once non-entity mode is implemented for Dask-based store
    retrieval_job = DaskOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[fv],
        feature_refs=["driver_stats:conv_rate"],
        entity_df=None,  # start/end-only mode
        registry=MagicMock(),
        project="test_project",
        full_feature_names=False,
        start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2023, 1, 2, tzinfo=timezone.utc),
    )

    # When implemented, should return a RetrievalJob
    from feast.infra.offline_stores.offline_store import RetrievalJob

    assert isinstance(retrieval_job, RetrievalJob)
