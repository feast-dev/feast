from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from feast.entity import Entity
from feast.feature_view import FeatureView, Field
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStore,
    SparkOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.repo_config import RepoConfig
from feast.types import Float32, ValueType


def _mock_spark_offline_store_config():
    return SparkOfflineStoreConfig(type="spark")


def _mock_entity():
    return [
        Entity(
            name="user_id",
            join_keys=["user_id"],
            description="User ID",
            value_type=ValueType.INT64,
        )
    ]


def _mock_feature_view():
    return FeatureView(
        name="user_stats",
        entities=_mock_entity(),
        schema=[
            Field(name="metric", dtype=Float32),
        ],
        source=SparkSource(
            name="user_stats_source",
            table="default.user_stats",
            timestamp_field="event_timestamp",
            date_partition_column="ds",
            date_partition_column_format="%Y-%m-%d",
        ),
    )


@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.spark.get_spark_session_or_start_new_with_repoconfig"
)
def test_spark_non_entity_historical_retrieval_accepts_dates(mock_get_spark_session):
    # Why: Avoid executing real Spark SQL against non-existent tables during unit tests.
    mock_spark_session = MagicMock()
    mock_get_spark_session.return_value = mock_spark_session
    repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=_mock_spark_offline_store_config(),
    )

    fv = _mock_feature_view()

    retrieval_job = SparkOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[fv],
        feature_refs=["user_stats:metric"],
        entity_df=None,  # start/end-only mode
        registry=MagicMock(),
        project="test_project",
        full_feature_names=False,
        start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2023, 1, 2, tzinfo=timezone.utc),
    )

    from feast.infra.offline_stores.offline_store import RetrievalJob

    assert isinstance(retrieval_job, RetrievalJob)
