from datetime import datetime, timedelta, timezone
from tempfile import mkstemp
from unittest.mock import MagicMock, patch

from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.types import Float32


def _feature_store() -> FeatureStore:
    _, registry_path = mkstemp()
    _, online_store_path = mkstemp()
    return FeatureStore(
        config=RepoConfig(
            registry=registry_path,
            project="default",
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=online_store_path),
            entity_key_serialization_version=3,
        )
    )


def _retrieval_end_date(**kwargs) -> datetime:
    store = _feature_store()
    store.apply(
        [
            FeatureView(
                name="driver_stats",
                entities=[],
                schema=[Field(name="conv_rate", dtype=Float32)],
                source=FileSource(
                    path="file://feast/*",
                    timestamp_field="event_timestamp",
                ),
                ttl=timedelta(days=1),
            ),
        ]
    )
    with patch.object(
        store._get_provider(), "get_historical_features", return_value=MagicMock()
    ) as provider:
        store.get_historical_features(features=["driver_stats:conv_rate"], **kwargs)
    assert provider.call_count == 1
    return provider.call_args.kwargs["end_date"]


def test_end_date_defaults_to_utc_when_entity_df_is_omitted():
    before = datetime.now(timezone.utc)
    end_date = _retrieval_end_date()
    after = datetime.now(timezone.utc)

    assert end_date.utcoffset() == timedelta(0)
    assert before <= end_date <= after


def test_explicit_end_date_reaches_the_offline_store_unchanged():
    requested = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert _retrieval_end_date(end_date=requested) == requested
