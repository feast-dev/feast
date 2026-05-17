from unittest.mock import MagicMock, patch

import assertpy

from feast.infra.offline_stores.remote import (
    RemoteOfflineStore,
    RemoteOfflineStoreConfig,
    _create_retrieval_metadata,
)
from feast.offline_server import OfflineServer


def test_create_retrieval_metadata_with_sql_string():
    """SQL string entity_df should produce a stub with empty keys and no timestamps."""
    sql = "SELECT driver_id, event_timestamp FROM driver_stats"
    metadata = _create_retrieval_metadata(
        feature_refs=["driver_hourly_stats:conv_rate"], entity_df=sql
    )
    assertpy.assert_that(metadata.features).is_equal_to(
        ["driver_hourly_stats:conv_rate"]
    )
    assertpy.assert_that(list(metadata.keys)).is_empty()
    assertpy.assert_that(metadata.min_event_timestamp).is_none()
    assertpy.assert_that(metadata.max_event_timestamp).is_none()


def test_remote_offline_store_sql_entity_df_routing():
    """RemoteOfflineStore.get_historical_features moves SQL into api_parameters."""
    sql = "SELECT driver_id, event_timestamp FROM driver_stats"

    mock_client = MagicMock()
    with patch(
        "feast.infra.offline_stores.remote.build_arrow_flight_client",
        return_value=mock_client,
    ):
        job = RemoteOfflineStore.get_historical_features(
            config=MagicMock(
                offline_store=RemoteOfflineStoreConfig(
                    type="remote", host="localhost", port=8815
                ),
                auth_config=MagicMock(type="no_auth"),
            ),
            feature_views=[],
            feature_refs=["driver_hourly_stats:conv_rate"],
            entity_df=sql,
            registry=MagicMock(),
            project="test",
            full_feature_names=False,
        )

    assertpy.assert_that(job.entity_df).is_none()
    assertpy.assert_that(job.api_parameters).contains_key("entity_df_sql")
    assertpy.assert_that(job.api_parameters["entity_df_sql"]).is_equal_to(sql)


def test_offline_server_get_historical_features_passes_sql_to_store():
    """OfflineServer forwards entity_df_sql to the backing offline store."""
    sql = "SELECT driver_id, event_timestamp FROM driver_stats"

    mock_job = MagicMock()
    mock_offline_store = MagicMock()
    mock_offline_store.get_historical_features.return_value = mock_job

    mock_store = MagicMock()
    mock_store.config.project = "test"

    server = MagicMock(spec=OfflineServer)
    server.offline_store = mock_offline_store
    server.store = mock_store
    server.flights = {}
    server.list_feature_views_by_name.return_value = []

    command = {
        "api": "get_historical_features",
        "command_id": "abc",
        "feature_view_names": [],
        "name_aliases": [],
        "feature_refs": ["driver_hourly_stats:conv_rate"],
        "project": "test",
        "full_feature_names": False,
        "entity_df_sql": sql,
    }

    result = OfflineServer.get_historical_features(server, command, key=None)

    assertpy.assert_that(result).is_equal_to(mock_job)
    _, kwargs = mock_offline_store.get_historical_features.call_args
    assertpy.assert_that(kwargs["entity_df"]).is_equal_to(sql)
