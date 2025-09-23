import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from feast.data_source import PushMode
from feast.errors import PushSourceNotFoundException
from feast.feature_server import get_app
from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.utils import _utc_now
from tests.foo_provider import FooProvider
from tests.utils.cli_repo_creator import CliRunner, get_example_repo


@pytest.fixture
def mock_fs_factory():
    def builder(**async_support):
        provider = FooProvider.with_async_support(**async_support)
        fs = MagicMock()
        fs._get_provider.return_value = provider
        empty_response = OnlineResponse(GetOnlineFeaturesResponse(results=[]))
        fs.get_online_features = MagicMock(return_value=empty_response)
        fs.push = MagicMock()
        fs.get_online_features_async = AsyncMock(return_value=empty_response)
        fs.push_async = AsyncMock()
        return fs

    return builder


@pytest.fixture
def test_client():
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"), "file"
    ) as store:
        yield TestClient(get_app(store))


def get_online_features_body():
    return {
        "features": [
            "pushed_driver_locations:driver_lat",
            "pushed_driver_locations:driver_long",
        ],
        "entities": {"driver_id": [123]},
    }


def push_body(push_mode=PushMode.ONLINE, lat=42.0):
    return {
        "push_source_name": "driver_locations_push",
        "df": {
            "driver_lat": [lat],
            "driver_long": ["42.0"],
            "driver_id": [123],
            "event_timestamp": [str(_utc_now())],
            "created_timestamp": [str(_utc_now())],
        },
        "to": push_mode.name.lower(),
    }


@pytest.mark.parametrize("async_online_read", [True, False])
def test_get_online_features_async_supported(async_online_read, mock_fs_factory):
    fs = mock_fs_factory(online_read=async_online_read)
    client = TestClient(get_app(fs))
    client.post("/get-online-features", json=get_online_features_body())
    assert fs.get_online_features.call_count == int(not async_online_read)
    assert fs.get_online_features_async.await_count == int(async_online_read)


@pytest.mark.parametrize(
    "online_write,push_mode,async_count",
    [
        (True, PushMode.ONLINE_AND_OFFLINE, 1),
        (True, PushMode.OFFLINE, 0),
        (True, PushMode.ONLINE, 1),
        (False, PushMode.ONLINE_AND_OFFLINE, 0),
        (False, PushMode.OFFLINE, 0),
        (False, PushMode.ONLINE, 0),
    ],
)
def test_push_online_async_supported(
    online_write, push_mode, async_count, mock_fs_factory
):
    fs = mock_fs_factory(online_write=online_write)
    client = TestClient(get_app(fs))
    client.post("/push", json=push_body(push_mode))
    assert fs.push.call_count == 1 - async_count
    assert fs.push_async.await_count == async_count


async def test_push_and_get(test_client):
    driver_lat = 55.1
    push_payload = push_body(lat=driver_lat)
    response = test_client.post("/push", json=push_payload)
    assert response.status_code == 200

    # Check new pushed temperature is fetched
    request_payload = get_online_features_body()
    actual_resp = test_client.post("/get-online-features", json=request_payload)
    actual = json.loads(actual_resp.text)

    ix = actual["metadata"]["feature_names"].index("driver_lat")
    assert actual["results"][ix]["values"][0] == pytest.approx(driver_lat, 0.0001)

    assert_get_online_features_response_format(
        actual, request_payload["entities"]["driver_id"][0]
    )


def assert_get_online_features_response_format(parsed_response, expected_entity_id):
    assert "metadata" in parsed_response
    metadata = parsed_response["metadata"]
    expected_features = ["driver_id", "driver_lat", "driver_long"]
    response_feature_names = metadata["feature_names"]
    assert len(response_feature_names) == len(expected_features)
    for expected_feature in expected_features:
        assert expected_feature in response_feature_names
    assert "results" in parsed_response
    results = parsed_response["results"]
    for result in results:
        # Same order as in metadata
        assert len(result["statuses"]) == 1  # Requested one entity
        for status in result["statuses"]:
            assert status == "PRESENT"
    results_driver_id_index = response_feature_names.index("driver_id")
    assert results[results_driver_id_index]["values"][0] == expected_entity_id


def test_push_source_does_not_exist(test_client):
    with pytest.raises(
        PushSourceNotFoundException,
        match="Unable to find push source 'push_source_does_not_exist'",
    ):
        test_client.post(
            "/push",
            json={
                "push_source_name": "push_source_does_not_exist",
                "df": {
                    "any_data": [1],
                    "event_timestamp": [str(_utc_now())],
                },
            },
        )


def test_materialize_with_timestamps(test_client):
    """Test standard materialization with timestamps"""
    response = test_client.post(
        "/materialize",
        json={
            "start_ts": "2021-01-01T00:00:00",
            "end_ts": "2021-01-02T00:00:00",
            "feature_views": ["driver_hourly_stats"]
        }
    )
    assert response.status_code == 200


def test_materialize_disable_event_timestamp(test_client):
    """Test materialization with disable_event_timestamp flag"""
    response = test_client.post(
        "/materialize",
        json={
            "feature_views": ["driver_hourly_stats"],
            "disable_event_timestamp": True
        }
    )
    assert response.status_code == 200


def test_materialize_missing_timestamps_fails(test_client):
    """Test that missing timestamps without disable_event_timestamp fails"""
    response = test_client.post(
        "/materialize",
        json={
            "feature_views": ["driver_hourly_stats"]
        }
    )
    assert response.status_code == 422  # Validation error for missing required fields


def test_materialize_request_model():
    """Test MaterializeRequest model validation"""
    from feast.feature_server import MaterializeRequest

    # Test with disable_event_timestamp=True (no timestamps needed)
    req1 = MaterializeRequest(
        feature_views=["test"],
        disable_event_timestamp=True
    )
    assert req1.disable_event_timestamp is True
    assert req1.start_ts is None
    assert req1.end_ts is None

    # Test with disable_event_timestamp=False (timestamps provided)
    req2 = MaterializeRequest(
        start_ts="2021-01-01T00:00:00",
        end_ts="2021-01-02T00:00:00",
        feature_views=["test"]
    )
    assert req2.disable_event_timestamp is False
    assert req2.start_ts == "2021-01-01T00:00:00"
    assert req2.end_ts == "2021-01-02T00:00:00"
