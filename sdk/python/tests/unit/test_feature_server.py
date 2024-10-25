import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient
from pytest_lazyfixture import lazy_fixture

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
def feature_store_with_local_registry():
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"), "file"
    ) as store:
        yield store


def get_online_features_body():
    return {
        "features": [
            "driver_locations:lat",
            "driver_locations:lon",
        ],
        "entities": {"driver_id": [5001, 5002]},
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


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
async def test_push_and_get(test_feature_store):
    request_payload = push_body(lat=55.1)
    client = TestClient(get_app(test_feature_store))
    response = client.post("/push", json=request_payload)

    # Check new pushed temperature is fetched
    assert response.status_code == 200
    actual_resp = client.post(
        "/get-online-features",
        json={
            "feature_service": "driver_locations_service",
            "entities": {"driver_id": [request_payload["entities"]["driver_id"][0]]},
        },
    )
    actual = json.loads(actual_resp.text)
    print(actual)
    ix = actual["metadata"]["feature_names"].index("lat")
    assert actual["results"][ix]["values"][0] == request_payload["df"]["lon"]
    assert_get_online_features_response_format(
        actual, request_payload["entities"]["driver_id"]
    )


def assert_get_online_features_response_format(parsed_response, expected_entity_id):
    assert "metadata" in parsed_response
    metadata = parsed_response["metadata"]
    expected_features = ["driver_id", "lat", "lon"]
    response_feature_names = metadata["feature_names"]
    assert len(response_feature_names) == len(expected_features)
    for expected_feature in expected_features:
        assert expected_feature in response_feature_names
    assert "results" in parsed_response
    results = parsed_response["results"]
    for result in results:
        # Same order as in metadata
        assert len(result["statuses"]) == 2  # Requested two entities
        for status in result["statuses"]:
            assert status == "PRESENT"
    results_driver_id_index = response_feature_names.index("driver_id")
    assert results[results_driver_id_index]["values"] == expected_entity_id


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_push_source_does_not_exist(test_feature_store):
    with pytest.raises(
        PushSourceNotFoundException,
        match="Unable to find push source 'push_source_does_not_exist'",
    ):
        client = TestClient(get_app(test_feature_store))
        client.post(
            "/push",
            json={
                "push_source_name": "push_source_does_not_exist",
                "df": {
                    "any_data": [1],
                    "event_timestamp": [str(_utc_now())],
                },
            },
        )
