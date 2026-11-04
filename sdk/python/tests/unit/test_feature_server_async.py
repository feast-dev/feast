from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient

from feast.feature_server import get_app
from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse


def test_async_get_online_features():
    """Test that async get_online_features endpoint works correctly"""
    fs = MagicMock()
    fs._get_provider.return_value.async_supported.online.read = True
    fs.get_online_features_async = AsyncMock(
        return_value=OnlineResponse(GetOnlineFeaturesResponse())
    )
    fs.get_feature_service = MagicMock()
    fs.initialize = AsyncMock()
    fs.close = AsyncMock()

    client = TestClient(get_app(fs))
    response = client.post(
        "/get-online-features",
        json={"features": ["test:feature"], "entities": {"entity_id": [123]}},
    )

    assert response.status_code == 200
    assert fs.get_online_features_async.await_count == 1
