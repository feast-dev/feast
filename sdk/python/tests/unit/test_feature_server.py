import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from feast import FeatureStore
from feast.data_source import PushMode
from feast.feature_server import get_app
from feast.utils import _utc_now
from tests.foo_provider import FooProvider


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
def test_push_online_async_supported(online_write, push_mode, async_count, environment):
    push_payload = json.dumps(
        {
            "push_source_name": "location_stats_push_source",
            "df": {
                "location_id": [1],
                "temperature": [100],
                "event_timestamp": [str(_utc_now())],
                "created": [str(_utc_now())],
            },
            "to": push_mode.name.lower(),
        }
    )

    provider = FooProvider.with_async_support(online_write=online_write)
    print(provider.async_supported.online.write)
    with patch.object(FeatureStore, "_get_provider", return_value=provider):
        fs = environment.feature_store
        fs.push = MagicMock()
        fs.push_async = AsyncMock()
        client = TestClient(get_app(fs))
        client.post("/push", data=push_payload)
        assert fs.push.call_count == 1 - async_count
        assert fs.push_async.await_count == async_count
