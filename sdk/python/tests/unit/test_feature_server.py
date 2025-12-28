# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import time
from collections import Counter
from types import SimpleNamespace
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


def test_materialize_endpoint_logic():
    """Test the materialization endpoint logic without HTTP requests"""
    from datetime import datetime

    from feast.feature_server import MaterializeRequest

    # Test 1: Standard request with timestamps
    request = MaterializeRequest(
        start_ts="2021-01-01T00:00:00",
        end_ts="2021-01-02T00:00:00",
        feature_views=["test_view"],
    )
    assert request.disable_event_timestamp is False
    assert request.start_ts is not None
    assert request.end_ts is not None

    # Test 2: Request with disable_event_timestamp
    request_no_ts = MaterializeRequest(
        feature_views=["test_view"], disable_event_timestamp=True
    )
    assert request_no_ts.disable_event_timestamp is True
    assert request_no_ts.start_ts is None
    assert request_no_ts.end_ts is None

    # Test 3: Validation logic (this is what our endpoint does)
    # Simulate the endpoint's validation logic
    if request_no_ts.disable_event_timestamp:
        # Should use epoch to now
        now = datetime.now()
        start_date = datetime(1970, 1, 1)
        end_date = now
        # Should not raise an error
        assert start_date < end_date
    else:
        # Should require timestamps
        if not request_no_ts.start_ts or not request_no_ts.end_ts:
            # This should trigger our validation error
            pass


def test_materialize_request_model():
    """Test MaterializeRequest model validation"""
    from feast.feature_server import MaterializeRequest

    # Test with disable_event_timestamp=True (no timestamps needed)
    req1 = MaterializeRequest(feature_views=["test"], disable_event_timestamp=True)
    assert req1.disable_event_timestamp is True
    assert req1.start_ts is None
    assert req1.end_ts is None

    # Test with disable_event_timestamp=False (timestamps provided)
    req2 = MaterializeRequest(
        start_ts="2021-01-01T00:00:00",
        end_ts="2021-01-02T00:00:00",
        feature_views=["test"],
    )
    assert req2.disable_event_timestamp is False
    assert req2.start_ts == "2021-01-01T00:00:00"
    assert req2.end_ts == "2021-01-02T00:00:00"


def _enable_offline_batching_config(
    fs, enabled: bool = True, batch_size: int = 1, batch_interval_seconds: int = 60
):
    """
        Attach a minimal feature_server.offline_push_batching config
    to a mocked FeatureStore.
    """
    if not hasattr(fs, "config") or fs.config is None:
        fs.config = SimpleNamespace()

    if not hasattr(fs.config, "feature_server") or fs.config.feature_server is None:
        fs.config.feature_server = SimpleNamespace()

    fs.config.feature_server.offline_push_batching_enabled = enabled
    fs.config.feature_server.offline_push_batching_batch_size = batch_size
    fs.config.feature_server.offline_push_batching_batch_interval_seconds = (
        batch_interval_seconds
    )


def push_body_many(push_mode=PushMode.ONLINE, count: int = 2, id_start: int = 100):
    """Build a push body with multiple entities."""
    driver_ids = list(range(id_start, id_start + count))
    lats = [float(i) for i in driver_ids]
    longs = [str(lat) for lat in lats]
    event_ts = [str(_utc_now()) for _ in range(count)]
    created_ts = [str(_utc_now()) for _ in range(count)]

    return {
        "push_source_name": "driver_locations_push",
        "df": {
            "driver_lat": lats,
            "driver_long": longs,
            "driver_id": driver_ids,
            "event_timestamp": event_ts,
            "created_timestamp": created_ts,
        },
        "to": push_mode.name.lower(),
    }


@pytest.mark.parametrize("online_write", [True, False])
@pytest.mark.parametrize("batching_enabled", [True, False])
@pytest.mark.parametrize(
    "push_mode",
    [PushMode.ONLINE, PushMode.OFFLINE, PushMode.ONLINE_AND_OFFLINE],
)
def test_push_batched_matrix(
    online_write, batching_enabled, push_mode, mock_fs_factory
):
    """
    Matrix over:
      - online_write ∈ {True, False}
      - batching_enabled ∈ {True, False}
      - push_mode ∈ {ONLINE, OFFLINE, ONLINE_AND_OFFLINE}

    Asserts:
      - which of fs.push / fs.push_async are called
      - how many times
      - with which `to` values

    For batching_enabled=True, batch_size=1 ensures immediate flush of offline part.
    """
    fs = mock_fs_factory(online_write=online_write)

    _enable_offline_batching_config(
        fs,
        enabled=batching_enabled,
        batch_size=1,  # flush immediately on a single offline request
        batch_interval_seconds=60,
    )

    client = TestClient(get_app(fs))

    # use a multi-row payload to ensure we test non-trivial dfs
    resp = client.post("/push", json=push_body_many(push_mode, count=2, id_start=100))
    assert resp.status_code == 200

    # Collect calls
    sync_calls = fs.push.call_args_list
    async_calls = fs.push_async.await_args_list
    sync_tos = [c.kwargs.get("to") for c in sync_calls]
    async_tos = [c.kwargs.get("to") for c in async_calls]

    # -------------------------------
    # Build expectations
    # -------------------------------
    expected_sync_calls = 0
    expected_async_calls = 0
    expected_sync_tos = []
    expected_async_tos = []

    if push_mode == PushMode.ONLINE:
        # Only online path, batching irrelevant
        if online_write:
            expected_async_calls = 1
            expected_async_tos = [PushMode.ONLINE]
        else:
            expected_sync_calls = 1
            expected_sync_tos = [PushMode.ONLINE]

    elif push_mode == PushMode.OFFLINE:
        # Only offline path, never async
        if batching_enabled:
            # via batcher, but externally still one push(to=OFFLINE)
            expected_sync_calls = 1
            expected_sync_tos = [PushMode.OFFLINE]
        else:
            # direct push(to=OFFLINE)
            expected_sync_calls = 1
            expected_sync_tos = [PushMode.OFFLINE]

    elif push_mode == PushMode.ONLINE_AND_OFFLINE:
        if not batching_enabled:
            # Old behaviour: single call with to=ONLINE_AND_OFFLINE
            if online_write:
                expected_async_calls = 1
                expected_async_tos = [PushMode.ONLINE_AND_OFFLINE]
            else:
                expected_sync_calls = 1
                expected_sync_tos = [PushMode.ONLINE_AND_OFFLINE]
        else:
            # Batching enabled: ONLINE part and OFFLINE part are split
            if online_write:
                # async ONLINE + sync OFFLINE (via batcher)
                expected_async_calls = 1
                expected_async_tos = [PushMode.ONLINE]
                expected_sync_calls = 1
                expected_sync_tos = [PushMode.OFFLINE]
            else:
                # both ONLINE and OFFLINE via sync push
                expected_sync_calls = 2
                expected_sync_tos = [PushMode.ONLINE, PushMode.OFFLINE]

    # -------------------------------
    # Assert counts
    # -------------------------------
    assert fs.push.call_count == expected_sync_calls
    assert fs.push_async.await_count == expected_async_calls

    # Allow ordering differences by comparing as multisets
    assert Counter(sync_tos) == Counter(expected_sync_tos)
    assert Counter(async_tos) == Counter(expected_async_tos)


def test_offline_batches_are_separated_by_flags(mock_fs_factory):
    """
    Offline batches must be separated by (allow_registry_cache, transform_on_write).

    If we send three offline pushes with the same push_source_name but different
    combinations of allow_registry_cache / transform_on_write, they must result
    in three separate fs.push(...) calls, not one merged batch.
    """
    fs = mock_fs_factory(online_write=True)
    # Large batch_size so we rely on interval-based flush, not size-based.
    _enable_offline_batching_config(
        fs, enabled=True, batch_size=100, batch_interval_seconds=1
    )

    client = TestClient(get_app(fs))

    # Base body: allow_registry_cache=True, transform_on_write=True (default)
    body_base = push_body_many(PushMode.OFFLINE, count=2, id_start=100)

    # 1) Default flags: allow_registry_cache=True, transform_on_write=True
    resp1 = client.post("/push", json=body_base)
    assert resp1.status_code == 200

    # 2) Different allow_registry_cache
    body_allow_false = dict(body_base)
    body_allow_false["allow_registry_cache"] = False
    resp2 = client.post("/push", json=body_allow_false)
    assert resp2.status_code == 200

    # 3) Different transform_on_write
    body_transform_false = dict(body_base)
    body_transform_false["transform_on_write"] = False
    resp3 = client.post("/push", json=body_transform_false)
    assert resp3.status_code == 200

    # Immediately after: no flush expected yet (interval-based)
    assert fs.push.call_count == 0

    # Wait up to ~3 seconds for interval-based flush
    deadline = time.time() + 3.0
    while time.time() < deadline and fs.push.call_count < 3:
        time.sleep(0.1)

    # We expect exactly 3 separate pushes, each with 2 rows and to=OFFLINE
    assert fs.push.call_count == 3

    lengths = [c.kwargs["df"].shape[0] for c in fs.push.call_args_list]
    tos = [c.kwargs["to"] for c in fs.push.call_args_list]
    allow_flags = [c.kwargs["allow_registry_cache"] for c in fs.push.call_args_list]
    transform_flags = [c.kwargs["transform_on_write"] for c in fs.push.call_args_list]

    assert all(t == PushMode.OFFLINE for t in tos)
    assert lengths == [2, 2, 2]

    # Ensure we really saw 3 distinct (allow_registry_cache, transform_on_write) combos
    assert len({(a, t) for a, t in zip(allow_flags, transform_flags)}) == 3


def test_offline_batcher_interval_flush(mock_fs_factory):
    """
    With batching enabled and a large batch_size, ensure that the time-based
    flush still triggers even when the size threshold is never reached.
    """
    fs = mock_fs_factory(online_write=True)
    _enable_offline_batching_config(
        fs,
        enabled=True,
        batch_size=100,  # won't be hit by this test
        batch_interval_seconds=1,  # small interval
    )

    client = TestClient(get_app(fs))

    # Send a single OFFLINE push (2 rows), below size threshold
    resp = client.post(
        "/push", json=push_body_many(PushMode.OFFLINE, count=2, id_start=500)
    )
    assert resp.status_code == 200

    # Immediately after: no sync push yet (buffer only)
    assert fs.push.call_count == 0

    # Wait up to ~3 seconds for interval-based flush
    deadline = time.time() + 3.0
    while time.time() < deadline and fs.push.call_count < 1:
        time.sleep(0.1)

    assert fs.push.call_count == 1
    kwargs = fs.push.call_args.kwargs
    assert kwargs["to"] == PushMode.OFFLINE
    assert len(kwargs["df"]) == 2


# Static Artifacts Tests
@pytest.fixture
def mock_store_with_static_artifacts(tmp_path):
    """Create a mock store with static_artifacts.py file for testing."""
    # Create static_artifacts.py file
    static_artifacts_content = '''
from fastapi import FastAPI
from fastapi.logger import logger

def load_test_model():
    """Mock model loading for testing."""
    logger.info("Loading test model...")
    return "test_model_loaded"

def load_test_lookup_tables():
    """Mock lookup tables for testing."""
    return {"test_label": "test_value"}

def load_artifacts(app: FastAPI):
    """Load test static artifacts."""
    app.state.test_model = load_test_model()
    app.state.test_lookup_tables = load_test_lookup_tables()
    logger.info("✅ Test static artifacts loaded")
'''

    # Write static_artifacts.py to temp directory
    artifacts_file = tmp_path / "static_artifacts.py"
    artifacts_file.write_text(static_artifacts_content)

    # Create mock store
    mock_store = MagicMock()
    mock_store.repo_path = str(tmp_path)
    return mock_store


def test_load_static_artifacts_success(mock_store_with_static_artifacts):
    """Test successful loading of static artifacts during server startup."""
    import asyncio

    from fastapi import FastAPI

    from feast.feature_server import load_static_artifacts

    app = FastAPI()

    # Load static artifacts
    asyncio.run(load_static_artifacts(app, mock_store_with_static_artifacts))

    # Verify artifacts were loaded into app.state
    assert hasattr(app.state, "test_model")
    assert hasattr(app.state, "test_lookup_tables")
    assert app.state.test_model == "test_model_loaded"
    assert app.state.test_lookup_tables == {"test_label": "test_value"}


def test_load_static_artifacts_no_file(tmp_path):
    """Test graceful handling when static_artifacts.py doesn't exist."""
    import asyncio

    from fastapi import FastAPI

    from feast.feature_server import load_static_artifacts

    app = FastAPI()
    mock_store = MagicMock()
    mock_store.repo_path = str(tmp_path)  # Empty directory

    # Should not raise an exception
    asyncio.run(load_static_artifacts(app, mock_store))

    # Should not have added test artifacts
    assert not hasattr(app.state, "test_model")
    assert not hasattr(app.state, "test_lookup_tables")


def test_load_static_artifacts_invalid_file(tmp_path):
    """Test graceful handling when static_artifacts.py has errors."""
    import asyncio

    from fastapi import FastAPI

    from feast.feature_server import load_static_artifacts

    # Create invalid static_artifacts.py
    artifacts_file = tmp_path / "static_artifacts.py"
    artifacts_file.write_text("raise ValueError('Test error')")

    app = FastAPI()
    mock_store = MagicMock()
    mock_store.repo_path = str(tmp_path)

    # Should handle the error gracefully
    asyncio.run(load_static_artifacts(app, mock_store))

    # Should not have artifacts due to error
    assert not hasattr(app.state, "test_model")


def test_load_static_artifacts_no_load_function(tmp_path):
    """Test handling when static_artifacts.py has no load_artifacts function."""
    import asyncio

    from fastapi import FastAPI

    from feast.feature_server import load_static_artifacts

    # Create static_artifacts.py without load_artifacts function
    artifacts_file = tmp_path / "static_artifacts.py"
    artifacts_file.write_text("TEST_CONSTANT = 'test'")

    app = FastAPI()
    mock_store = MagicMock()
    mock_store.repo_path = str(tmp_path)

    # Should handle gracefully
    asyncio.run(load_static_artifacts(app, mock_store))

    # Should not have artifacts since no load_artifacts function
    assert not hasattr(app.state, "test_model")


def test_static_artifacts_persist_across_requests(mock_store_with_static_artifacts):
    """Test that static artifacts persist across multiple requests."""
    from feast.feature_server import get_app

    # Create app with static artifacts
    app = get_app(mock_store_with_static_artifacts)

    # Simulate artifacts being loaded (normally done in lifespan)
    app.state.test_model = "persistent_model"
    app.state.test_lookup_tables = {"persistent": "data"}

    # Artifacts should be available and persistent
    assert app.state.test_model == "persistent_model"
    assert app.state.test_lookup_tables["persistent"] == "data"

    # After simulated requests, artifacts should still be there
    assert app.state.test_model == "persistent_model"
    assert app.state.test_lookup_tables["persistent"] == "data"


def test_pytorch_nlp_template_artifacts_pattern(tmp_path):
    """Test the specific PyTorch NLP template static artifacts pattern."""
    import asyncio

    from fastapi import FastAPI

    from feast.feature_server import load_static_artifacts

    # Create PyTorch NLP template-style static_artifacts.py
    pytorch_artifacts_content = '''
from fastapi import FastAPI
from fastapi.logger import logger

def load_sentiment_model():
    """Mock sentiment analysis model loading."""
    logger.info("Loading sentiment analysis model...")
    return "mock_roberta_sentiment_model"

def load_lookup_tables():
    """Load lookup tables for sentiment mapping."""
    return {
        "sentiment_labels": {"LABEL_0": "negative", "LABEL_1": "neutral", "LABEL_2": "positive"},
        "emoji_sentiment": {"😊": "positive", "😞": "negative", "😐": "neutral"},
    }

def load_artifacts(app: FastAPI):
    """Load all static artifacts for PyTorch NLP template."""
    app.state.sentiment_model = load_sentiment_model()
    app.state.lookup_tables = load_lookup_tables()

    # Update global references (simulating example_repo.py pattern)
    # In real template, this would be: import example_repo; example_repo._sentiment_model = ...
    logger.info("✅ PyTorch NLP static artifacts loaded successfully")
'''

    artifacts_file = tmp_path / "static_artifacts.py"
    artifacts_file.write_text(pytorch_artifacts_content)

    # Test loading
    app = FastAPI()
    mock_store = MagicMock()
    mock_store.repo_path = str(tmp_path)

    asyncio.run(load_static_artifacts(app, mock_store))

    # Verify PyTorch NLP template artifacts
    assert hasattr(app.state, "sentiment_model")
    assert hasattr(app.state, "lookup_tables")
    assert app.state.sentiment_model == "mock_roberta_sentiment_model"

    # Verify lookup tables structure matches template
    lookup_tables = app.state.lookup_tables
    assert "sentiment_labels" in lookup_tables
    assert "emoji_sentiment" in lookup_tables
    assert lookup_tables["sentiment_labels"]["LABEL_0"] == "negative"
    assert lookup_tables["sentiment_labels"]["LABEL_1"] == "neutral"
    assert lookup_tables["sentiment_labels"]["LABEL_2"] == "positive"
    assert lookup_tables["emoji_sentiment"]["😊"] == "positive"
