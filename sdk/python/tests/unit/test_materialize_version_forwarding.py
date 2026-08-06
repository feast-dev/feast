# Copyright 2026 The Feast Authors
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
"""Server tests: version is forwarded on materialize sync and async paths."""

from concurrent.futures import Future, ThreadPoolExecutor
from inspect import signature
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from feast.feature_server import _authorize_materialize_views, get_app
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureViewState


def _mock_store_for_materialize():
    fs = MagicMock()
    fs.project = "test_project"
    fs.initialize = AsyncMock()
    fs.close = AsyncMock()
    fs._validate_materialize_version.return_value = "v2"
    fv = MagicMock()
    fv.name = "driver_hourly_stats"
    fv.state = FeatureViewState.GENERATED
    fs._get_feature_views_to_materialize.return_value = [fv]
    fs.registry.get_feature_view.return_value = fv
    fs.materialize = MagicMock()
    fs.materialize_incremental = MagicMock()
    return fs


def _run_executor_inline(loop, executor, func, *args):
    """Make asyncio run_in_executor execute synchronously for tests."""
    fut: Future = Future()
    try:
        fut.set_result(func(*args) if args else func())
    except Exception as exc:
        fut.set_exception(exc)
    return fut


def test_run_async_defaults_to_false():
    for method_name in (
        "materialize",
        "materialize_incremental",
        "_delegate_remote_materialize",
    ):
        param = signature(getattr(FeatureStore, method_name)).parameters["run_async"]
        assert param.default is False, (
            f"{method_name}.run_async default should be False"
        )


@patch("feast.feature_server.assert_permissions")
def test_authorize_materialize_views_forwards_version(_mock_perms):
    fs = _mock_store_for_materialize()
    names = _authorize_materialize_views(fs, ["driver_hourly_stats"], version="v2")
    assert names == ["driver_hourly_stats"]
    fs._validate_materialize_version.assert_called_once_with(
        "v2", ["driver_hourly_stats"]
    )
    fs._get_feature_views_to_materialize.assert_called_once_with(
        ["driver_hourly_stats"], version="v2"
    )


@patch("feast.feature_server.assert_permissions")
def test_materialize_sync_forwards_version(_mock_perms):
    fs = _mock_store_for_materialize()
    client = TestClient(get_app(fs))

    response = client.post(
        "/materialize",
        json={
            "start_ts": "2021-01-01T00:00:00",
            "end_ts": "2021-01-02T00:00:00",
            "feature_views": ["driver_hourly_stats"],
            "version": "v2",
        },
    )

    assert response.status_code == 200
    fs._validate_materialize_version.assert_called_with("v2", ["driver_hourly_stats"])
    fs.materialize.assert_called_once()
    _, kwargs = fs.materialize.call_args
    assert kwargs.get("version") == "v2"
    assert fs.materialize.call_args.args[2] == ["driver_hourly_stats"]


@patch("feast.feature_server.assert_permissions")
def test_materialize_incremental_sync_forwards_version(_mock_perms):
    fs = _mock_store_for_materialize()
    client = TestClient(get_app(fs))

    response = client.post(
        "/materialize-incremental",
        json={
            "end_ts": "2021-01-02T00:00:00",
            "feature_views": ["driver_hourly_stats"],
            "version": "v2",
        },
    )

    assert response.status_code == 200
    fs._validate_materialize_version.assert_called_with("v2", ["driver_hourly_stats"])
    fs.materialize_incremental.assert_called_once()
    _, kwargs = fs.materialize_incremental.call_args
    assert kwargs.get("version") == "v2"
    assert fs.materialize_incremental.call_args.args[1] == ["driver_hourly_stats"]


@patch("feast.feature_server.assert_permissions")
def test_materialize_async_uses_dedicated_executor(_mock_perms):
    """Async materialize must not use the shared default pool (executor=None)."""
    fs = _mock_store_for_materialize()
    seen: dict = {}

    def capture_executor(loop, executor, func, *args):
        seen["executor"] = executor
        return _run_executor_inline(loop, executor, func, *args)

    with patch(
        "asyncio.BaseEventLoop.run_in_executor",
        new=capture_executor,
    ):
        client = TestClient(get_app(fs))
        response = client.post(
            "/materialize?async=true",
            json={
                "start_ts": "2021-01-01T00:00:00",
                "end_ts": "2021-01-02T00:00:00",
                "feature_views": ["driver_hourly_stats"],
            },
        )

    assert response.status_code == 202
    assert seen.get("executor") is not None
    assert isinstance(seen["executor"], ThreadPoolExecutor)
    assert "feast-materialize" in seen["executor"]._thread_name_prefix


@patch(
    "asyncio.BaseEventLoop.run_in_executor",
    new=_run_executor_inline,
)
@patch("feast.feature_server.assert_permissions")
def test_materialize_async_forwards_version(_mock_perms):
    fs = _mock_store_for_materialize()
    client = TestClient(get_app(fs))

    response = client.post(
        "/materialize?async=true",
        json={
            "start_ts": "2021-01-01T00:00:00",
            "end_ts": "2021-01-02T00:00:00",
            "feature_views": ["driver_hourly_stats"],
            "version": "v2",
        },
    )

    assert response.status_code == 202
    fs.materialize.assert_called_once()
    _, kwargs = fs.materialize.call_args
    assert kwargs.get("version") == "v2"


@patch(
    "asyncio.BaseEventLoop.run_in_executor",
    new=_run_executor_inline,
)
@patch("feast.feature_server.assert_permissions")
def test_materialize_incremental_async_forwards_version(_mock_perms):
    fs = _mock_store_for_materialize()
    client = TestClient(get_app(fs))

    response = client.post(
        "/materialize-incremental?async=true",
        json={
            "end_ts": "2021-01-02T00:00:00",
            "feature_views": ["driver_hourly_stats"],
            "version": "v2",
        },
    )

    assert response.status_code == 202
    fs.materialize_incremental.assert_called_once()
    _, kwargs = fs.materialize_incremental.call_args
    assert kwargs.get("version") == "v2"
