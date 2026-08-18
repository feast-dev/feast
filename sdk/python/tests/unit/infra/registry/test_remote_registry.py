# Copyright 2024 The Feast Authors
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

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from feast.infra.registry.remote import RemoteRegistry
from feast.protos.feast.registry import RegistryServer_pb2


@pytest.fixture
def remote_registry():
    with patch.object(RemoteRegistry, "__init__", return_value=None):
        registry = RemoteRegistry.__new__(RemoteRegistry)
        registry.stub = MagicMock()
        registry.stub.ListAllFeatureViews.return_value = (
            RegistryServer_pb2.ListAllFeatureViewsResponse(feature_views=[])
        )
        yield registry


def _captured_updated_since(remote_registry) -> Timestamp:
    """Return the updated_since Timestamp from the last ListAllFeatureViews call."""
    call_args = remote_registry.stub.ListAllFeatureViews.call_args
    request: RegistryServer_pb2.ListAllFeatureViewsRequest = call_args[0][0]
    return request.updated_since


def test_updated_since_utc_aware(remote_registry):
    """A UTC-aware datetime is encoded to the correct UTC epoch seconds."""
    dt = datetime(2024, 6, 1, 17, 0, 0, tzinfo=timezone.utc)
    remote_registry.list_all_feature_views("project", updated_since=dt)

    ts = _captured_updated_since(remote_registry)
    assert ts.seconds == int(dt.timestamp())


def test_updated_since_non_utc_aware(remote_registry):
    """A non-UTC tz-aware datetime is converted to the correct UTC epoch, not treated as UTC."""
    est = timezone(timedelta(hours=-5))
    # 2024-06-01 12:00 EST == 2024-06-01 17:00 UTC
    dt_est = datetime(2024, 6, 1, 12, 0, 0, tzinfo=est)
    dt_utc = datetime(2024, 6, 1, 17, 0, 0, tzinfo=timezone.utc)

    remote_registry.list_all_feature_views("project", updated_since=dt_est)

    ts = _captured_updated_since(remote_registry)
    assert ts.seconds == int(dt_utc.timestamp()), (
        "Non-UTC datetime must be converted to UTC before encoding, "
        "not have its tzinfo stripped (which would misinterpret 12:00 EST as 12:00 UTC)"
    )


def test_updated_since_naive_datetime(remote_registry):
    """A naive datetime is treated as UTC by protobuf's FromDatetime."""
    dt_naive = datetime(2024, 6, 1, 17, 0, 0)
    dt_utc = datetime(2024, 6, 1, 17, 0, 0, tzinfo=timezone.utc)
    remote_registry.list_all_feature_views("project", updated_since=dt_naive)

    ts = _captured_updated_since(remote_registry)
    assert ts.seconds == int(dt_utc.timestamp())


def test_updated_since_none(remote_registry):
    """When updated_since is None, the field is not set in the request."""
    remote_registry.list_all_feature_views("project", updated_since=None)

    request: RegistryServer_pb2.ListAllFeatureViewsRequest = (
        remote_registry.stub.ListAllFeatureViews.call_args[0][0]
    )
    assert not request.HasField("updated_since")


@patch("feast.infra.registry.remote.grpc.insecure_channel")
def test_remote_registry_channel_options(mock_insecure_channel):
    from feast.infra.registry.remote import RemoteRegistryConfig

    config = RemoteRegistryConfig(
        path="localhost:50051",
        keepalive_time_ms=10000,
        keepalive_timeout_ms=5000,
    )
    # We patch grpc.intercept_channel to avoid auth interceptor type checks during test
    with patch("feast.infra.registry.remote.grpc.intercept_channel"):
        RemoteRegistry(config, project="test", repo_path=None)

    mock_insecure_channel.assert_called_once()
    args, kwargs = mock_insecure_channel.call_args
    assert args[0] == "localhost:50051"
    options = kwargs.get("options", [])
    assert ("grpc.keepalive_time_ms", 10000) in options
    assert ("grpc.keepalive_timeout_ms", 5000) in options


def test_remote_registry_client_timeout_interceptor():
    from feast.permissions.auth_model import AuthConfig
    from feast.permissions.client.grpc_client_auth_interceptor import (
        GrpcClientAuthHeaderInterceptor,
    )

    # Setup interceptor with a default timeout of 10 seconds
    interceptor = GrpcClientAuthHeaderInterceptor(
        AuthConfig(type="no_auth"), timeout=10
    )

    # Mock continuation function
    mock_continuation = MagicMock()

    # Mock ClientCallDetails
    class MockClientCallDetails:
        def __init__(self, timeout=None):
            self.timeout = timeout

        def _replace(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
            return self

    call_details = MockClientCallDetails(timeout=None)

    # Call handle_call
    interceptor._handle_call(mock_continuation, call_details, None)

    # Verify continuation was called with modified call_details carrying the default float timeout
    mock_continuation.assert_called_once()
    passed_details = mock_continuation.call_args[0][0]
    assert passed_details.timeout == 10.0

    # If call details already had a timeout, it should NOT be overridden
    mock_continuation.reset_mock()
    call_details_with_timeout = MockClientCallDetails(timeout=5.0)
    interceptor._handle_call(mock_continuation, call_details_with_timeout, None)
    passed_details_with_timeout = mock_continuation.call_args[0][0]
    assert passed_details_with_timeout.timeout == 5.0


def test_remote_registry_validation_positive_values():
    from pydantic import ValidationError

    from feast.infra.registry.remote import RemoteRegistryConfig

    # Valid configurations should work
    config = RemoteRegistryConfig(
        path="localhost:50051",
        timeout=5,
        keepalive_time_ms=1000,
        keepalive_timeout_ms=500,
    )
    assert config.timeout == 5
    assert config.keepalive_time_ms == 1000
    assert config.keepalive_timeout_ms == 500

    # Invalid values should throw ValidationError
    for field in ["timeout", "keepalive_time_ms", "keepalive_timeout_ms"]:
        for bad_val in [0, -1]:
            with pytest.raises(ValidationError):
                RemoteRegistryConfig(path="localhost:50051", **{field: bad_val})
