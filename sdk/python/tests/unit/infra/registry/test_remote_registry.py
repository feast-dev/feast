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
