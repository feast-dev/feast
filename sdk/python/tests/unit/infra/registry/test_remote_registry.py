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

from feast import Entity, FeatureView, Field, FileSource
from feast.infra.registry.caching_registry import CachingRegistry
from feast.infra.registry.remote import RemoteRegistry
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.registry import RegistryServer_pb2
from feast.types import Int64
from feast.value_type import ValueType


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


PROJECT = "demo"


def _registry_proto_with_feature_view() -> tuple[RegistryProto, object]:
    """A RegistryProto holding one feature view, as the server's Proto RPC returns."""
    entity = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    source = FileSource(name="src", path="foo.parquet", timestamp_field="ts")
    fv = FeatureView(
        name="my_fv",
        entities=[entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Int64),
        ],
        source=source,
    )
    fv_proto = fv.to_proto()
    fv_proto.spec.project = PROJECT
    registry_proto = RegistryProto()
    registry_proto.feature_views.append(fv_proto)
    return registry_proto, fv_proto


@pytest.fixture
def cached_remote_registry():
    """A RemoteRegistry with its client-side cache warmed from a mocked server."""
    registry_proto, fv_proto = _registry_proto_with_feature_view()
    stub = MagicMock()
    stub.Proto.return_value = registry_proto
    stub.GetFeatureView.return_value = fv_proto

    registry = RemoteRegistry.__new__(RemoteRegistry)
    registry.stub = stub
    CachingRegistry.__init__(
        registry, project=PROJECT, cache_ttl_seconds=600, cache_mode="sync"
    )
    return registry


def test_remote_registry_is_a_caching_registry(cached_remote_registry):
    assert isinstance(cached_remote_registry, CachingRegistry)


def test_allow_cache_true_serves_from_client_cache(cached_remote_registry):
    """allow_cache=True must not issue one RPC per read (feast#6672)."""
    stub = cached_remote_registry.stub
    proto_calls_after_warmup = stub.Proto.call_count
    stub.GetFeatureView.reset_mock()

    for _ in range(100):
        fv = cached_remote_registry.get_feature_view("my_fv", PROJECT, allow_cache=True)
        assert fv.name == "my_fv"

    assert stub.GetFeatureView.call_count == 0, (
        "allow_cache=True reads must be served from the client cache, not gRPC"
    )
    # No extra full-registry refreshes within the TTL either.
    assert stub.Proto.call_count == proto_calls_after_warmup


def test_allow_cache_false_still_issues_rpc(cached_remote_registry):
    """allow_cache=False keeps the fresh-read contract: one RPC per read."""
    stub = cached_remote_registry.stub
    stub.GetFeatureView.reset_mock()

    for _ in range(5):
        cached_remote_registry.get_feature_view("my_fv", PROJECT, allow_cache=False)

    assert stub.GetFeatureView.call_count == 5


def test_is_cache_valid_true_after_warmup(cached_remote_registry):
    """RemoteRegistry now advertises a valid client cache, unlike before."""
    assert cached_remote_registry.is_cache_valid() is True


def test_explicit_refresh_reloads_cache_and_pokes_server(cached_remote_registry):
    """A project-scoped refresh reloads the client cache and pokes the server."""
    stub = cached_remote_registry.stub
    stub.Refresh.reset_mock()
    proto_calls_before = stub.Proto.call_count

    cached_remote_registry.refresh(PROJECT)

    # Client cache reloaded via the Proto RPC ...
    assert stub.Proto.call_count == proto_calls_before + 1
    # ... and the historical server-side refresh side effect is preserved.
    assert stub.Refresh.call_count == 1


def test_internal_refresh_reloads_cache_without_poking_server(cached_remote_registry):
    """A refresh with no project (internal TTL path) must not RPC Refresh.

    _refresh_cached_registry_if_necessary calls refresh() with no project, so a
    server-side Refresh here would both add load and, if it raised, abort the
    client-cache reload that keeps allow_cache reads correct (feast#6672).
    """
    stub = cached_remote_registry.stub
    stub.Refresh.reset_mock()
    proto_calls_before = stub.Proto.call_count

    cached_remote_registry.refresh()

    assert stub.Proto.call_count == proto_calls_before + 1
    assert stub.Refresh.call_count == 0
