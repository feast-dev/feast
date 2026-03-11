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

from feast import FeatureView, Field
from feast.entity import Entity
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.snowflake import SnowflakeRegistry
from feast.types import Float32


def _make_feature_view(name: str, entity: Entity, source: FileSource) -> FeatureView:
    return FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=source,
    )


@pytest.fixture
def mock_snowflake_registry():
    """Return a SnowflakeRegistry whose Snowflake connection is fully mocked."""
    config = MagicMock()
    config.path = "snowflake://account/db/schema"
    config.registry_type = "snowflake"
    config.cache_ttl_seconds = 0
    config.cache_mode = "sync"

    with patch(
        "feast.infra.registry.snowflake.SnowflakeRegistry.__init__",
        return_value=None,
    ):
        registry = SnowflakeRegistry.__new__(SnowflakeRegistry)
        registry.registry_config = config
        registry.registry_path = "db.schema"
        registry.cache_mode = "sync"
        yield registry


def _make_mock_fv(name: str, updated_at: datetime) -> MagicMock:
    fv = MagicMock()
    fv.name = name
    # Snowflake stores timestamps as offset-naive UTC after round-tripping through proto
    fv.last_updated_timestamp = updated_at.replace(tzinfo=None)
    fv.tags = {}
    return fv


def test_list_all_feature_views_updated_since_no_cache(mock_snowflake_registry):
    """updated_since filters Python-side when allow_cache=False."""
    old_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    new_ts = datetime(2024, 6, 1, tzinfo=timezone.utc)

    old_fv = _make_mock_fv("old_view", old_ts)
    new_fv = _make_mock_fv("new_view", new_ts)

    mock_snowflake_registry.list_feature_views = MagicMock(
        return_value=[old_fv, new_fv]
    )
    mock_snowflake_registry.list_stream_feature_views = MagicMock(return_value=[])
    mock_snowflake_registry.list_on_demand_feature_views = MagicMock(return_value=[])

    cutoff = datetime(2023, 1, 1, tzinfo=timezone.utc)
    result = mock_snowflake_registry.list_all_feature_views(
        "project", allow_cache=False, updated_since=cutoff
    )
    assert [fv.name for fv in result] == ["new_view"]


def test_list_all_feature_views_updated_since_no_filter(mock_snowflake_registry):
    """Without updated_since, all feature views are returned."""
    ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    fv1 = _make_mock_fv("view_a", ts)
    fv2 = _make_mock_fv("view_b", ts)

    mock_snowflake_registry.list_feature_views = MagicMock(return_value=[fv1, fv2])
    mock_snowflake_registry.list_stream_feature_views = MagicMock(return_value=[])
    mock_snowflake_registry.list_on_demand_feature_views = MagicMock(return_value=[])

    result = mock_snowflake_registry.list_all_feature_views(
        "project", allow_cache=False
    )
    assert len(result) == 2


def test_list_all_feature_views_updated_since_future_returns_empty(
    mock_snowflake_registry,
):
    """A future cutoff returns no feature views."""
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    fv = _make_mock_fv("any_view", ts)

    mock_snowflake_registry.list_feature_views = MagicMock(return_value=[fv])
    mock_snowflake_registry.list_stream_feature_views = MagicMock(return_value=[])
    mock_snowflake_registry.list_on_demand_feature_views = MagicMock(return_value=[])

    future = datetime(2999, 1, 1, tzinfo=timezone.utc)
    result = mock_snowflake_registry.list_all_feature_views(
        "project", allow_cache=False, updated_since=future
    )
    assert result == []


def test_list_all_feature_views_updated_since_non_utc_tz(mock_snowflake_registry):
    """A non-UTC tz-aware cutoff is converted to UTC before comparing against naive-UTC timestamps."""
    est = timezone(timedelta(hours=-5))
    # 2023-01-01 00:00 EST == 2023-01-01 05:00 UTC
    cutoff_est = datetime(2023, 1, 1, 0, 0, 0, tzinfo=est)

    # Feature view updated at 2023-01-01 03:00 UTC — after midnight EST but before 05:00 UTC
    ts_between = datetime(2023, 1, 1, 3, 0, 0, tzinfo=timezone.utc)
    # Feature view updated at 2023-01-01 06:00 UTC — after both midnight EST and 05:00 UTC
    ts_after = datetime(2023, 1, 1, 6, 0, 0, tzinfo=timezone.utc)

    fv_between = _make_mock_fv("view_between", ts_between)
    fv_after = _make_mock_fv("view_after", ts_after)

    mock_snowflake_registry.list_feature_views = MagicMock(
        return_value=[fv_between, fv_after]
    )
    mock_snowflake_registry.list_stream_feature_views = MagicMock(return_value=[])
    mock_snowflake_registry.list_on_demand_feature_views = MagicMock(return_value=[])

    result = mock_snowflake_registry.list_all_feature_views(
        "project", allow_cache=False, updated_since=cutoff_est
    )
    # Only view_after is at or after 05:00 UTC; view_between (03:00 UTC) should be excluded
    assert [fv.name for fv in result] == ["view_after"]
