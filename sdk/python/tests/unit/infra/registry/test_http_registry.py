# Copyright 2021 The Feast Authors
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

from unittest.mock import MagicMock, patch

import httpx
import pytest
from httpx import HTTPStatusError

from feast.errors import (
    FeatureViewNotFoundException,
    SortedFeatureViewNotFoundException,
)
from feast.feature_view import FeatureView
from feast.infra.registry.http import HttpRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.sorted_feature_view import SortedFeatureView


@pytest.fixture
def http_registry():
    """Create an HttpRegistry with mocked HTTP client and background thread."""
    with (
        patch.object(HttpRegistry, "__init__", lambda self, *a, **kw: None),
        patch.object(HttpRegistry, "proto", return_value=MagicMock()),
    ):
        registry = HttpRegistry.__new__(HttpRegistry)
        registry.base_url = "http://test-registry"
        registry.http_client = MagicMock()
        registry.project = "test_project"
        registry.cached_registry_proto = MagicMock()
        registry.cached_registry_proto_created = None
        registry.cached_registry_proto_ttl = MagicMock(
            total_seconds=MagicMock(return_value=0)
        )
        yield registry


class TestGetAnyFeatureView:
    """Tests for HttpRegistry.get_any_feature_view"""

    def test_returns_feature_view_when_found(self, http_registry):
        """Test that a FeatureView is returned when get_feature_view succeeds."""
        mock_fv = MagicMock(spec=FeatureView)
        with patch.object(http_registry, "get_feature_view", return_value=mock_fv):
            result = http_registry.get_any_feature_view(
                "my_fv", "test_project", allow_cache=False
            )
        assert result is mock_fv

    def test_returns_sorted_feature_view_when_fv_not_found(self, http_registry):
        """Test fallback to SortedFeatureView when FeatureView is not found."""
        mock_sfv = MagicMock(spec=SortedFeatureView)
        with (
            patch.object(
                http_registry,
                "get_feature_view",
                side_effect=FeatureViewNotFoundException("my_sfv", "test_project"),
            ),
            patch.object(
                http_registry, "get_sorted_feature_view", return_value=mock_sfv
            ),
        ):
            result = http_registry.get_any_feature_view(
                "my_sfv", "test_project", allow_cache=False
            )
        assert result is mock_sfv

    def test_returns_on_demand_feature_view_when_fv_and_sfv_not_found(
        self, http_registry
    ):
        """Test fallback to OnDemandFeatureView when FeatureView and SortedFeatureView are not found."""
        mock_odfv = MagicMock(spec=OnDemandFeatureView)
        with (
            patch.object(
                http_registry,
                "get_feature_view",
                side_effect=FeatureViewNotFoundException("my_odfv", "test_project"),
            ),
            patch.object(
                http_registry,
                "get_sorted_feature_view",
                side_effect=SortedFeatureViewNotFoundException(
                    "my_odfv", "test_project"
                ),
            ),
            patch.object(
                http_registry, "get_on_demand_feature_view", return_value=mock_odfv
            ),
        ):
            result = http_registry.get_any_feature_view(
                "my_odfv", "test_project", allow_cache=False
            )
        assert result is mock_odfv

    def test_raises_not_found_when_no_view_exists(self, http_registry):
        """Test that FeatureViewNotFoundException is raised when no feature view type matches."""
        with (
            patch.object(
                http_registry,
                "get_feature_view",
                side_effect=FeatureViewNotFoundException("missing", "test_project"),
            ),
            patch.object(
                http_registry,
                "get_sorted_feature_view",
                side_effect=SortedFeatureViewNotFoundException(
                    "missing", "test_project"
                ),
            ),
            patch.object(
                http_registry,
                "get_on_demand_feature_view",
                side_effect=httpx.HTTPError("not found"),
            ),
            patch.object(
                http_registry,
                "get_stream_feature_view",
                side_effect=NotImplementedError("Method not implemented"),
            ),
        ):
            with pytest.raises(FeatureViewNotFoundException):
                http_registry.get_any_feature_view(
                    "missing", "test_project", allow_cache=False
                )

    def test_handles_http_status_error_on_feature_view(self, http_registry):
        """Test that HTTPStatusError from get_feature_view is caught and falls through."""
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_sfv = MagicMock(spec=SortedFeatureView)

        with (
            patch.object(
                http_registry,
                "get_feature_view",
                side_effect=HTTPStatusError(
                    "error", request=mock_request, response=mock_response
                ),
            ),
            patch.object(
                http_registry, "get_sorted_feature_view", return_value=mock_sfv
            ),
        ):
            result = http_registry.get_any_feature_view(
                "my_fv", "test_project", allow_cache=False
            )
        assert result is mock_sfv

    def test_handles_http_error_on_sorted_feature_view(self, http_registry):
        """Test that httpx.HTTPError from get_sorted_feature_view is caught and falls through."""
        mock_odfv = MagicMock(spec=OnDemandFeatureView)

        with (
            patch.object(
                http_registry,
                "get_feature_view",
                side_effect=FeatureViewNotFoundException("my_fv", "test_project"),
            ),
            patch.object(
                http_registry,
                "get_sorted_feature_view",
                side_effect=httpx.HTTPError("not found"),
            ),
            patch.object(
                http_registry, "get_on_demand_feature_view", return_value=mock_odfv
            ),
        ):
            result = http_registry.get_any_feature_view(
                "my_fv", "test_project", allow_cache=False
            )
        assert result is mock_odfv

    def test_cache_path_delegates_to_proto_utils(self, http_registry):
        """Test that allow_cache=True delegates to proto_registry_utils."""
        mock_fv = MagicMock(spec=FeatureView)
        with (
            patch.object(http_registry, "_check_if_registry_refreshed"),
            patch(
                "feast.infra.registry.http.proto_registry_utils.get_any_feature_view",
                return_value=mock_fv,
            ) as mock_proto_util,
        ):
            result = http_registry.get_any_feature_view(
                "my_fv", "test_project", allow_cache=True
            )
        assert result is mock_fv
        mock_proto_util.assert_called_once_with(
            http_registry.cached_registry_proto, "my_fv", "test_project"
        )

    def test_feature_view_found_does_not_try_other_types(self, http_registry):
        """Test that when FeatureView is found, no other get methods are called."""
        mock_fv = MagicMock(spec=FeatureView)
        with (
            patch.object(
                http_registry, "get_feature_view", return_value=mock_fv
            ) as mock_get_fv,
            patch.object(http_registry, "get_sorted_feature_view") as mock_get_sfv,
            patch.object(http_registry, "get_on_demand_feature_view") as mock_get_odfv,
            patch.object(http_registry, "get_stream_feature_view") as mock_get_stream,
        ):
            result = http_registry.get_any_feature_view(
                "my_fv", "test_project", allow_cache=False
            )

        assert result is mock_fv
        mock_get_fv.assert_called_once_with("my_fv", "test_project")
        mock_get_sfv.assert_not_called()
        mock_get_odfv.assert_not_called()
        mock_get_stream.assert_not_called()
