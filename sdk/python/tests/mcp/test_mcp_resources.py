# Copyright 2019 The Feast Authors
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

import pytest

from feast.feature_store import FeatureStore
from feast.mcp.resources import register_feature_resources


@pytest.fixture
def mock_feature_store():
    """Create a mock feature store for testing."""
    feature_store = MagicMock(spec=FeatureStore)
    feature_store.project = "test_project"
    
    # Mock feature views
    feature_view = MagicMock()
    feature_view.name = "test_feature_view"
    feature_view.description = "Test feature view"
    feature_view.features = [MagicMock(name="feature1"), MagicMock(name="feature2")]
    feature_view.entities = ["entity1", "entity2"]
    feature_view.ttl = "86400s"
    feature_view.online = True
    feature_view.tags = {"team": "ml"}
    
    # Mock entities
    entity = MagicMock()
    entity.name = "test_entity"
    entity.description = "Test entity"
    entity.value_type = "STRING"
    entity.join_key = "id"
    entity.tags = {"team": "ml"}
    
    # Mock feature services
    feature_service = MagicMock()
    feature_service.name = "test_feature_service"
    feature_service.description = "Test feature service"
    projection = MagicMock()
    projection.name = "test_feature_view"
    projection.features = [MagicMock(name="feature1")]
    feature_service.feature_view_projections = [projection]
    feature_service.tags = {"team": "ml"}
    
    # Mock data sources
    data_source = MagicMock()
    data_source.name = "test_data_source"
    data_source.description = "Test data source"
    data_source.tags = {"team": "ml"}
    
    # Set up mock returns
    feature_store.list_feature_views.return_value = [feature_view]
    feature_store.get_feature_view.return_value = feature_view
    feature_store.list_entities.return_value = [entity]
    feature_store.get_entity.return_value = entity
    feature_store.list_feature_services.return_value = [feature_service]
    feature_store.get_feature_service.return_value = feature_service
    feature_store.list_data_sources.return_value = [data_source]
    feature_store.get_data_source.return_value = data_source
    
    return feature_store


def test_register_feature_resources(mock_feature_store):
    """Test that feature resources are registered correctly."""
    mock_mcp = MagicMock()
    
    # Call the function to register resources
    register_feature_resources(mock_mcp, mock_feature_store)
    
    # Check that resources were registered
    assert mock_mcp.resource.call_count == 8
    
    # Check resource paths
    resource_paths = [call.args[0] for call in mock_mcp.resource.call_args_list]
    assert "feast://feature-views" in resource_paths
    assert "feast://feature-view/{name}" in resource_paths
    assert "feast://entities" in resource_paths
    assert "feast://entity/{name}" in resource_paths
    assert "feast://feature-services" in resource_paths
    assert "feast://feature-service/{name}" in resource_paths
    assert "feast://data-sources" in resource_paths
    assert "feast://data-source/{name}" in resource_paths
