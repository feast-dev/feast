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
from feast.mcp.prompts import register_feature_prompts


@pytest.fixture
def mock_feature_store():
    """Create a mock feature store for testing."""
    feature_store = MagicMock(spec=FeatureStore)
    feature_store.project = "test_project"
    
    # Mock feature views
    feature_view = MagicMock()
    feature_view.name = "test_feature_view"
    feature_view.description = "Test feature view"
    
    # Mock entities
    entity = MagicMock()
    entity.name = "test_entity"
    entity.description = "Test entity"
    
    # Mock feature services
    feature_service = MagicMock()
    feature_service.name = "test_feature_service"
    feature_service.description = "Test feature service"
    
    # Mock data sources
    data_source = MagicMock()
    data_source.name = "test_data_source"
    data_source.description = "Test data source"
    
    # Set up mock returns
    feature_store.list_feature_views.return_value = [feature_view]
    feature_store.list_entities.return_value = [entity]
    feature_store.list_feature_services.return_value = [feature_service]
    feature_store.list_data_sources.return_value = [data_source]
    
    return feature_store


def test_register_feature_prompts(mock_feature_store):
    """Test that feature prompts are registered correctly."""
    mock_mcp = MagicMock()
    
    # Call the function to register prompts
    register_feature_prompts(mock_mcp, mock_feature_store)
    
    # Check that prompts were registered
    assert mock_mcp.prompt.call_count == 3
    
    # Check prompt names
    prompt_names = [call.args[0] for call in mock_mcp.prompt.call_args_list]
    assert "feast_feature_store_overview" in prompt_names
    assert "feast_feature_retrieval_guide" in prompt_names
    assert "feast_feature_exploration" in prompt_names
