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
from datetime import datetime

import pytest
import pandas as pd

from feast.feature_store import FeatureStore
from feast.mcp.tools import register_feature_tools
from feast.data_source import PushMode


@pytest.fixture
def mock_feature_store():
    """Create a mock feature store for testing."""
    feature_store = MagicMock(spec=FeatureStore)
    feature_store.project = "test_project"
    
    # Mock feature view
    feature_view = MagicMock()
    feature_view.name = "test_feature_view"
    feature_view.description = "Test feature view"
    feature_view.features = [MagicMock(name="feature1", dtype="float"), MagicMock(name="feature2", dtype="int")]
    feature_view.entities = ["entity1", "entity2"]
    feature_view.ttl = "86400s"
    feature_view.online = True
    
    # Set up mock returns
    feature_store.get_feature_view.return_value = feature_view
    
    # Mock get_online_features
    online_response = MagicMock()
    online_response.to_dict.return_value = {
        "feature1": [0.5],
        "feature2": [10]
    }
    feature_store.get_online_features.return_value = online_response
    
    return feature_store


def test_register_feature_tools(mock_feature_store):
    """Test that feature tools are registered correctly."""
    mock_mcp = MagicMock()
    
    # Call the function to register tools
    register_feature_tools(mock_mcp, mock_feature_store)
    
    # Check that tools were registered
    assert mock_mcp.tool.call_count == 5
    
    # Check that the tool decorator was called
    mock_mcp.tool.assert_called()
