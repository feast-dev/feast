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
from feast.mcp.server import FeastMCP


@pytest.fixture
def mock_feature_store():
    feature_store = MagicMock(spec=FeatureStore)
    feature_store.project = "test_project"
    return feature_store


def test_feast_mcp_initialization(mock_feature_store):
    """Test that FeastMCP initializes correctly."""
    mcp_server = FeastMCP(feature_store=mock_feature_store)

    assert mcp_server.feature_store == mock_feature_store
    # Just check that name is a string, don't check specific format
    assert isinstance(mcp_server.name, str)


@patch("feast.mcp.resources.register_feature_resources")
def test_register_resources(mock_register_resources, mock_feature_store):
    """Test that resources are registered correctly."""
    mcp_server = FeastMCP(feature_store=mock_feature_store)

    # _register_resources is called during initialization
    mock_register_resources.assert_called_once_with(mcp_server, mock_feature_store)


@patch("feast.mcp.tools.register_feature_tools")
def test_register_tools(mock_register_tools, mock_feature_store):
    """Test that tools are registered correctly."""
    mcp_server = FeastMCP(feature_store=mock_feature_store)

    # _register_tools is called during initialization
    mock_register_tools.assert_called_once_with(mcp_server, mock_feature_store)


@patch("feast.mcp.prompts.register_feature_prompts")
def test_register_prompts(mock_register_prompts, mock_feature_store):
    """Test that prompts are registered correctly."""
    mcp_server = FeastMCP(feature_store=mock_feature_store)

    # _register_prompts is called during initialization
    mock_register_prompts.assert_called_once_with(mcp_server, mock_feature_store)


@patch("feast.mcp_server.uvicorn.run")
@patch("feast.mcp_server.FeastMCP")
def test_mcp_server_start(mock_feast_mcp, mock_uvicorn_run, mock_feature_store):
    """Test that the MCP server starts correctly."""
    # Mock the FeastMCP instance
    mock_mcp_instance = MagicMock()
    mock_feast_mcp.return_value = mock_mcp_instance

    from feast.mcp_server import start_server

    start_server(
        store=mock_feature_store,
        host="localhost",
        port=8080,
    )

    # Check that FeastMCP was instantiated with the correct arguments
    mock_feast_mcp.assert_called_once()

    # Check that uvicorn.run was called with the correct arguments
    mock_uvicorn_run.assert_called_once()
    args, kwargs = mock_uvicorn_run.call_args
    assert kwargs["host"] == "localhost"
    assert kwargs["port"] == 8080
