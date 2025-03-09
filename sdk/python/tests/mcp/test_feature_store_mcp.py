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

from feast.feature_store import FeatureStore


@patch("feast.mcp_server.start_server")
def test_serve_mcp(mock_start_server):
    """Test that serve_mcp calls the start_server function with the correct arguments."""
    # Create a mock feature store
    feature_store = MagicMock(spec=FeatureStore)
    
    # Call serve_mcp
    feature_store.serve_mcp(
        host="localhost",
        port=8080,
        registry_ttl_sec=5,
        tls_cert_path="cert.pem",
        tls_key_path="key.pem",
        enable_auth=True,
        enable_metrics=True,
        cors_origins=["http://localhost:3000"],
    )
    
    # Check that start_server was called with the correct arguments
    mock_start_server.assert_called_once_with(
        feature_store,
        host="localhost",
        port=8080,
        registry_ttl_sec=5,
        tls_cert_path="cert.pem",
        tls_key_path="key.pem",
        enable_auth=True,
        enable_metrics=True,
        cors_origins=["http://localhost:3000"],
    )


@patch("feast.feature_store.warnings.warn")
@patch("feast.feature_store.flags_helper.is_test", return_value=True)
@patch("feast.mcp_server.start_server")
def test_serve_mcp_warning(mock_start_server, mock_is_test, mock_warn):
    """Test that serve_mcp shows a warning when in test mode."""
    # Create a mock feature store
    feature_store = MagicMock(spec=FeatureStore)
    
    # Call serve_mcp
    feature_store.serve_mcp(
        host="localhost",
        port=8080,
    )
    
    # Check that a warning was shown
    mock_warn.assert_called_once()
    warning_message = mock_warn.call_args[0][0]
    assert "experimental feature" in warning_message
    
    # Check that start_server was called
    mock_start_server.assert_called_once()
