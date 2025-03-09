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


def test_serve_mcp():
    """Test that serve_mcp calls the start_server function with the correct arguments."""
    # Create a mock feature store
    feature_store = FeatureStore
    
    # Mock the start_server function
    with patch("feast.feature_store.mcp_server") as mock_mcp_server:
        # Create an instance method for serve_mcp
        original_serve_mcp = FeatureStore.serve_mcp
        
        try:
            # Call serve_mcp
            original_serve_mcp(
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
            
            # Check that start_server was called with the correct arguments
            mock_mcp_server.start_server.assert_called_once()
            args, kwargs = mock_mcp_server.start_server.call_args
            
            assert args[0] == feature_store
            assert kwargs["host"] == "localhost"
            assert kwargs["port"] == 8080
            assert kwargs["registry_ttl_sec"] == 5
            assert kwargs["tls_cert_path"] == "cert.pem"
            assert kwargs["tls_key_path"] == "key.pem"
            assert kwargs["enable_auth"] is True
            assert kwargs["enable_metrics"] is True
            assert kwargs["cors_origins"] == ["http://localhost:3000"]
        finally:
            # Restore the original method
            FeatureStore.serve_mcp = original_serve_mcp


def test_serve_mcp_warning():
    """Test that serve_mcp shows a warning when in test mode."""
    # Create a mock feature store
    feature_store = FeatureStore
    
    # Mock the dependencies
    with patch("feast.feature_store.warnings.warn") as mock_warn, \
         patch("feast.feature_store.flags_helper.is_test", return_value=True), \
         patch("feast.feature_store.mcp_server"):
        
        # Create an instance method for serve_mcp
        original_serve_mcp = FeatureStore.serve_mcp
        
        try:
            # Call serve_mcp
            original_serve_mcp(
                feature_store,
                host="localhost",
                port=8080,
            )
            
            # Check that a warning was shown
            mock_warn.assert_called_once()
            warning_message = mock_warn.call_args[0][0]
            assert "experimental feature" in warning_message
        finally:
            # Restore the original method
            FeatureStore.serve_mcp = original_serve_mcp
