# Copyright 2025 The Feast Authors
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

"""
Unit tests for MongoDB connection utilities.
"""

import pytest
from unittest.mock import MagicMock, patch

from feast.infra.utils.mongodb.connection_utils import (
    get_mongo_client,
    get_async_mongo_client,
    is_atlas,
    _parse_read_preference,
    get_collection_name,
)

try:
    from pymongo.read_preferences import ReadPreference
except ImportError:
    pytest.skip("pymongo not installed", allow_module_level=True)


class TestReadPreferenceParsing:
    """Test read preference parsing."""

    def test_primary(self):
        """Test parsing 'primary' read preference."""
        result = _parse_read_preference("primary")
        assert result == ReadPreference.PRIMARY

    def test_primary_preferred(self):
        """Test parsing 'primaryPreferred' read preference."""
        result = _parse_read_preference("primaryPreferred")
        assert result == ReadPreference.PRIMARY_PREFERRED

    def test_secondary(self):
        """Test parsing 'secondary' read preference."""
        result = _parse_read_preference("secondary")
        assert result == ReadPreference.SECONDARY

    def test_secondary_preferred(self):
        """Test parsing 'secondaryPreferred' read preference."""
        result = _parse_read_preference("secondaryPreferred")
        assert result == ReadPreference.SECONDARY_PREFERRED

    def test_nearest(self):
        """Test parsing 'nearest' read preference."""
        result = _parse_read_preference("nearest")
        assert result == ReadPreference.NEAREST

    def test_invalid_preference(self):
        """Test that invalid preference raises ValueError."""
        with pytest.raises(ValueError, match="Invalid read preference"):
            _parse_read_preference("invalid_preference")


class TestCollectionNameGeneration:
    """Test collection name generation."""

    def test_basic_names(self):
        """Test basic project and table names."""
        result = get_collection_name("my_project", "my_table")
        assert result == "my_project_my_table"

    def test_sanitizes_dollar_sign(self):
        """Test that dollar signs are replaced."""
        result = get_collection_name("project$name", "table$name")
        assert result == "project_name_table_name"
        assert "$" not in result

    def test_sanitizes_null_character(self):
        """Test that null characters are removed."""
        result = get_collection_name("project\x00name", "table\x00name")
        assert result == "projectname_tablename"
        assert "\x00" not in result

    def test_handles_underscores(self):
        """Test that underscores are preserved."""
        result = get_collection_name("my_project", "my_table")
        assert result == "my_project_my_table"


class TestMongoClientCreation:
    """Test MongoDB client creation."""

    @patch("feast.infra.utils.mongodb.connection_utils.MongoClient")
    def test_get_mongo_client_default_params(self, mock_mongo_client):
        """Test client creation with default parameters."""
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        connection_string = "mongodb://localhost:27017"
        client = get_mongo_client(connection_string)

        # Verify MongoClient was called with correct parameters
        mock_mongo_client.assert_called_once()
        call_kwargs = mock_mongo_client.call_args[1]

        assert call_kwargs["maxPoolSize"] == 50
        assert call_kwargs["minPoolSize"] == 10
        assert call_kwargs["w"] == 1

        # Verify ping was called to test connection
        mock_client_instance.admin.command.assert_called_once_with("ping")

    @patch("feast.infra.utils.mongodb.connection_utils.MongoClient")
    def test_get_mongo_client_custom_params(self, mock_mongo_client):
        """Test client creation with custom parameters."""
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        connection_string = "mongodb://localhost:27017"
        client = get_mongo_client(
            connection_string,
            max_pool_size=100,
            min_pool_size=20,
            write_concern_w=2,
            read_preference="primary",
        )

        # Verify MongoClient was called with custom parameters
        mock_mongo_client.assert_called_once()
        call_kwargs = mock_mongo_client.call_args[1]

        assert call_kwargs["maxPoolSize"] == 100
        assert call_kwargs["minPoolSize"] == 20
        assert call_kwargs["w"] == 2
        assert call_kwargs["readPreference"] == ReadPreference.PRIMARY

    @patch("feast.infra.utils.mongodb.connection_utils.MongoClient")
    def test_get_mongo_client_connection_failure(self, mock_mongo_client):
        """Test that connection failures raise ConnectionError."""
        mock_client_instance = MagicMock()
        mock_client_instance.admin.command.side_effect = Exception("Connection failed")
        mock_mongo_client.return_value = mock_client_instance

        connection_string = "mongodb://invalid:27017"

        with pytest.raises(ConnectionError, match="Failed to connect to MongoDB"):
            get_mongo_client(connection_string)

    @patch("feast.infra.utils.mongodb.connection_utils.AsyncMongoClient")
    def test_get_async_mongo_client(self, mock_async_mongo_client):
        """Test async client creation."""
        mock_client_instance = MagicMock()
        mock_async_mongo_client.return_value = mock_client_instance

        connection_string = "mongodb://localhost:27017"
        client = get_async_mongo_client(connection_string)

        # Verify AsyncMongoClient was called
        mock_async_mongo_client.assert_called_once()
        call_kwargs = mock_async_mongo_client.call_args[1]

        assert call_kwargs["maxPoolSize"] == 50
        assert call_kwargs["minPoolSize"] == 10

    @patch("feast.infra.utils.mongodb.connection_utils.AsyncMongoClient")
    def test_get_async_mongo_client_connection_failure(self, mock_async_mongo_client):
        """Test that async client creation failures raise ConnectionError."""
        mock_async_mongo_client.side_effect = Exception("Connection failed")

        connection_string = "mongodb://invalid:27017"

        with pytest.raises(ConnectionError, match="Failed to create async MongoDB client"):
            get_async_mongo_client(connection_string)


class TestAtlasDetection:
    """Test MongoDB Atlas detection."""

    def test_is_atlas_with_enterprise_module(self):
        """Test Atlas detection with enterprise module."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {
            "version": "5.0.0",
            "modules": ["enterprise"],
        }

        result = is_atlas(mock_client)
        assert result is True

    def test_is_atlas_with_atlas_in_build_info(self):
        """Test Atlas detection with 'atlas' in build info."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {
            "version": "5.0.0-atlas",
            "modules": [],
        }

        result = is_atlas(mock_client)
        assert result is True

    def test_is_not_atlas(self):
        """Test non-Atlas deployment detection."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {
            "version": "5.0.0",
            "modules": [],
        }

        result = is_atlas(mock_client)
        assert result is False

    def test_is_atlas_exception_handling(self):
        """Test that exceptions in Atlas detection return False."""
        mock_client = MagicMock()
        mock_client.admin.command.side_effect = Exception("Command failed")

        result = is_atlas(mock_client)
        assert result is False

    @pytest.mark.asyncio
    async def test_is_atlas_async(self):
        """Test async Atlas detection."""
        from feast.infra.utils.mongodb.connection_utils import is_atlas_async

        mock_client = MagicMock()
        mock_client.admin.command = MagicMock(
            return_value={
                "version": "5.0.0",
                "modules": ["enterprise"],
            }
        )

        result = await is_atlas_async(mock_client)
        assert result is True
