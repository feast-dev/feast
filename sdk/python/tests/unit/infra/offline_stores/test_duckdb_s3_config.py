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

"""
Unit tests for DuckDB offline store S3 configuration.

These tests verify the S3 configuration functionality for DuckDB,
specifically the s3_url_style and related S3 settings that enable
compatibility with S3-compatible storage providers like MinIO.
"""

from unittest.mock import MagicMock, patch

import pytest

from feast.infra.offline_stores.duckdb import (
    DuckDBOfflineStoreConfig,
    _configure_duckdb_for_s3,
    _is_s3_path,
)


class TestDuckDBOfflineStoreConfig:
    """Tests for DuckDBOfflineStoreConfig S3 settings."""

    def test_default_config_has_no_s3_settings(self):
        """Test that default config has None for all S3 settings."""
        config = DuckDBOfflineStoreConfig()

        assert config.s3_url_style is None
        assert config.s3_endpoint is None
        assert config.s3_access_key_id is None
        assert config.s3_secret_access_key is None
        assert config.s3_region is None
        assert config.s3_use_ssl is None

    def test_s3_url_style_path(self):
        """Test that s3_url_style can be set to 'path'."""
        config = DuckDBOfflineStoreConfig(s3_url_style="path")
        assert config.s3_url_style == "path"

    def test_s3_url_style_vhost(self):
        """Test that s3_url_style can be set to 'vhost'."""
        config = DuckDBOfflineStoreConfig(s3_url_style="vhost")
        assert config.s3_url_style == "vhost"

    def test_s3_url_style_invalid_raises_error(self):
        """Test that invalid s3_url_style raises a validation error."""
        with pytest.raises(Exception):  # Pydantic validation error
            DuckDBOfflineStoreConfig(s3_url_style="invalid")

    def test_full_minio_config(self):
        """Test a full MinIO-compatible configuration."""
        config = DuckDBOfflineStoreConfig(
            s3_url_style="path",
            s3_endpoint="localhost:9000",
            s3_access_key_id="minioadmin",
            s3_secret_access_key="minioadmin",
            s3_region="us-east-1",
            s3_use_ssl=False,
        )

        assert config.s3_url_style == "path"
        assert config.s3_endpoint == "localhost:9000"
        assert config.s3_access_key_id == "minioadmin"
        assert config.s3_secret_access_key == "minioadmin"
        assert config.s3_region == "us-east-1"
        assert config.s3_use_ssl is False

    def test_partial_s3_config(self):
        """Test that partial S3 config (only some fields) works."""
        config = DuckDBOfflineStoreConfig(
            s3_url_style="path",
            s3_endpoint="s3.custom-provider.com",
        )

        assert config.s3_url_style == "path"
        assert config.s3_endpoint == "s3.custom-provider.com"
        assert config.s3_access_key_id is None
        assert config.s3_secret_access_key is None

    def test_s3_use_ssl_true(self):
        """Test that s3_use_ssl can be set to True."""
        config = DuckDBOfflineStoreConfig(s3_use_ssl=True)
        assert config.s3_use_ssl is True

    def test_s3_use_ssl_false(self):
        """Test that s3_use_ssl can be set to False."""
        config = DuckDBOfflineStoreConfig(s3_use_ssl=False)
        assert config.s3_use_ssl is False

    def test_config_with_staging_location(self):
        """Test config with both S3 settings and staging location."""
        config = DuckDBOfflineStoreConfig(
            staging_location="s3://my-bucket/staging",
            staging_location_endpoint_override="http://localhost:9000",
            s3_url_style="path",
            s3_endpoint="localhost:9000",
        )

        assert config.staging_location == "s3://my-bucket/staging"
        assert config.staging_location_endpoint_override == "http://localhost:9000"
        assert config.s3_url_style == "path"
        assert config.s3_endpoint == "localhost:9000"


class TestIsS3Path:
    """Tests for _is_s3_path helper function."""

    def test_s3_path(self):
        """Test that s3:// paths are detected."""
        assert _is_s3_path("s3://bucket/key") is True

    def test_s3a_path(self):
        """Test that s3a:// paths are detected."""
        assert _is_s3_path("s3a://bucket/key") is True

    def test_local_path(self):
        """Test that local paths are not detected as S3."""
        assert _is_s3_path("/path/to/file.parquet") is False

    def test_http_path(self):
        """Test that HTTP paths are not detected as S3."""
        assert _is_s3_path("http://example.com/file.parquet") is False

    def test_gs_path(self):
        """Test that GCS paths are not detected as S3."""
        assert _is_s3_path("gs://bucket/key") is False


class TestConfigureDuckDBForS3:
    """Tests for _configure_duckdb_for_s3 function."""

    def setup_method(self):
        """Reset the global _s3_configured flag before each test."""
        import feast.infra.offline_stores.duckdb as duckdb_module

        duckdb_module._s3_configured = False

    def test_no_config_does_nothing(self):
        """Test that empty config doesn't call ibis."""
        config = DuckDBOfflineStoreConfig()

        with patch("feast.infra.offline_stores.duckdb.ibis") as mock_ibis:
            _configure_duckdb_for_s3(config)
            mock_ibis.get_backend.assert_not_called()

    def test_s3_url_style_configures_duckdb(self):
        """Test that s3_url_style triggers DuckDB configuration."""
        config = DuckDBOfflineStoreConfig(s3_url_style="path")

        mock_con = MagicMock()
        with patch(
            "feast.infra.offline_stores.duckdb.ibis.get_backend", return_value=mock_con
        ):
            _configure_duckdb_for_s3(config)

            # Check that httpfs was installed and loaded
            mock_con.raw_sql.assert_any_call("INSTALL httpfs;")
            mock_con.raw_sql.assert_any_call("LOAD httpfs;")

            # Check that s3_url_style was set
            mock_con.raw_sql.assert_any_call("SET s3_url_style='path';")

    def test_full_s3_config_sets_all_options(self):
        """Test that all S3 options are configured correctly."""
        config = DuckDBOfflineStoreConfig(
            s3_url_style="path",
            s3_endpoint="localhost:9000",
            s3_access_key_id="mykey",
            s3_secret_access_key="mysecret",
            s3_region="us-west-2",
            s3_use_ssl=False,
        )

        mock_con = MagicMock()
        with patch(
            "feast.infra.offline_stores.duckdb.ibis.get_backend", return_value=mock_con
        ):
            _configure_duckdb_for_s3(config)

            # Verify all SQL commands were executed
            calls = [str(call) for call in mock_con.raw_sql.call_args_list]

            assert any("s3_url_style='path'" in call for call in calls)
            assert any("s3_endpoint='localhost:9000'" in call for call in calls)
            assert any("s3_access_key_id='mykey'" in call for call in calls)
            assert any("s3_secret_access_key='mysecret'" in call for call in calls)
            assert any("s3_region='us-west-2'" in call for call in calls)
            assert any("s3_use_ssl=false" in call for call in calls)

    def test_s3_use_ssl_true_sets_correctly(self):
        """Test that s3_use_ssl=True is set correctly."""
        config = DuckDBOfflineStoreConfig(s3_use_ssl=True)

        mock_con = MagicMock()
        with patch(
            "feast.infra.offline_stores.duckdb.ibis.get_backend", return_value=mock_con
        ):
            _configure_duckdb_for_s3(config)

            mock_con.raw_sql.assert_any_call("SET s3_use_ssl=true;")

    def test_config_only_runs_once(self):
        """Test that S3 configuration only runs once (cached)."""
        config = DuckDBOfflineStoreConfig(s3_url_style="path")

        mock_con = MagicMock()
        with patch(
            "feast.infra.offline_stores.duckdb.ibis.get_backend", return_value=mock_con
        ):
            # First call should configure
            _configure_duckdb_for_s3(config)
            first_call_count = mock_con.raw_sql.call_count

            # Second call should not configure again
            _configure_duckdb_for_s3(config)
            assert mock_con.raw_sql.call_count == first_call_count

    def test_handles_ibis_error_gracefully(self):
        """Test that ibis errors are handled gracefully."""
        config = DuckDBOfflineStoreConfig(s3_url_style="path")

        with patch(
            "feast.infra.offline_stores.duckdb.ibis.get_backend",
            side_effect=Exception("ibis error"),
        ):
            # Should not raise an exception
            _configure_duckdb_for_s3(config)

    def test_only_endpoint_configured(self):
        """Test configuration with only endpoint set."""
        config = DuckDBOfflineStoreConfig(s3_endpoint="minio.local:9000")

        mock_con = MagicMock()
        with patch(
            "feast.infra.offline_stores.duckdb.ibis.get_backend", return_value=mock_con
        ):
            _configure_duckdb_for_s3(config)

            # Should set endpoint but not url_style
            calls = [str(call) for call in mock_con.raw_sql.call_args_list]
            assert any("s3_endpoint='minio.local:9000'" in call for call in calls)
            assert not any("s3_url_style" in call for call in calls)


class TestConfigFromYaml:
    """Tests for loading DuckDB config from YAML-style dictionaries."""

    def test_from_dict_with_s3_settings(self):
        """Test creating config from dictionary (simulating YAML parsing)."""
        config_dict = {
            "type": "duckdb",
            "s3_url_style": "path",
            "s3_endpoint": "localhost:9000",
            "s3_access_key_id": "minioadmin",
            "s3_secret_access_key": "minioadmin",
            "s3_region": "us-east-1",
            "s3_use_ssl": False,
        }

        config = DuckDBOfflineStoreConfig(**config_dict)

        assert config.type == "duckdb"
        assert config.s3_url_style == "path"
        assert config.s3_endpoint == "localhost:9000"
        assert config.s3_access_key_id == "minioadmin"
        assert config.s3_secret_access_key == "minioadmin"
        assert config.s3_region == "us-east-1"
        assert config.s3_use_ssl is False

    def test_from_dict_minimal(self):
        """Test creating minimal config from dictionary."""
        config_dict = {"type": "duckdb"}

        config = DuckDBOfflineStoreConfig(**config_dict)

        assert config.type == "duckdb"
        assert config.s3_url_style is None
