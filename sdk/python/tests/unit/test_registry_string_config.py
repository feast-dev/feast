"""Tests for string-based registry configuration in RepoConfig.

Verifies that passing registry as a string (e.g. "gs://bucket/registry.pb")
correctly allows auto-detection of the registry store class from the URI
scheme, rather than hardcoding FileRegistryStore.

Regression test for: RepoConfig.registry property hardcodes
get_registry_config_from_type("file") when registry_config is a string,
ignoring URI scheme and breaking gs:// and s3:// paths.
"""

from pathlib import Path

import pytest

from feast.infra.registry.registry import (
    REGISTRY_STORE_CLASS_FOR_SCHEME,
    get_registry_store_class_from_scheme,
)
from feast.repo_config import RegistryConfig, RepoConfig


def _make_repo_config(registry):
    """Helper to create a minimal RepoConfig with the given registry value."""
    return RepoConfig(
        project="test",
        provider="gcp",
        registry=registry,
        online_store={"type": "redis", "connection_string": "localhost:6379"},
        entity_key_serialization_version=3,
    )


class TestStringRegistryAutoDetection:
    """When registry is passed as a string, RepoConfig must produce a
    RegistryConfig that allows Registry.__init__ to auto-detect the correct
    store class from the URI scheme."""

    def test_gcs_string_registry_produces_correct_config(self):
        """gs:// string -> RegistryConfig with registry_store_type=None
        so Registry.__init__ auto-detects GCSRegistryStore."""
        config = _make_repo_config("gs://my-bucket/feast/registry.pb")
        reg = config.registry

        assert isinstance(reg, RegistryConfig)
        assert reg.path == "gs://my-bucket/feast/registry.pb"
        assert reg.registry_store_type is None

    def test_s3_string_registry_produces_correct_config(self):
        """s3:// string -> RegistryConfig with registry_store_type=None
        so Registry.__init__ auto-detects S3RegistryStore."""
        config = _make_repo_config("s3://my-bucket/feast/registry.pb")
        reg = config.registry

        assert isinstance(reg, RegistryConfig)
        assert reg.path == "s3://my-bucket/feast/registry.pb"
        assert reg.registry_store_type is None

    def test_local_string_registry_still_works(self):
        """A local file path string must still produce a valid RegistryConfig
        (no regression for the common case)."""
        config = _make_repo_config("/tmp/feast/registry.db")
        reg = config.registry

        assert isinstance(reg, RegistryConfig)
        assert reg.path == "/tmp/feast/registry.db"
        # registry_type defaults to "file", which is correct for local paths
        assert reg.registry_type == "file"

    def test_dict_registry_still_works(self):
        """Dict-based registry config must continue to work as before."""
        config = _make_repo_config({"path": "gs://my-bucket/feast/registry.pb"})
        reg = config.registry

        assert isinstance(reg, RegistryConfig)
        assert reg.path == "gs://my-bucket/feast/registry.pb"
        assert reg.registry_store_type is None

    def test_dict_registry_with_explicit_registry_type(self):
        """Dict with explicit registry_type must route through
        get_registry_config_from_type (no change in behavior)."""
        config = _make_repo_config(
            {"registry_type": "file", "path": "/tmp/registry.db"}
        )
        reg = config.registry

        assert isinstance(reg, RegistryConfig)
        assert reg.path == "/tmp/registry.db"
        assert reg.registry_type == "file"


class TestRegistryStoreSchemeDetection:
    """Verify that REGISTRY_STORE_CLASS_FOR_SCHEME and
    get_registry_store_class_from_scheme correctly map URI schemes
    to their store classes."""

    def test_gcs_scheme_selects_gcs_registry_store(self):
        assert "gs" in REGISTRY_STORE_CLASS_FOR_SCHEME
        cls = get_registry_store_class_from_scheme("gs://bucket/registry.pb")
        assert cls.__name__ == "GCSRegistryStore"

    def test_s3_scheme_selects_s3_registry_store(self):
        assert "s3" in REGISTRY_STORE_CLASS_FOR_SCHEME
        cls = get_registry_store_class_from_scheme("s3://bucket/registry.pb")
        assert cls.__name__ == "S3RegistryStore"

    def test_file_scheme_selects_file_registry_store(self):
        assert "file" in REGISTRY_STORE_CLASS_FOR_SCHEME
        cls = get_registry_store_class_from_scheme("file:///tmp/registry.db")
        assert cls.__name__ == "FileRegistryStore"

    def test_unknown_scheme_raises(self):
        with pytest.raises(Exception, match="unsupported scheme"):
            get_registry_store_class_from_scheme("ftp://host/registry.pb")


class TestFileRegistryStorePathHandling:
    """Demonstrate that FileRegistryStore cannot handle cloud URIs —
    this is the root cause of the IsADirectoryError in production when
    the bug is present."""

    def test_pathlib_does_not_treat_gcs_as_absolute(self):
        """pathlib.Path('gs://...') is NOT absolute, so FileRegistryStore
        joins it with repo_path producing nonsense like /app/gs://..."""
        gcs_path = Path("gs://my-bucket/feast/registry.pb")
        assert not gcs_path.is_absolute()

        joined = Path("/app").joinpath(gcs_path)
        assert str(joined).startswith("/app/gs:")

    def test_pathlib_does_not_treat_s3_as_absolute(self):
        """Same issue for s3:// paths."""
        s3_path = Path("s3://my-bucket/feast/registry.pb")
        assert not s3_path.is_absolute()

        joined = Path("/app").joinpath(s3_path)
        assert str(joined).startswith("/app/s3:")
