"""
Unit tests for SparkRetrievalJob._resolve_staging_filesystem.

Verifies that the correct PyArrow filesystem and prefix-stripped paths
are returned for S3, S3A, GCS, file://, and plain local paths.
"""

from unittest.mock import MagicMock, patch

import pytest

from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkRetrievalJob,
)


@pytest.fixture()
def retrieval_job():
    """Minimal SparkRetrievalJob with a mock config that has no offline_store region."""
    job = object.__new__(SparkRetrievalJob)
    config = MagicMock()
    config.offline_store.region = None
    job._config = config
    return job


class TestResolveS3Filesystem:
    def test_s3_scheme_returns_s3_filesystem(self, retrieval_job):
        with patch("pyarrow.fs.S3FileSystem") as mock_s3:
            mock_s3.return_value = MagicMock(name="s3fs")
            fs, paths = retrieval_job._resolve_staging_filesystem(
                ["s3://my-bucket/path/a.parquet", "s3://my-bucket/path/b.parquet"]
            )
            mock_s3.assert_called_once()
            assert fs is mock_s3.return_value
            assert paths == ["my-bucket/path/a.parquet", "my-bucket/path/b.parquet"]

    def test_s3a_scheme_strips_prefix(self, retrieval_job):
        with patch("pyarrow.fs.S3FileSystem") as mock_s3:
            mock_s3.return_value = MagicMock(name="s3fs")
            fs, paths = retrieval_job._resolve_staging_filesystem(
                ["s3a://bucket/dir/file.parquet"]
            )
            assert paths == ["bucket/dir/file.parquet"]

    def test_s3_with_minio_endpoint(self, retrieval_job, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://minio.local:9000")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
        with patch("pyarrow.fs.S3FileSystem") as mock_s3:
            mock_s3.return_value = MagicMock(name="s3fs")
            retrieval_job._resolve_staging_filesystem(["s3://bucket/file.parquet"])
            call_kwargs = mock_s3.call_args[1]
            assert call_kwargs["endpoint_override"] == "minio.local:9000"
            assert call_kwargs["scheme"] == "http"

    def test_s3_with_https_endpoint(self, retrieval_job, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "https://s3.custom.corp")
        with patch("pyarrow.fs.S3FileSystem") as mock_s3:
            mock_s3.return_value = MagicMock(name="s3fs")
            retrieval_job._resolve_staging_filesystem(["s3://bucket/file.parquet"])
            call_kwargs = mock_s3.call_args[1]
            assert call_kwargs["endpoint_override"] == "s3.custom.corp"
            assert call_kwargs["scheme"] == "https"

    def test_s3_falls_back_to_aws_s3_endpoint_env(self, retrieval_job, monkeypatch):
        monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
        monkeypatch.setenv("AWS_S3_ENDPOINT", "http://legacy-minio:9000")
        with patch("pyarrow.fs.S3FileSystem") as mock_s3:
            mock_s3.return_value = MagicMock(name="s3fs")
            retrieval_job._resolve_staging_filesystem(["s3://bucket/file.parquet"])
            call_kwargs = mock_s3.call_args[1]
            assert "endpoint_override" in call_kwargs

    def test_s3_no_endpoint_no_override(self, retrieval_job, monkeypatch):
        monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
        monkeypatch.delenv("AWS_S3_ENDPOINT", raising=False)
        with patch("pyarrow.fs.S3FileSystem") as mock_s3:
            mock_s3.return_value = MagicMock(name="s3fs")
            retrieval_job._resolve_staging_filesystem(["s3://bucket/file.parquet"])
            call_kwargs = mock_s3.call_args[1]
            assert "endpoint_override" not in call_kwargs
            assert "scheme" not in call_kwargs

    def test_s3_region_from_offline_store_config(self, retrieval_job):
        retrieval_job._config.offline_store.region = "eu-west-1"
        with patch("pyarrow.fs.S3FileSystem") as mock_s3:
            mock_s3.return_value = MagicMock(name="s3fs")
            retrieval_job._resolve_staging_filesystem(["s3://bucket/file.parquet"])
            call_kwargs = mock_s3.call_args[1]
            assert call_kwargs["region"] == "eu-west-1"

    def test_s3_region_fallback_to_env(self, retrieval_job, monkeypatch):
        retrieval_job._config.offline_store.region = None
        monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-1")
        with patch("pyarrow.fs.S3FileSystem") as mock_s3:
            mock_s3.return_value = MagicMock(name="s3fs")
            retrieval_job._resolve_staging_filesystem(["s3://bucket/file.parquet"])
            call_kwargs = mock_s3.call_args[1]
            assert call_kwargs["region"] == "ap-southeast-1"


class TestResolveGCSFilesystem:
    def test_gs_scheme_returns_gcs_filesystem(self, retrieval_job):
        with patch("pyarrow.fs.GcsFileSystem") as mock_gcs:
            mock_gcs.return_value = MagicMock(name="gcsfs")
            fs, paths = retrieval_job._resolve_staging_filesystem(
                ["gs://my-bucket/path/a.parquet", "gs://my-bucket/path/b.parquet"]
            )
            mock_gcs.assert_called_once()
            assert fs is mock_gcs.return_value
            assert paths == ["my-bucket/path/a.parquet", "my-bucket/path/b.parquet"]


class TestResolveLocalFilesystem:
    def test_file_scheme_stripped(self, retrieval_job):
        fs, paths = retrieval_job._resolve_staging_filesystem(
            ["file:///tmp/staging/a.parquet"]
        )
        assert fs is None
        assert paths == ["/tmp/staging/a.parquet"]

    def test_plain_local_path_unchanged(self, retrieval_job):
        fs, paths = retrieval_job._resolve_staging_filesystem(
            ["/tmp/staging/a.parquet", "/tmp/staging/b.parquet"]
        )
        assert fs is None
        assert paths == ["/tmp/staging/a.parquet", "/tmp/staging/b.parquet"]

    def test_mixed_file_and_plain_paths(self, retrieval_job):
        fs, paths = retrieval_job._resolve_staging_filesystem(
            ["file:///tmp/a.parquet", "/tmp/b.parquet"]
        )
        assert fs is None
        assert paths == ["/tmp/a.parquet", "/tmp/b.parquet"]
