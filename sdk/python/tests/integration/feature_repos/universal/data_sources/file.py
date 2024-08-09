import logging
import os.path
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from minio import Minio
from testcontainers.core.generic import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.minio import MinioContainer

from feast import FileSource, RepoConfig
from feast.data_format import DeltaFormat, ParquetFormat
from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.offline_stores.duckdb import DuckDBOfflineStoreConfig
from feast.infra.offline_stores.file_source import (
    FileLoggingDestination,
    SavedDatasetFileStorage,
)
from feast.infra.offline_stores.remote import RemoteOfflineStoreConfig
from feast.repo_config import FeastConfigBaseModel, RegistryConfig
from feast.wait import wait_retry_backoff  # noqa: E402
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.utils.auth_permissions_util import include_auth_config
from tests.utils.http_server import check_port_open, free_port  # noqa: E402

logger = logging.getLogger(__name__)


class FileDataSourceCreator(DataSourceCreator):
    files: List[Any]
    dirs: List[Any]
    keep: List[Any]

    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        self.files = []
        self.dirs = []
        self.keep = []

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column="created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        destination_name = self.get_prefixed_table_name(destination_name)

        f = tempfile.NamedTemporaryFile(
            prefix=f"{self.project_name}_{destination_name}",
            suffix=".parquet",
            delete=False,
        )
        df.to_parquet(f.name)
        self.files.append(f)
        return FileSource(
            file_format=ParquetFormat(),
            path=f"{f.name}",
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_saved_dataset_destination(self) -> SavedDatasetFileStorage:
        d = tempfile.mkdtemp(prefix=self.project_name)
        self.dirs.append(d)
        return SavedDatasetFileStorage(
            path=d, file_format=ParquetFormat(), s3_endpoint_override=None
        )

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}.{suffix}"

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return DaskOfflineStoreConfig()

    def create_logged_features_destination(self) -> LoggingDestination:
        d = tempfile.mkdtemp(prefix=self.project_name)
        self.dirs.append(d)
        return FileLoggingDestination(path=d)

    def teardown(self):
        for f in self.files:
            f.close()

        for d in self.dirs:
            if not os.path.exists(d):
                continue
            shutil.rmtree(d)


class DeltaFileSourceCreator(FileDataSourceCreator):
    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column="created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        from deltalake.writer import write_deltalake

        destination_name = self.get_prefixed_table_name(destination_name)

        delta_path = tempfile.TemporaryDirectory(
            prefix=f"{self.project_name}_{destination_name}"
        )

        self.keep.append(delta_path)

        write_deltalake(delta_path.name, df)

        return FileSource(
            file_format=DeltaFormat(),
            path=delta_path.name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_saved_dataset_destination(self) -> SavedDatasetFileStorage:
        d = tempfile.mkdtemp(prefix=self.project_name)
        self.keep.append(d)
        return SavedDatasetFileStorage(
            path=d, file_format=DeltaFormat(), s3_endpoint_override=None
        )

    # LoggingDestination is parquet-only
    def create_logged_features_destination(self) -> LoggingDestination:
        d = tempfile.mkdtemp(prefix=self.project_name)
        self.keep.append(d)
        return FileLoggingDestination(path=d)


class DeltaS3FileSourceCreator(FileDataSourceCreator):
    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        self.minio = MinioContainer()
        self.minio.start()
        client = self.minio.get_client()
        if not client.bucket_exists("test"):
            client.make_bucket("test")
        host_ip = self.minio.get_container_host_ip()
        exposed_port = self.minio.get_exposed_port(self.minio.port)
        self.endpoint_url = f"http://{host_ip}:{exposed_port}"

        self.mock_environ = {
            "AWS_ACCESS_KEY_ID": self.minio.access_key,
            "AWS_SECRET_ACCESS_KEY": self.minio.secret_key,
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column="created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        from deltalake.writer import write_deltalake

        destination_name = self.get_prefixed_table_name(destination_name)

        storage_options = {
            "AWS_ACCESS_KEY_ID": self.minio.access_key,
            "AWS_SECRET_ACCESS_KEY": self.minio.secret_key,
            "AWS_ENDPOINT_URL": self.endpoint_url,
        }

        path = f"s3://test/{str(uuid.uuid4())}/{destination_name}"

        write_deltalake(path, df, storage_options=storage_options)

        return FileSource(
            file_format=DeltaFormat(),
            path=path,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
            s3_endpoint_override=self.endpoint_url,
        )

    def create_saved_dataset_destination(self) -> SavedDatasetFileStorage:
        return SavedDatasetFileStorage(
            path=f"s3://test/{str(uuid.uuid4())}",
            file_format=DeltaFormat(),
            s3_endpoint_override=self.endpoint_url,
        )

    # LoggingDestination is parquet-only
    def create_logged_features_destination(self) -> LoggingDestination:
        d = tempfile.mkdtemp(prefix=self.project_name)
        self.keep.append(d)
        return FileLoggingDestination(path=d)

    def teardown(self):
        self.minio.stop()


class FileParquetDatasetSourceCreator(FileDataSourceCreator):
    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column="created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        destination_name = self.get_prefixed_table_name(destination_name)

        dataset_path = tempfile.TemporaryDirectory(
            prefix=f"{self.project_name}_{destination_name}"
        )
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            table,
            base_dir=dataset_path.name,
            compression="snappy",
            format="parquet",
            existing_data_behavior="overwrite_or_ignore",
        )
        self.files.append(dataset_path.name)
        return FileSource(
            file_format=ParquetFormat(),
            path=dataset_path.name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )


class S3FileDataSourceCreator(DataSourceCreator):
    f: Any
    minio: DockerContainer
    bucket = "feast-test"
    access_key = "AKIAIOSFODNN7EXAMPLE"
    secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    minio_image = "minio/minio:RELEASE.2021-08-17T20-53-08Z"

    def __init__(self, _: str):
        self._setup_minio()

    def _setup_minio(self):
        self.minio = DockerContainer(self.minio_image)
        self.minio.with_exposed_ports(9000).with_exposed_ports(9001).with_env(
            "MINIO_ROOT_USER", self.access_key
        ).with_env("MINIO_ROOT_PASSWORD", self.secret).with_command(
            'server /data --console-address ":9001"'
        )
        self.minio.start()
        log_string_to_wait_for = (
            "API"  # The minio container will print "API: ..." when ready.
        )
        wait_for_logs(container=self.minio, predicate=log_string_to_wait_for, timeout=5)

    def _upload_parquet_file(self, df, file_name, minio_endpoint):
        self.f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        df.to_parquet(self.f.name)

        client = Minio(
            minio_endpoint,
            access_key=self.access_key,
            secret_key=self.secret,
            secure=False,
        )
        if not client.bucket_exists(self.bucket):
            client.make_bucket(self.bucket)
        client.fput_object(
            self.bucket,
            file_name,
            self.f.name,
        )

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column="created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        filename = f"{destination_name}.parquet"
        port = self.minio.get_exposed_port("9000")
        host = self.minio.get_container_host_ip()
        minio_endpoint = f"{host}:{port}"

        self._upload_parquet_file(df, filename, minio_endpoint)

        return FileSource(
            file_format=ParquetFormat(),
            path=f"s3://{self.bucket}/{filename}",
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
            s3_endpoint_override=f"http://{host}:{port}",
        )

    def create_saved_dataset_destination(self) -> SavedDatasetFileStorage:
        port = self.minio.get_exposed_port("9000")
        host = self.minio.get_container_host_ip()

        return SavedDatasetFileStorage(
            path=f"s3://{self.bucket}/persisted/{str(uuid.uuid4())}",
            file_format=ParquetFormat(),
            s3_endpoint_override=f"http://{host}:{port}",
        )

    def create_logged_features_destination(self) -> LoggingDestination:
        port = self.minio.get_exposed_port("9000")
        host = self.minio.get_container_host_ip()

        return FileLoggingDestination(
            path=f"s3://{self.bucket}/logged_features/{str(uuid.uuid4())}",
            s3_endpoint_override=f"http://{host}:{port}",
        )

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{suffix}"

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return DaskOfflineStoreConfig()

    def teardown(self):
        self.minio.stop()
        self.f.close()


# TODO split up DataSourceCreator and OfflineStoreCreator
class DuckDBDataSourceCreator(FileDataSourceCreator):
    def create_offline_store_config(self):
        self.duckdb_offline_store_config = DuckDBOfflineStoreConfig()
        return self.duckdb_offline_store_config


class DuckDBDeltaDataSourceCreator(DeltaFileSourceCreator):
    def create_offline_store_config(self):
        self.duckdb_offline_store_config = DuckDBOfflineStoreConfig()
        return self.duckdb_offline_store_config


class DuckDBDeltaS3DataSourceCreator(DeltaS3FileSourceCreator):
    def create_offline_store_config(self):
        self.duckdb_offline_store_config = DuckDBOfflineStoreConfig(
            staging_location="s3://test/staging",
            staging_location_endpoint_override=self.endpoint_url,
        )
        return self.duckdb_offline_store_config


class RemoteOfflineStoreDataSourceCreator(FileDataSourceCreator):
    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        self.server_port: int = 0
        self.proc = None

    def setup(self, registry: RegistryConfig):
        parent_offline_config = super().create_offline_store_config()
        config = RepoConfig(
            project=self.project_name,
            provider="local",
            offline_store=parent_offline_config,
            registry=registry.path,
            entity_key_serialization_version=2,
        )

        repo_path = Path(tempfile.mkdtemp())
        with open(repo_path / "feature_store.yaml", "w") as outfile:
            yaml.dump(config.model_dump(by_alias=True), outfile)
        repo_path = str(repo_path.resolve())

        self.server_port = free_port()
        host = "0.0.0.0"
        cmd = [
            "feast",
            "-c" + repo_path,
            "serve_offline",
            "--host",
            host,
            "--port",
            str(self.server_port),
        ]
        self.proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )

        _time_out_sec: int = 60
        # Wait for server to start
        wait_retry_backoff(
            lambda: (None, check_port_open(host, self.server_port)),
            timeout_secs=_time_out_sec,
            timeout_msg=f"Unable to start the feast remote offline server in {_time_out_sec} seconds at port={self.server_port}",
        )
        return "grpc+tcp://{}:{}".format(host, self.server_port)

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        self.remote_offline_store_config = RemoteOfflineStoreConfig(
            type="remote", host="0.0.0.0", port=self.server_port
        )
        return self.remote_offline_store_config

    def teardown(self):
        super().teardown()
        if self.proc is not None:
            self.proc.kill()

            # wait server to free the port
            wait_retry_backoff(
                lambda: (
                    None,
                    not check_port_open("localhost", self.server_port),
                ),
                timeout_secs=30,
            )


class RemoteOfflineOidcAuthStoreDataSourceCreator(FileDataSourceCreator):
    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        if "fixture_request" in kwargs:
            request = kwargs["fixture_request"]
            self.keycloak_url = request.getfixturevalue("start_keycloak_server")
        else:
            raise RuntimeError(
                "fixture_request object is not passed to inject keycloak fixture dynamically."
            )
        auth_config_template = """
auth:
  type: oidc
  client_id: feast-integration-client
  client_secret: feast-integration-client-secret
  username: reader_writer
  password: password
  realm: master
  auth_server_url: {keycloak_url}
  auth_discovery_url: {keycloak_url}/realms/master/.well-known/openid-configuration
"""
        self.auth_config = auth_config_template.format(keycloak_url=self.keycloak_url)
        self.server_port: int = 0
        self.proc = None

    def setup(self, registry: RegistryConfig):
        parent_offline_config = super().create_offline_store_config()
        config = RepoConfig(
            project=self.project_name,
            provider="local",
            offline_store=parent_offline_config,
            registry=registry.path,
            entity_key_serialization_version=2,
        )

        repo_path = Path(tempfile.mkdtemp())
        with open(repo_path / "feature_store.yaml", "w") as outfile:
            yaml.dump(config.model_dump(by_alias=True), outfile)
        repo_path = str(repo_path.resolve())

        include_auth_config(
            file_path=f"{repo_path}/feature_store.yaml", auth_config=self.auth_config
        )

        self.server_port = free_port()
        host = "0.0.0.0"
        cmd = [
            "feast",
            "-c" + repo_path,
            "serve_offline",
            "--host",
            host,
            "--port",
            str(self.server_port),
        ]
        self.proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )

        _time_out_sec: int = 60
        # Wait for server to start
        wait_retry_backoff(
            lambda: (None, check_port_open(host, self.server_port)),
            timeout_secs=_time_out_sec,
            timeout_msg=f"Unable to start the feast remote offline server in {_time_out_sec} seconds at port={self.server_port}",
        )
        return "grpc+tcp://{}:{}".format(host, self.server_port)

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        self.remote_offline_store_config = RemoteOfflineStoreConfig(
            type="remote", host="0.0.0.0", port=self.server_port
        )
        return self.remote_offline_store_config

    def get_keycloak_url(self):
        return self.keycloak_url

    def teardown(self):
        super().teardown()
        if self.proc is not None:
            self.proc.kill()

            # wait server to free the port
            wait_retry_backoff(
                lambda: (
                    None,
                    not check_port_open("localhost", self.server_port),
                ),
                timeout_secs=30,
            )
