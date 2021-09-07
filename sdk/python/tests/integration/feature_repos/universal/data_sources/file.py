import tempfile
from typing import Any, Dict, List, Optional

import pandas as pd
from minio import Minio
from testcontainers.core.generic import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from feast import FileSource
from feast.data_format import ParquetFormat
from feast.data_source import DataSource
from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.repo_config import FeastConfigBaseModel
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class FileDataSourceCreator(DataSourceCreator):
    files: List[Any]

    def __init__(self, project_name: str):
        self.project_name = project_name
        self.files = []

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
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
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}.{suffix}"

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return FileOfflineStoreConfig()

    def teardown(self):
        for f in self.files:
            f.close()


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
            self.bucket, file_name, self.f.name,
        )

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: Optional[str] = None,
        suffix: Optional[str] = None,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
    ) -> DataSource:
        filename = f"{destination_name}.parquet"
        port = self.minio.get_exposed_port("9000")
        host = self.minio.get_container_host_ip()
        minio_endpoint = f"{host}:{port}"

        self._upload_parquet_file(df, filename, minio_endpoint)

        return FileSource(
            file_format=ParquetFormat(),
            path=f"s3://{self.bucket}/{filename}",
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            field_mapping=field_mapping or {"ts_1": "ts"},
            s3_endpoint_override=f"http://{host}:{port}",
        )

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{suffix}"

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return FileOfflineStoreConfig()

    def teardown(self):
        self.minio.stop()
        self.f.close()
