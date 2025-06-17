import os
import tempfile
from typing import Any, Dict, Optional

from sdk.python.feast.infra.offline_stores.contrib.ray_offline_store.ray import (
    RayOfflineStoreConfig,
)

from feast.data_format import ParquetFormat
from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.infra.offline_stores.file_source import (
    FileLoggingDestination,
    FileSource,
    SavedDatasetFileStorage,
)
from feast.repo_config import FeastConfigBaseModel
from feast.saved_dataset import SavedDatasetStorage
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class RayDataSourceCreator(DataSourceCreator):
    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name, *args, **kwargs)
        self.offline_store_config = RayOfflineStoreConfig(
            type="ray",
            storage_path="/tmp/ray-storage",
            ray_address=None,
            use_ray_cluster=False,
        )
        self.files = []
        self.dirs = []

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return self.offline_store_config

    def create_data_source(
        self,
        df: Any,
        destination_name: str,
        created_timestamp_column: Optional[Any] = "created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        # For Ray, we'll use parquet files as the underlying storage
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
            path=f.name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}.{suffix}"

    def create_saved_dataset_destination(self) -> SavedDatasetStorage:
        d = tempfile.mkdtemp(prefix=self.project_name)
        self.dirs.append(d)
        return SavedDatasetFileStorage(
            path=d,
            file_format=ParquetFormat(),
        )

    def create_logged_features_destination(self) -> LoggingDestination:
        d = tempfile.mkdtemp(prefix=self.project_name)
        self.dirs.append(d)
        return FileLoggingDestination(path=d)

    def teardown(self) -> None:
        # Clean up any temporary files or resources
        import shutil

        for f in self.files:
            f.close()
            try:
                os.unlink(f.name)
            except OSError:
                pass

        for d in self.dirs:
            if os.path.exists(d):
                shutil.rmtree(d)

    def get_saved_dataset_data_source(self) -> Dict[str, str]:
        return {
            "type": "parquet",
            "path": "data/saved_dataset.parquet",
        }


# Define the full repo configurations for Ray offline store
FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=RayDataSourceCreator,
    ),
]
