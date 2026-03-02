from feast.infra.offline_stores.duckdb import DuckDBOfflineStoreConfig
from tests.integration.feature_repos.universal.data_sources.file import (
    DeltaFileSourceCreator,
    DeltaS3FileSourceCreator,
    FileDataSourceCreator,
)


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


AVAILABLE_OFFLINE_STORES = [
    ("local", DuckDBDataSourceCreator),
    ("local", DuckDBDeltaDataSourceCreator),
]

AVAILABLE_ONLINE_STORES = {"sqlite": ({"type": "sqlite"}, None)}
