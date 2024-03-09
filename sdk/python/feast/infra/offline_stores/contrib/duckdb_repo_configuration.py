from feast.infra.offline_stores.contrib.duckdb_offline_store.duckdb import (
    DuckDBOfflineStoreConfig,
)
from tests.integration.feature_repos.universal.data_sources.file import (  # noqa: E402
    FileDataSourceCreator,
)


class DuckDBDataSourceCreator(FileDataSourceCreator):
    def create_offline_store_config(self):
        self.duckdb_offline_store_config = DuckDBOfflineStoreConfig()
        return self.duckdb_offline_store_config


AVAILABLE_OFFLINE_STORES = [
    ("local", DuckDBDataSourceCreator),
]

AVAILABLE_ONLINE_STORES = {"sqlite": ({"type": "sqlite"}, None)}
