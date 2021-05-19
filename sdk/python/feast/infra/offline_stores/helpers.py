from feast.data_source import BigQuerySource, DataSource, FileSource
from feast.errors import FeastOfflineStoreUnsupportedDataSource
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.repo_config import (
    BigQueryOfflineStoreConfig,
    FileOfflineStoreConfig,
    OfflineStoreConfig,
)


def get_offline_store_from_config(
    offline_store_config: OfflineStoreConfig,
) -> OfflineStore:
    """Get the offline store from offline store config"""

    if isinstance(offline_store_config, FileOfflineStoreConfig):
        from feast.infra.offline_stores.file import FileOfflineStore

        return FileOfflineStore()
    elif isinstance(offline_store_config, BigQueryOfflineStoreConfig):
        from feast.infra.offline_stores.bigquery import BigQueryOfflineStore

        return BigQueryOfflineStore()

    raise ValueError(f"Unsupported offline store config '{offline_store_config}'")


def assert_offline_store_supports_data_source(
    offline_store_config: OfflineStoreConfig, data_source: DataSource
):
    if (
        isinstance(offline_store_config, FileOfflineStoreConfig)
        and isinstance(data_source, FileSource)
    ) or (
        isinstance(offline_store_config, BigQueryOfflineStoreConfig)
        and isinstance(data_source, BigQuerySource)
    ):
        return
    raise FeastOfflineStoreUnsupportedDataSource(
        offline_store_config.type, data_source.__class__.__name__
    )
