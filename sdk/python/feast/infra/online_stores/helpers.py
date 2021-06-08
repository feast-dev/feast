from feast.data_source import BigQuerySource, DataSource, FileSource
from feast.errors import FeastOnlineStoreUnsupportedDataSource
from feast.infra.online_stores.online_store import OnlineStore
from feast.repo_config import (
    DatastoreOnlineStoreConfig,
    OnlineStoreConfig,
    SqliteOnlineStoreConfig,
)


def get_online_store_from_config(
    online_store_config: OnlineStoreConfig,
) -> OnlineStore:
    """Get the offline store from offline store config"""

    if isinstance(online_store_config, SqliteOnlineStoreConfig):
        from feast.infra.online_stores.sqlite import SqliteOnlineStore

        return SqliteOnlineStore()
    elif isinstance(online_store_config, DatastoreOnlineStoreConfig):
        from feast.infra.online_stores.datastore import DatastoreOnlineStore

        return DatastoreOnlineStore()

    raise ValueError(f"Unsupported offline store config '{online_store_config}'")


def assert_online_store_supports_data_source(
    online_store_config: OnlineStoreConfig, data_source: DataSource
):
    if (
        isinstance(online_store_config, SqliteOnlineStoreConfig)
        and isinstance(data_source, FileSource)
    ) or (
        isinstance(online_store_config, DatastoreOnlineStoreConfig)
        and isinstance(data_source, BigQuerySource)
    ):
        return

    raise FeastOnlineStoreUnsupportedDataSource(
        online_store_config.type, data_source.__class__.__name__
    )
