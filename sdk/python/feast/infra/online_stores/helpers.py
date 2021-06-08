from feast.data_source import BigQuerySource, DataSource, FileSource
from feast.errors import FeastOnlineStoreUnsupportedDataSource
from feast.infra.online_stores.datastore import DatastoreOnlineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.sqlite import SqliteOnlineStore
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
        from feast.infra.offline_stores.file import FileOfflineStore

        return SqliteOnlineStore()
    elif isinstance(online_store_config, DatastoreOnlineStoreConfig):
        from feast.infra.offline_stores.bigquery import BigQueryOfflineStore

        return DatastoreOnlineStore()

    raise ValueError(f"Unsupported offline store config '{online_store_config}'")


SUPPORTED_DATA_SOURCES_FOR_ONLINE_STORE = {
    SqliteOnlineStoreConfig: {FileSource},
    DatastoreOnlineStoreConfig: {BigQuerySource},
}


def assert_online_store_supports_data_source(
    online_store_config: OnlineStoreConfig, data_source: DataSource
):
    if type(data_source) in SUPPORTED_DATA_SOURCES_FOR_ONLINE_STORE.get(
        type(online_store_config), set()
    ):
        return

    raise FeastOnlineStoreUnsupportedDataSource(
        online_store_config.type, data_source.__class__.__name__
    )
