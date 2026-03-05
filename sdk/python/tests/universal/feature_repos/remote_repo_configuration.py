from tests.universal.feature_repos.universal.data_sources.file import (
    RemoteOfflineOidcAuthStoreDataSourceCreator,
    RemoteOfflineStoreDataSourceCreator,
    RemoteOfflineTlsStoreDataSourceCreator,
)

AVAILABLE_OFFLINE_STORES = [
    ("local", RemoteOfflineStoreDataSourceCreator),
    ("local", RemoteOfflineOidcAuthStoreDataSourceCreator),
    ("local", RemoteOfflineTlsStoreDataSourceCreator),
]
AVAILABLE_ONLINE_STORES = {"sqlite": ({"type": "sqlite"}, None)}
