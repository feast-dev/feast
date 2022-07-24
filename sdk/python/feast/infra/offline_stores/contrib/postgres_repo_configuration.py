from feast.infra.offline_stores.contrib.postgres_offline_store.tests.data_source import (
    PostgreSQLDataSourceCreator,
)

AVAILABLE_OFFLINE_STORES = [("local", PostgreSQLDataSourceCreator)]

AVAILABLE_ONLINE_STORES = {"postgres": (None, PostgreSQLDataSourceCreator)}
