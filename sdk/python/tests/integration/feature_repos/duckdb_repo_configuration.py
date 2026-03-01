from tests.integration.feature_repos.universal.data_sources.file import (
    DuckDBDataSourceCreator,
    DuckDBDeltaDataSourceCreator,
)

AVAILABLE_OFFLINE_STORES = [
    ("local", DuckDBDataSourceCreator),
    ("local", DuckDBDeltaDataSourceCreator),
]

AVAILABLE_ONLINE_STORES = {"sqlite": ({"type": "sqlite"}, None)}
