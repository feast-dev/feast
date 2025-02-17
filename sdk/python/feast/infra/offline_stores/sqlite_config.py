from typing import Literal, Optional

from feast.repo_config import FeastConfigBaseModel


class SQLiteOfflineStoreConfig(FeastConfigBaseModel):
    """SQLite offline store configuration.
    Attributes:
        type: Offline store type selector
        path: Path to SQLite database file, use ":memory:" for in-memory database
        connection_timeout: SQLite connection timeout in seconds
        create_if_missing: Create database file if it doesn't exist
    """

    type: Literal["sqlite"] = "sqlite"
    path: Optional[str] = ":memory:"
    connection_timeout: float = 5.0
    create_if_missing: bool = True

    _offline_store_class_type = "feast.infra.offline_stores.sqlite.SQLiteOfflineStore"
    _offline_store_class = "feast.infra.offline_stores.sqlite.SQLiteOfflineStore"
    _source_class = "feast.infra.offline_stores.sqlite_source.SQLiteSource"
    _supports_prefetch = True
