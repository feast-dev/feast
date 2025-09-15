from typing import Literal

from pydantic import StrictStr

from feast.repo_config import FeastConfigBaseModel


class SQLiteOfflineStoreConfig(FeastConfigBaseModel):
    """Configuration for SQLite offline store."""

    type: Literal["sqlite"] = "sqlite"
    """Type of offline store (should be "sqlite")."""

    database: StrictStr = "feature_store.db"
    """Path to the SQLite database file. Defaults to "feature_store.db"."""
