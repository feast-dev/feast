import os
import sqlite3
from typing import List

from feast import FeatureTable
from feast.infra.provider import Provider
from feast.repo_config import LocalOnlineStoreConfig


def _table_id(project: str, table: FeatureTable) -> str:
    return f"{project}_{table.name}"


class LocalSqlite(Provider):
    _db_path: str

    def __init__(self, config: LocalOnlineStoreConfig):
        self._db_path = config.path

    def update_infra(
        self,
        project: str,
        tables_to_delete: List[FeatureTable],
        tables_to_keep: List[FeatureTable],
    ):
        conn = sqlite3.connect(self._db_path)
        for table in tables_to_keep:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {_table_id(project, table)} (key BLOB, value BLOB)"
            )

        for table in tables_to_delete:
            conn.execute(f"DROP TABLE IF EXISTS {_table_id(project, table)}")

    def teardown_infra(self, project: str, tables: List[FeatureTable]) -> None:
        os.unlink(self._db_path)
