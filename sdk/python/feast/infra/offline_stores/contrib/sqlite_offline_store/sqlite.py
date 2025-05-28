import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, List, Literal, Optional

from pydantic import StrictStr

from feast.infra.offline_stores.offline_store import OfflineStore
from feast.repo_config import FeastConfigBaseModel


class SQLiteOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for SQLite"""

    type: Literal["sqlite"] = "sqlite"
    connection_string: StrictStr
    database: Optional[StrictStr] = None


class SQLiteOfflineStore(OfflineStore):
    """SQLite implementation of Feast offline store"""

    def __init__(self, config: SQLiteOfflineStoreConfig):
        self.connection_string = config.connection_string
        self.database = config.database

    @staticmethod
    def from_config(config: SQLiteOfflineStoreConfig) -> "SQLiteOfflineStore":
        """Create SQLiteOfflineStore from config"""
        return SQLiteOfflineStore(config)

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = sqlite3.connect(self.connection_string)
            yield conn
        except sqlite3.Error as e:
            raise ValueError(f"Error connecting to SQLite database: {e}")
        finally:
            if conn:
                conn.close()

    def _get_sqlite_type(self, value: Any) -> str:
        """Map Python types to SQLite types"""
        if isinstance(value, (int, float)):
            return "REAL"
        elif isinstance(value, bool):
            return "INTEGER"
        else:
            return "TEXT"

    def write(self, table_name: str, data: List[Dict[str, Any]]):
        """Write data to SQLite table"""
        if not data:
            raise ValueError("No data provided for writing")

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Create table if not exists with appropriate types
                columns = ", ".join(
                    [
                        f"{key} {self._get_sqlite_type(value)}"
                        for key, value in data[0].items()
                    ]
                )
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")

                # Insert data
                placeholders = ", ".join(["?" for _ in data[0]])
                insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                cursor.executemany(insert_query, [tuple(row.values()) for row in data])
                conn.commit()
            except sqlite3.Error as e:
                conn.rollback()
                raise ValueError(f"Error writing to SQLite database: {e}")

    def read(
        self,
        table_name: str,
        columns: List[str],
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Read data from SQLite table"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                query = f"SELECT {', '.join(columns)} FROM {table_name}"
                params = []
                if filters:
                    where_clause = " AND ".join(
                        [f"{key} = ?" for key in filters.keys()]
                    )
                    query += f" WHERE {where_clause}"
                    params = list(filters.values())

                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(zip(columns, row)) for row in results]
            except sqlite3.Error as e:
                raise ValueError(f"Error reading from SQLite database: {e}")

    def delete(self, table_name: str, filters: Optional[Dict[str, Any]] = None):
        """Delete data from SQLite table"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                query = f"DELETE FROM {table_name}"
                params = []
                if filters:
                    where_clause = " AND ".join(
                        [f"{key} = ?" for key in filters.keys()]
                    )
                    query += f" WHERE {where_clause}"
                    params = list(filters.values())

                cursor.execute(query, params)
                conn.commit()
            except sqlite3.Error as e:
                conn.rollback()
                raise ValueError(f"Error deleting from SQLite database: {e}")
