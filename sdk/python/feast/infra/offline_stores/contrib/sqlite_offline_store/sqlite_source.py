import sqlite3
import urllib.parse
from typing import Callable, Dict, Iterable, Optional, Tuple

from typeguard import typechecked

from feast import type_map
from feast.data_source import DataSource
from feast.infra.offline_stores.file_source import FileOptions
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.value_type import ValueType


@typechecked
class SQLiteSource(DataSource):
    """A SQLiteSource object defines a data source that a SQLite offline store can use."""

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.BATCH_FILE

    def __init__(
        self,
        *,
        database: str,
        table: str,
        name: Optional[str] = "",
        query: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """
        Creates a SQLiteSource object.

        Args:
            database: Path to the SQLite database file.
            table: Table name in the SQLite database.
            name (optional): Name for the SQLite source. Defaults to the table name.
            query (optional): SQL query to use instead of table name.
            timestamp_field (optional): Event timestamp field used for point in time
                joins of feature values.
            created_timestamp_column (optional): Timestamp column when row was created, used for deduplicating rows.
            field_mapping: A dictionary mapping of column names in this data source to feature names in a feature table
                or view. Only used for feature columns, not entities or timestamp columns.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the SQLite source, typically the email of the primary
                maintainer.

        Examples:
            >>> from feast.infra.offline_stores.contrib.sqlite_offline_store import SQLiteSource
            >>> sqlite_source = SQLiteSource(
            ...     database="features.db",
            ...     table="user_features",
            ...     timestamp_field="event_timestamp"
            ... )
        """
        # Store SQLite-specific info in the path, encode table and query info
        sqlite_uri = f"sqlite://{database}?table={table}"
        if query:
            # URL encode the query
            sqlite_uri += f"&query={urllib.parse.quote(query)}"

        self.file_options = FileOptions(
            uri=sqlite_uri,
            file_format=None,  # SQLite doesn't use file formats
            s3_endpoint_override=None,
        )

        self._database = database
        self._table = table
        self._query = query

        super().__init__(
            name=name if name else table,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, SQLiteSource):
            raise TypeError(
                "Comparisons should only involve SQLiteSource class objects."
            )

        return (
            super().__eq__(other)
            and self._database == other._database
            and self._table == other._table
            and self._query == other._query
        )

    @property
    def database(self):
        """Returns the database path of this SQLite source."""
        return self._database

    @property
    def table(self):
        """Returns the table name of this SQLite source."""
        return self._table

    @property
    def query(self):
        """Returns the query of this SQLite source."""
        return self._query

    @property
    def path(self):
        """Returns the URI path of this SQLite source."""
        return self.file_options.uri

    def get_table_query_string(self) -> str:
        """Returns a string that can be used in a SQL query to reference this source."""
        if self.query:
            return f"({self.query})"
        else:
            return self.table

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        assert data_source.file_options

        # Parse the SQLite URI to extract database, table, and query
        import urllib.parse

        uri = data_source.file_options.uri
        if not uri.startswith("sqlite://"):
            raise ValueError(f"Invalid SQLite URI: {uri}")

        # Remove sqlite:// prefix and parse
        path_and_params = uri[9:]  # Remove "sqlite://"
        if "?" in path_and_params:
            database, params_str = path_and_params.split("?", 1)
            params = urllib.parse.parse_qs(params_str)
            table = params.get("table", [""])[0]
            query = params.get("query", [""])[0]
            if query:
                query = urllib.parse.unquote(query)
        else:
            database = path_and_params
            table = ""
            query = None

        return SQLiteSource(
            name=data_source.name,
            database=database,
            table=table,
            query=query or None,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_FILE,
            data_source_class_type="feast.infra.offline_stores.contrib.sqlite_offline_store.sqlite_source.SQLiteSource",
            field_mapping=self.field_mapping,
            file_options=self.file_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            timestamp_field=self.timestamp_field,
            created_timestamp_column=self.created_timestamp_column,
        )

        return data_source_proto

    def validate(self, config: RepoConfig):
        # TODO: Check if the database file exists and is accessible
        pass

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """Returns the list of column names and raw column types for a SQLite table."""
        with sqlite3.connect(self.database) as conn:
            if self.query:
                # For queries, we need to execute and inspect the result
                cursor = conn.execute(f"PRAGMA table_info(({self.query}))")
            else:
                cursor = conn.execute(f"PRAGMA table_info({self.table})")

            columns = cursor.fetchall()
            return [(col[1], col[2]) for col in columns]

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.sqlite_to_feast_value_type


@typechecked
class SavedDatasetSQLiteStorage(SavedDatasetStorage):
    """SQLite storage for saved datasets."""

    def __init__(self, database: str, table: str):
        self.database = database
        self.table = table

    @staticmethod
    def from_proto(
        storage_proto: SavedDatasetStorageProto,
    ) -> "SavedDatasetSQLiteStorage":
        # Parse from file_options since we're using BATCH_FILE type
        uri = storage_proto.file_storage.uri
        if not uri.startswith("sqlite://"):
            raise ValueError(f"Invalid SQLite URI: {uri}")

        import urllib.parse

        path_and_params = uri[9:]  # Remove "sqlite://"
        if "?" in path_and_params:
            database, params_str = path_and_params.split("?", 1)
            params = urllib.parse.parse_qs(params_str)
            table = params.get("table", [""])[0]
        else:
            database = path_and_params
            table = ""

        return SavedDatasetSQLiteStorage(database=database, table=table)

    def to_proto(self) -> SavedDatasetStorageProto:
        sqlite_uri = f"sqlite://{self.database}?table={self.table}"
        file_options = DataSourceProto.FileOptions(uri=sqlite_uri)
        return SavedDatasetStorageProto(file_storage=file_options)

    def to_data_source(self) -> DataSource:
        return SQLiteSource(
            database=self.database,
            table=self.table,
        )
