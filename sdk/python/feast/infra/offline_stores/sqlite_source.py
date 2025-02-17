import json
import sqlite3
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from typeguard import typechecked

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.value_type import ValueType


class SQLiteOptions:
    """SQLite storage options used to configure a SQLite data source."""

    def __init__(
        self,
        path: Optional[str],
        query: Optional[str] = None,
        table: Optional[str] = None,
    ):
        self._path = path or ""
        self._query = query or ""
        self._table = table or ""

    @classmethod
    def from_proto(cls, sqlite_options_proto: DataSourceProto.CustomSourceOptions):
        """Creates SQLiteOptions from proto."""
        config = json.loads(sqlite_options_proto.configuration.decode("utf8"))
        return cls(
            path=config["path"],
            query=config["query"],
            table=config["table"],
        )

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        """Converts SQLiteOptions to proto."""
        sqlite_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {
                    "path": self._path,
                    "query": self._query,
                    "table": self._table,
                }
            ).encode()
        )
        return sqlite_options_proto


@typechecked
class SQLiteSource(DataSource):
    """A SQLiteSource object defines a source of feature data from a SQLite database."""

    def __init__(
        self,
        path: Optional[str] = None,
        name: Optional[str] = None,
        query: Optional[str] = None,
        table: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """Creates a SQLiteSource object.

        Args:
            path: Path to the SQLite database file.
            name: Name of source, used to identify this source.
            query: SQL query to execute to fetch data.
            table: Table to query from.
            timestamp_field: Event timestamp field used for point-in-time joins.
            created_timestamp_column: Timestamp column indicating when the row was created.
            field_mapping: Maps feature table fields to columns.
            description: Source description.
            tags: User-defined metadata.
            owner: Owner of the data source.
        """
        self._sqlite_options = SQLiteOptions(
            path=path,
            query=query,
            table=table,
        )

        # If no name, use the table as the default name
        if name is None and table is None:
            raise DataSourceNoNameException()
        name = name or table
        assert name

        super().__init__(
            name=name,
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
            and self._sqlite_options._query == other._sqlite_options._query
            and self._sqlite_options._table == other._sqlite_options._table
            and self._sqlite_options._path == other._sqlite_options._path
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        """Creates a SQLiteSource from proto."""
        assert data_source.HasField("custom_options")

        sqlite_options = json.loads(data_source.custom_options.configuration)

        return SQLiteSource(
            name=sqlite_options["name"],
            path=sqlite_options["path"],
            query=sqlite_options["query"],
            table=sqlite_options["table"],
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def to_proto(self) -> DataSourceProto:
        """Converts SQLiteSource to proto."""
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.sqlite_offline_store.sqlite_source.SQLiteSource",
            field_mapping=self.field_mapping,
            custom_options=self._sqlite_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        """Validates the SQLite data source configuration."""
        if not self._sqlite_options._path:
            raise ValueError("SQLite source must have a path specified")

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        """Returns a callable that converts SQLite types to Feast types."""

        def convert_type(dtype: str) -> ValueType:
            if not dtype:
                return ValueType.UNKNOWN
            type_map = {
                "TEXT": ValueType.STRING,
                "VARCHAR": ValueType.STRING,
                "CHAR": ValueType.STRING,
                "INTEGER": ValueType.INT64,
                "INT": ValueType.INT64,
                "BIGINT": ValueType.INT64,
                "REAL": ValueType.DOUBLE,
                "FLOAT": ValueType.DOUBLE,
                "DOUBLE": ValueType.DOUBLE,
                "NUMERIC": ValueType.DOUBLE,
                "DECIMAL": ValueType.DOUBLE,
                "BOOLEAN": ValueType.BOOL,
                "TIMESTAMP": ValueType.UNIX_TIMESTAMP,
                "DATETIME": ValueType.UNIX_TIMESTAMP,
                "DATE": ValueType.UNIX_TIMESTAMP,
                "BLOB": ValueType.BYTES,
            }
            # Extract base type from type definition (e.g., "VARCHAR(255)" -> "VARCHAR")
            base_type = dtype.split("(")[0].upper()
            return type_map.get(base_type, ValueType.UNKNOWN)

        return convert_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """Retrieves a mapping of column names to types for this SQLite data source."""
        with sqlite3.connect(self._sqlite_options._path) as conn:
            cursor = conn.cursor()
            # Get column info using pragma
            query = f"SELECT * FROM {self.get_table_query_string()} LIMIT 0"
            cursor.execute(query)
            columns = cursor.description
            # Get detailed type info using pragma table_info
            table_name = self._sqlite_options._table
            if table_name:
                cursor.execute(f"PRAGMA table_info({table_name})")
                type_info = {row[1]: row[2] for row in cursor.fetchall()}
                return [(col[0], type_info.get(col[0], "TEXT")) for col in columns]
            else:
                # For queries, use basic type inference
                return [(col[0], self._infer_sqlite_type(col[1])) for col in columns]

    def _infer_sqlite_type(self, type_code: Optional[Any]) -> str:
        """Infer SQLite type from type code."""
        if not type_code or not isinstance(type_code, int):
            return "TEXT"
        # SQLite type codes from sqlite3.dbapi2
        type_map = {
            1: "INTEGER",  # SQLITE_INTEGER
            2: "REAL",  # SQLITE_FLOAT
            3: "TEXT",  # SQLITE_TEXT
            4: "BLOB",  # SQLITE_BLOB
            5: "NULL",  # SQLITE_NULL
        }
        return type_map.get(type_code, "TEXT")

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL."""
        if self._sqlite_options._table:
            return f"{self._sqlite_options._table}"
        else:
            return f"({self._sqlite_options._query})"


class SavedDatasetSQLiteStorage(SavedDatasetStorage):
    """SQLite storage for saved datasets."""

    _proto_attr_name = "custom_storage"

    sqlite_options: SQLiteOptions

    def __init__(self, table_ref: str, path: str):
        self.sqlite_options = SQLiteOptions(
            path=path,
            table=table_ref,
            query=None,
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        sqlite_options = SQLiteOptions.from_proto(storage_proto.custom_storage)
        return SavedDatasetSQLiteStorage(
            table_ref=sqlite_options._table,
            path=sqlite_options._path,
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(custom_storage=self.sqlite_options.to_proto())

    def to_data_source(self) -> DataSource:
        return SQLiteSource(
            table=self.sqlite_options._table,
            path=self.sqlite_options._path,
        )
