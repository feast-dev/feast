import sqlite3
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

from google.protobuf.message import Message

from feast.data_source import DataSource
from feast.infra.offline_stores.sqlite_config import SQLiteOfflineStoreConfig
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType


@dataclass
class SQLiteOptions:
    table: Optional[str] = None
    query: Optional[str] = None

    def to_proto(self) -> Message:
        options = DataSourceProto.CustomSourceOptions()
        config = {
            "table": self.table if self.table else "",
            "query": self.query if self.query else "",
        }
        options.configuration = bytes(str(config), "utf-8")
        return options

    @classmethod
    def from_proto(cls, config: Message) -> "SQLiteOptions":
        if not isinstance(config, DataSourceProto.CustomSourceOptions):
            raise ValueError(f"Expected CustomSourceOptions but got {type(config)}")
        config_dict = eval(config.configuration.decode("utf-8"))
        return cls(
            table=config_dict["table"] if config_dict["table"] else None,
            query=config_dict["query"] if config_dict["query"] else None,
        )


class SQLiteSource(DataSource):
    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        def _convert_type(dtype: str) -> ValueType:
            mapping = {
                "INTEGER": ValueType.INT64,
                "REAL": ValueType.FLOAT,
                "TEXT": ValueType.STRING,
                "BLOB": ValueType.BYTES,
                "BOOLEAN": ValueType.BOOL,
                "DATETIME": ValueType.UNIX_TIMESTAMP,
                "DATE": ValueType.UNIX_TIMESTAMP,
                "TIME": ValueType.STRING,
                "NUMERIC": ValueType.FLOAT,
                "VARCHAR": ValueType.STRING,
                "CHAR": ValueType.STRING,
            }
            return mapping.get(dtype.upper(), ValueType.UNKNOWN)

        return _convert_type

    def __init__(
        self,
        *,
        name: str = "",
        table: Optional[str] = None,
        query: Optional[str] = None,
        timestamp_field: str = "",
        created_timestamp_column: str = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
    ):
        self._table = table
        self._query = query
        self._conn = None
        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {},
            description=description,
            tags=tags or {},
        )

    @property
    def table(self) -> Optional[str]:
        return self._table

    @property
    def query(self) -> Optional[str]:
        return self._query

    def get_table_query_string(self) -> str:
        if self._table:
            return self._table
        else:
            return f"({self._query})"

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        assert data_source.HasField("custom_options")
        sqlite_options = SQLiteOptions.from_proto(data_source.custom_options)

        return SQLiteSource(
            name=data_source.name,
            field_mapping=dict(data_source.field_mapping),
            table=sqlite_options.table,
            query=sqlite_options.query,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
        )

    def _get_custom_options(self) -> Message:
        return SQLiteOptions(
            table=self._table,
            query=self._query,
        ).to_proto()

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.sqlite_source.SQLiteSource",
            field_mapping=self.field_mapping,
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=bytes(
                    str(
                        {
                            "table": self._table if self._table else "",
                            "query": self._query if self._query else "",
                        }
                    ),
                    "utf-8",
                )
            ),
            description=self.description,
            tags=self.tags,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        if not self._table and not self._query:
            raise ValueError("SQLite source must have either table or query specified")

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> List[Tuple[str, str]]:
        if not isinstance(config.offline_store, SQLiteOfflineStoreConfig):
            raise ValueError("SQLite source requires SQLiteOfflineStoreConfig")

        store_config = config.offline_store
        if store_config is None:
            raise ValueError("Offline store configuration is missing")

        path = str(store_config.path or ":memory:")
        timeout = float(store_config.connection_timeout)
        if (
            hasattr(store_config, "_conn")
            and getattr(store_config, "_conn") is not None
        ):
            conn = getattr(store_config, "_conn")
            use_existing_conn = True
        else:
            conn = sqlite3.connect(path, timeout=timeout)
            use_existing_conn = False

        try:
            cursor = conn.cursor()
            if self._table:
                cursor.execute(f"SELECT * FROM {self._table} LIMIT 0")
            else:
                cursor.execute(
                    f"WITH query AS ({self._query}) SELECT * FROM query LIMIT 0"
                )
            # Get column types using pragma table_info
            if self._table:
                cursor.execute(f"PRAGMA table_info({self._table})")
                columns = []
                for row in cursor.fetchall():
                    col_name = str(row[1])
                    col_type = str(row[2]).split("(")[0].upper()
                    if col_type == "BOOLEAN":
                        # SQLite stores booleans as integers, but we want to preserve the boolean type
                        cursor.execute(f"SELECT COUNT(*) FROM {self._table}")
                        if cursor.fetchone()[0] > 0:
                            cursor.execute(
                                f"SELECT typeof({col_name}) FROM {self._table} LIMIT 1"
                            )
                            actual_type = cursor.fetchone()[0].upper()
                            if actual_type == "INTEGER":
                                col_type = "BOOLEAN"
                    columns.append((col_name, col_type))
            else:
                # For queries, we need to get the column types from the query result
                cursor.execute(f"{self._query} LIMIT 0")
                columns = [(str(desc[0]), str(desc[1])) for desc in cursor.description]
            return columns
        finally:
            if not use_existing_conn:
                conn.close()