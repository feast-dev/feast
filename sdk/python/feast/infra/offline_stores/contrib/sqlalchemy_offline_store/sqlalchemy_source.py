# Copyright 2024 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from typing import Callable, Dict, Iterable, Optional, Tuple

from typeguard import typechecked

from feast.data_source import DataSource
from feast.errors import (
    DataSourceNoNameException,
    DataSourceNotFoundException,
    ZeroColumnQueryResult,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import sqlalchemy_type_to_feast_value_type
from feast.value_type import ValueType


@typechecked
class SQLAlchemySource(DataSource):
    """
    A SQLAlchemySource is a data source that uses SQLAlchemy to connect to various databases.

    This provides a unified interface to connect to any database supported by SQLAlchemy,
    including PostgreSQL, MySQL, SQLite, Oracle, MS SQL Server, and more.

    Example usage:

        from feast import Entity, FeatureView, Field
        from feast.types import Float32, Int64
        from feast.infra.offline_stores.contrib.sqlalchemy_offline_store import SQLAlchemySource

        driver_stats_source = SQLAlchemySource(
            name="driver_stats_source",
            table="driver_stats",
            timestamp_field="event_timestamp",
        )

        # Or using a custom query:
        driver_stats_source = SQLAlchemySource(
            name="driver_stats_source",
            query="SELECT * FROM driver_stats WHERE active = 1",
            timestamp_field="event_timestamp",
        )
    """

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    def __init__(
        self,
        name: Optional[str] = None,
        query: Optional[str] = None,
        table: Optional[str] = None,
        schema: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """
        Creates a SQLAlchemySource object.

        Args:
            name: Name of SQLAlchemySource, which should be unique within a project.
            query: SQL query that will be used to fetch the data. Mutually exclusive with `table`.
            table: Table name. Mutually exclusive with `query`.
            schema: Database schema name (e.g., "public" for PostgreSQL). Optional.
            timestamp_field (optional): Event timestamp field used for point-in-time joins
                of feature values.
            created_timestamp_column (optional): Timestamp column indicating when the row
                was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to feature names in a feature table or view. Only used for feature
                columns, not entity or timestamp columns.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the data source, typically the email of the primary
                maintainer.
        """
        if query and table:
            raise ValueError("Cannot specify both 'query' and 'table' parameters.")

        self._sqlalchemy_options = SQLAlchemyOptions(
            name=name, query=query, table=table, schema=schema
        )

        # If no name, use the table as the default name.
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
        if not isinstance(other, SQLAlchemySource):
            raise TypeError(
                "Comparisons should only involve SQLAlchemySource class objects."
            )

        return (
            super().__eq__(other)
            and self._sqlalchemy_options._query == other._sqlalchemy_options._query
            and self._sqlalchemy_options._table == other._sqlalchemy_options._table
            and self._sqlalchemy_options._schema == other._sqlalchemy_options._schema
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        assert data_source.HasField("custom_options")

        sqlalchemy_options = json.loads(data_source.custom_options.configuration)

        return SQLAlchemySource(
            name=sqlalchemy_options.get("name"),
            query=sqlalchemy_options.get("query"),
            table=sqlalchemy_options.get("table"),
            schema=sqlalchemy_options.get("schema"),
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.sqlalchemy_offline_store.sqlalchemy_source.SQLAlchemySource",
            field_mapping=self.field_mapping,
            custom_options=self._sqlalchemy_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        """
        Validates the SQLAlchemy data source by attempting to connect to the database
        and verify that the table or query is accessible.

        Args:
            config: RepoConfig containing the offline store configuration

        Raises:
            DataSourceNotFoundException: If the table does not exist
            Exception: If the database connection fails or query is invalid
        """
        from sqlalchemy import create_engine, inspect, text
        from sqlalchemy.exc import NoSuchTableError, ProgrammingError

        # Get the offline store config
        offline_store_config = config.offline_store

        # Validate that we have a SQLAlchemy offline store config
        if not hasattr(offline_store_config, "connection_string"):
            raise ValueError(
                "SQLAlchemySource requires a SQLAlchemyOfflineStoreConfig with a connection_string"
            )

        try:
            engine = create_engine(
                offline_store_config.connection_string,
                **getattr(offline_store_config, "sqlalchemy_config_kwargs", {}),
            )

            with engine.connect() as conn:
                if self._sqlalchemy_options._table:
                    # For table-based sources, check if the table exists
                    inspector = inspect(engine)
                    schema = self._sqlalchemy_options._schema or None

                    # Get list of tables in the schema
                    tables = inspector.get_table_names(schema=schema)
                    views = inspector.get_view_names(schema=schema)
                    all_tables = tables + views

                    # Case-insensitive comparison for databases like Oracle that uppercase identifiers
                    table_name = self._sqlalchemy_options._table
                    table_exists = (
                        table_name in all_tables
                        or table_name.upper() in [t.upper() for t in all_tables]
                        or table_name.lower() in [t.lower() for t in all_tables]
                    )

                    if not table_exists:
                        table_ref = (
                            f"{schema}.{self._sqlalchemy_options._table}"
                            if schema
                            else self._sqlalchemy_options._table
                        )
                        raise DataSourceNotFoundException(table_ref)

                    # Verify we can query the table - use dialect-specific syntax
                    table_ref = self.get_table_query_string()
                    dialect = engine.dialect.name
                    if dialect == "oracle":
                        test_query = f"SELECT * FROM {table_ref} WHERE ROWNUM = 0"
                    else:
                        test_query = f"SELECT * FROM {table_ref} LIMIT 0"
                    conn.execute(text(test_query))

                else:
                    # For query-based sources, try to execute the query
                    dialect = engine.dialect.name
                    if dialect == "oracle":
                        test_query = f"SELECT * FROM ({self._sqlalchemy_options._query}) validation_subquery WHERE ROWNUM = 0"
                    else:
                        test_query = f"SELECT * FROM ({self._sqlalchemy_options._query}) AS validation_subquery LIMIT 0"
                    conn.execute(text(test_query))

        except (NoSuchTableError, ProgrammingError) as e:
            table_or_query = (
                self._sqlalchemy_options._table or self._sqlalchemy_options._query
            )
            raise DataSourceNotFoundException(table_or_query) from e

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return sqlalchemy_type_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from sqlalchemy import create_engine, text

        engine = create_engine(
            config.offline_store.connection_string,
            **config.offline_store.sqlalchemy_config_kwargs,
        )

        dialect = engine.dialect.name
        from_expression = self.get_table_query_string()
        # Oracle doesn't use AS for table aliases; also LIMIT is not supported
        alias_keyword = "" if dialect == "oracle" else "AS "
        if dialect == "oracle":
            query = (
                f"SELECT * FROM {from_expression} {alias_keyword}sub WHERE ROWNUM = 0"
            )
        else:
            query = f"SELECT * FROM {from_expression} {alias_keyword}sub LIMIT 0"

        with engine.connect() as conn:
            result = conn.execute(text(query))
            if result.cursor is None or result.cursor.description is None:
                raise ZeroColumnQueryResult(query)

            # Return column name and type name
            return [
                (col[0], str(col[1].__class__.__name__).lower())
                for col in result.cursor.description
            ]

    def get_table_query_string(self) -> str:
        """Returns a string that can be used to reference this table in SQL.

        For query-based sources, returns the query wrapped in parentheses.

        Note:
            When using the returned string directly in a FROM clause,
            you may need to add an alias if this is a query-based source.
        """
        if self._sqlalchemy_options._table:
            if self._sqlalchemy_options._schema:
                return f"{self._sqlalchemy_options._schema}.{self._sqlalchemy_options._table}"
            return f"{self._sqlalchemy_options._table}"
        else:
            return f"({self._sqlalchemy_options._query})"

    def get_table_query_string_with_alias(
        self, alias: str = "subquery", dialect: str = "generic"
    ) -> str:
        """Returns a string for use in FROM clause with alias.

        Most SQL databases require all subqueries in FROM clauses to have aliases.
        This method automatically adds an alias when the source is query-based.

        Args:
            alias: The alias to use for query-based sources. Defaults to "subquery".
            dialect: The SQL dialect (e.g., "oracle", "postgresql"). Oracle doesn't
                use AS for table aliases. Defaults to "generic" (uses AS).

        Returns:
            For table-based sources: the table name (no alias needed).
            For query-based sources: "(query) [AS] alias".

        Example::

            source = SQLAlchemySource(query="SELECT * FROM my_table", ...)
            entity_sql = f"SELECT id, ts FROM {source.get_table_query_string_with_alias()}"
            # Results in: "SELECT id, ts FROM (SELECT * FROM my_table) AS subquery"
        """
        if self._sqlalchemy_options._table:
            if self._sqlalchemy_options._schema:
                return f"{self._sqlalchemy_options._schema}.{self._sqlalchemy_options._table}"
            return f"{self._sqlalchemy_options._table}"
        else:
            alias_keyword = "" if dialect == "oracle" else "AS "
            return f"({self._sqlalchemy_options._query}) {alias_keyword}{alias}"

    @property
    def table(self) -> Optional[str]:
        """Returns the table name if this is a table-based source."""
        return self._sqlalchemy_options._table or None

    @property
    def query(self) -> Optional[str]:
        """Returns the query if this is a query-based source."""
        return self._sqlalchemy_options._query or None

    @property
    def schema(self) -> Optional[str]:
        """Returns the schema name if specified."""
        return self._sqlalchemy_options._schema or None


class SQLAlchemyOptions:
    """Configuration options for SQLAlchemy data source."""

    def __init__(
        self,
        name: Optional[str],
        query: Optional[str],
        table: Optional[str],
        schema: Optional[str] = None,
    ):
        self._name = name or ""
        self._query = query or ""
        self._table = table or ""
        self._schema = schema or ""

    @classmethod
    def from_proto(cls, sqlalchemy_options_proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(sqlalchemy_options_proto.configuration.decode("utf8"))
        sqlalchemy_options = cls(
            name=config.get("name"),
            query=config.get("query"),
            table=config.get("table"),
            schema=config.get("schema"),
        )
        return sqlalchemy_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        sqlalchemy_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {
                    "name": self._name,
                    "query": self._query,
                    "table": self._table,
                    "schema": self._schema,
                }
            ).encode()
        )
        return sqlalchemy_options_proto


class SavedDatasetSQLAlchemyStorage(SavedDatasetStorage):
    """Storage for saved datasets using SQLAlchemy-compatible databases."""

    _proto_attr_name = "custom_storage"

    sqlalchemy_options: SQLAlchemyOptions

    def __init__(self, table_ref: str, schema: Optional[str] = None):
        self.sqlalchemy_options = SQLAlchemyOptions(
            table=table_ref, name=None, query=None, schema=schema
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        options = SQLAlchemyOptions.from_proto(storage_proto.custom_storage)
        return SavedDatasetSQLAlchemyStorage(
            table_ref=options._table, schema=options._schema
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(
            custom_storage=self.sqlalchemy_options.to_proto()
        )

    def to_data_source(self) -> DataSource:
        return SQLAlchemySource(
            table=self.sqlalchemy_options._table, schema=self.sqlalchemy_options._schema
        )
