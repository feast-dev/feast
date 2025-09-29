import warnings
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, no_type_check

from typeguard import typechecked

from feast import type_map
from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException, DataSourceNotFoundException
from feast.feature_logging import LoggingDestination
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.FeatureService_pb2 import (
    LoggingConfig as LoggingConfigProto,
)
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.value_type import ValueType


@typechecked
class SnowflakeSource(DataSource):
    """A SnowflakeSource object defines a data source that a SnowflakeOfflineStore class can use."""

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        database: Optional[str] = None,
        warehouse: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        query: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """
        Creates a SnowflakeSource object.

        Args:
            name (optional): Name for the source. Defaults to the table if not specified, in which
                case the table must be specified.
            timestamp_field (optional): Event timestamp field used for point in time
                joins of feature values.
            database (optional): Snowflake database where the features are stored.
            schema (optional): Snowflake schema in which the table is located.
            table (optional): Snowflake table where the features are stored. Exactly one of 'table'
                and 'query' must be specified.
            query (optional): The query to be executed to obtain the features. Exactly one of 'table'
                and 'query' must be specified.
            created_timestamp_column (optional): Timestamp column indicating when the
                row was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to column names in a feature table or view.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the snowflake source, typically the email of the primary
                maintainer.
        """

        if warehouse:
            warnings.warn(
                "Specifying a warehouse within a SnowflakeSource is to be deprecated."
                "Starting v0.32.0, the warehouse as part of the Snowflake store config will be used.",
                RuntimeWarning,
            )

        if table is None and query is None:
            raise ValueError('No "table" or "query" argument provided.')
        if table and query:
            raise ValueError('Both "table" and "query" argument provided.')

        # The default Snowflake schema is named "PUBLIC".
        _schema = "PUBLIC" if (database and table and not schema) else schema

        self.snowflake_options = SnowflakeOptions(
            database=database,
            schema=_schema,
            table=table,
            query=query,
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

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        """
        Creates a SnowflakeSource from a protobuf representation of a SnowflakeSource.

        Args:
            data_source: A protobuf representation of a SnowflakeSource

        Returns:
            A SnowflakeSource object based on the data_source protobuf.
        """
        return SnowflakeSource(
            name=data_source.name,
            timestamp_field=data_source.timestamp_field,
            database=data_source.snowflake_options.database,
            schema=data_source.snowflake_options.schema,
            table=data_source.snowflake_options.table,
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            query=data_source.snowflake_options.query,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    # Note: Python requires redefining hash in child classes that override __eq__
    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, SnowflakeSource):
            raise TypeError(
                "Comparisons should only involve SnowflakeSource class objects."
            )

        return (
            super().__eq__(other)
            and self.database == other.database
            and self.schema == other.schema
            and self.table == other.table
            and self.query == other.query
        )

    @property
    def database(self):
        """Returns the database of this snowflake source."""
        return self.snowflake_options.database

    @property
    def schema(self):
        """Returns the schema of this snowflake source."""
        return self.snowflake_options.schema

    @property
    def table(self):
        """Returns the table of this snowflake source."""
        return self.snowflake_options.table

    @property
    def query(self):
        """Returns the snowflake options of this snowflake source."""
        return self.snowflake_options.query

    def _to_proto_impl(self) -> DataSourceProto:
        """
        Converts a SnowflakeSource object to its protobuf representation.

        Returns:
            A DataSourceProto object.
        """
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_SNOWFLAKE,
            field_mapping=self.field_mapping,
            snowflake_options=self.snowflake_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        # As long as the query gets successfully executed, or the table exists,
        # the data source is validated. We don't need the results though.
        self.get_table_column_names_and_types(config)

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL."""
        if self.database and self.schema and self.table:
            return f'"{self.database}"."{self.schema}"."{self.table}"'
        elif self.schema and self.table:
            return f'"{self.schema}"."{self.table}"'
        elif self.table:
            return f'"{self.table}"'
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.snowflake_type_to_feast_value_type

    @no_type_check
    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """
        Returns a mapping of column names to types for this snowflake source.

        Args:
            config: A RepoConfig describing the feature repo
        """
        from feast.infra.offline_stores.snowflake import SnowflakeOfflineStoreConfig
        from feast.infra.utils.snowflake.snowflake_utils import (
            GetSnowflakeConnection,
            execute_snowflake_statement,
        )

        assert isinstance(config.offline_store, SnowflakeOfflineStoreConfig)

        with GetSnowflakeConnection(config.offline_store) as conn:
            query = f"SELECT * FROM {self.get_table_query_string()} LIMIT 5"
            cursor = execute_snowflake_statement(conn, query)

            metadata: List[Dict[str, Any]] = [
                {
                    "column_name": column.name,
                    "type_code": column.type_code,
                    "precision": column.precision,
                    "scale": column.scale,
                    "is_nullable": column.is_nullable,
                    "snowflake_type": None,
                }
                for column in cursor.description
            ]

            if cursor.fetch_pandas_all().empty:
                raise DataSourceNotFoundException(
                    "The following source:\n" + query + "\n ... is empty"
                )

        high_precision_number_columns = [
            col["column_name"]
            for col in metadata
            if col["type_code"] == 0 and col["scale"] == 0 and col["precision"] > 19
        ]

        if high_precision_number_columns:
            max_selects = [
                f'MAX("{col}") AS "{col}"' for col in high_precision_number_columns
            ]
            query = (
                f"SELECT {', '.join(max_selects)} FROM {self.get_table_query_string()}"
            )

            with GetSnowflakeConnection(config.offline_store) as conn:
                result = execute_snowflake_statement(conn, query).fetch_pandas_all()

            for col in high_precision_number_columns:
                max_value = result[col].iloc[0]
                if max_value is not None:
                    str_length = len(str(int(max_value)))
                    for row in metadata:
                        if row["column_name"] == col:
                            if str_length <= 9:
                                row["snowflake_type"] = "NUMBER32"
                            elif str_length <= 19:
                                row["snowflake_type"] = "NUMBER64"
                            else:
                                raise NotImplementedError(
                                    f"Number in column {col} larger than INT64 is not supported"
                                )

        for row in metadata:
            if row["type_code"] == 0:
                if row["scale"] == 0:
                    if row["precision"] <= 9:  # max precision size to ensure INT32
                        row["snowflake_type"] = "NUMBER32"
                    elif row["precision"] <= 18:  # max precision size to ensure INT64
                        row["snowflake_type"] = "NUMBER64"
                    else:
                        continue
                else:
                    row["snowflake_type"] = "NUMBERwSCALE"

            elif row["type_code"] in [5, 9, 12]:
                datatype = snowflake_unsupported_map[row["type_code"]]
                raise NotImplementedError(
                    f"The datatype of column {row['column_name']} is of type {datatype} in datasource {query}. This type is not supported. Try converting to VARCHAR."
                )
            elif row["type_code"] in [1, 2, 3, 4, 6, 7, 8, 10, 11, 13]:
                row["snowflake_type"] = snowflake_type_code_map[row["type_code"]]
            else:
                raise NotImplementedError(
                    f"The datatype of column {row['column_name']} in datasource {query} is not supported."
                )

        return [
            (str(column["column_name"]), str(column["snowflake_type"]))
            for column in metadata
        ]

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        """
        Returns the source type of this data source.
        """
        return DataSourceProto.BATCH_SNOWFLAKE


snowflake_type_code_map = {
    0: "NUMBER",
    1: "DOUBLE",
    2: "VARCHAR",
    3: "DATE",
    4: "TIMESTAMP",
    6: "TIMESTAMP_LTZ",
    7: "TIMESTAMP_TZ",
    8: "TIMESTAMP_NTZ",
    10: "ARRAY",
    11: "BINARY",
    13: "BOOLEAN",
}

snowflake_unsupported_map = {
    5: "VARIANT",
    9: "OBJECT",
    12: "TIME",
}

python_int_to_snowflake_type_map = {
    "int64": "NUMBER64",
    "int32": "NUMBER32",
    "int16": "NUMBER32",
    "int8": "NUMBER32",
}


class SnowflakeOptions:
    """
    Configuration options for a Snowflake data source.
    """

    def __init__(
        self,
        database: Optional[str],
        schema: Optional[str],
        table: Optional[str],
        query: Optional[str],
    ):
        self.database = database or ""
        self.schema = schema or ""
        self.table = table or ""
        self.query = query or ""

    @classmethod
    def from_proto(cls, snowflake_options_proto: DataSourceProto.SnowflakeOptions):
        """
        Creates a SnowflakeOptions from a protobuf representation of a snowflake option.

        Args:
            snowflake_options_proto: A protobuf representation of a DataSource

        Returns:
            A SnowflakeOptions object based on the snowflake_options protobuf.
        """
        snowflake_options = cls(
            database=snowflake_options_proto.database,
            schema=snowflake_options_proto.schema,
            table=snowflake_options_proto.table,
            query=snowflake_options_proto.query,
        )

        return snowflake_options

    def to_proto(self) -> DataSourceProto.SnowflakeOptions:
        """
        Converts an SnowflakeOptionsProto object to its protobuf representation.

        Returns:
            A SnowflakeOptionsProto protobuf.
        """
        snowflake_options_proto = DataSourceProto.SnowflakeOptions(
            database=self.database,
            schema=self.schema,
            table=self.table,
            query=self.query,
        )

        return snowflake_options_proto


class SavedDatasetSnowflakeStorage(SavedDatasetStorage):
    _proto_attr_name = "snowflake_storage"

    snowflake_options: SnowflakeOptions

    def __init__(self, table_ref: str):
        self.snowflake_options = SnowflakeOptions(
            database=None,
            schema=None,
            table=table_ref,
            query=None,
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        return SavedDatasetSnowflakeStorage(
            table_ref=SnowflakeOptions.from_proto(storage_proto.snowflake_storage).table
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(
            snowflake_storage=self.snowflake_options.to_proto()
        )

    def to_data_source(self) -> DataSource:
        return SnowflakeSource(table=self.snowflake_options.table)


class SnowflakeLoggingDestination(LoggingDestination):
    table_name: str

    _proto_kind = "snowflake_destination"

    def __init__(self, *, table_name: str):
        self.table_name = table_name

    @classmethod
    def from_proto(cls, config_proto: LoggingConfigProto) -> "LoggingDestination":
        return SnowflakeLoggingDestination(
            table_name=config_proto.snowflake_destination.table_name,
        )

    def to_proto(self) -> LoggingConfigProto:
        return LoggingConfigProto(
            snowflake_destination=LoggingConfigProto.SnowflakeDestination(
                table_name=self.table_name,
            )
        )

    def to_data_source(self) -> DataSource:
        return SnowflakeSource(
            table=self.table_name,
        )
