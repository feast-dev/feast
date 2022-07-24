import warnings
from typing import Callable, Dict, Iterable, Optional, Tuple

from typeguard import typechecked

from feast import type_map
from feast.data_source import DataSource
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
    def __init__(
        self,
        *,
        database: Optional[str] = None,
        warehouse: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        query: Optional[str] = None,
        event_timestamp_column: Optional[str] = "",
        date_partition_column: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        name: Optional[str] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        timestamp_field: Optional[str] = "",
    ):
        """
        Creates a SnowflakeSource object.

        Args:
            database (optional): Snowflake database where the features are stored.
            warehouse (optional): Snowflake warehouse where the database is stored.
            schema (optional): Snowflake schema in which the table is located.
            table (optional): Snowflake table where the features are stored.
            event_timestamp_column (optional): (Deprecated in favor of timestamp_field) Event
                timestamp column used for point in time joins of feature values.
            query (optional): The query to be executed to obtain the features.
            created_timestamp_column (optional): Timestamp column indicating when the
                row was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to column names in a feature table or view.
            date_partition_column (deprecated): Timestamp column used for partitioning.
            name (optional): Name for the source. Defaults to the table if not specified.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the snowflake source, typically the email of the primary
                maintainer.
        """
        if table is None and query is None:
            raise ValueError('No "table" argument provided.')

        # The default Snowflake schema is named "PUBLIC".
        _schema = "PUBLIC" if (database and table and not schema) else schema

        self.snowflake_options = SnowflakeOptions(
            database=database,
            schema=_schema,
            table=table,
            query=query,
            warehouse=warehouse,
        )

        # If no name, use the table as the default name
        _name = name
        if not _name:
            if table:
                _name = table
            else:
                warnings.warn(
                    (
                        f"Starting in Feast 0.24, Feast will require either a name for a data source (if using query) "
                        f"or `table`: {self.query}"
                    ),
                    DeprecationWarning,
                )

        if date_partition_column:
            warnings.warn(
                (
                    "The argument 'date_partition_column' is not supported for Snowflake sources."
                    "It will be removed in Feast 0.24+"
                ),
                DeprecationWarning,
            )

        super().__init__(
            name=_name if _name else "",
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
            timestamp_field=timestamp_field,
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
            field_mapping=dict(data_source.field_mapping),
            database=data_source.snowflake_options.database,
            schema=data_source.snowflake_options.schema,
            table=data_source.snowflake_options.table,
            warehouse=data_source.snowflake_options.warehouse,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
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
            and self.warehouse == other.warehouse
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

    @property
    def warehouse(self):
        """Returns the warehouse of this snowflake source."""
        return self.snowflake_options.warehouse

    def to_proto(self) -> DataSourceProto:
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
        if self.database and self.table:
            return f'"{self.database}"."{self.schema}"."{self.table}"'
        elif self.table:
            return f'"{self.table}"'
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.snowflake_python_type_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """
        Returns a mapping of column names to types for this snowflake source.

        Args:
            config: A RepoConfig describing the feature repo
        """

        from feast.infra.offline_stores.snowflake import SnowflakeOfflineStoreConfig
        from feast.infra.utils.snowflake_utils import (
            execute_snowflake_statement,
            get_snowflake_conn,
        )

        assert isinstance(config.offline_store, SnowflakeOfflineStoreConfig)

        snowflake_conn = get_snowflake_conn(config.offline_store)

        if self.database and self.table:
            query = f'SELECT * FROM "{self.database}"."{self.schema}"."{self.table}" LIMIT 1'
        elif self.table:
            query = f'SELECT * FROM "{self.table}" LIMIT 1'
        else:
            query = f"SELECT * FROM ({self.query}) LIMIT 1"

        result = execute_snowflake_statement(snowflake_conn, query).fetch_pandas_all()

        if not result.empty:
            metadata = result.dtypes.apply(str)
            return list(zip(metadata.index, metadata))
        else:
            raise ValueError("The following source:\n" + query + "\n ... is empty")


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
        warehouse: Optional[str],
    ):
        self.database = database or ""
        self.schema = schema or ""
        self.table = table or ""
        self.query = query or ""
        self.warehouse = warehouse or ""

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
            warehouse=snowflake_options_proto.warehouse,
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
            warehouse=self.warehouse,
        )

        return snowflake_options_proto


class SavedDatasetSnowflakeStorage(SavedDatasetStorage):
    _proto_attr_name = "snowflake_storage"

    snowflake_options: SnowflakeOptions

    def __init__(self, table_ref: str):
        self.snowflake_options = SnowflakeOptions(
            database=None, schema=None, table=table_ref, query=None, warehouse=None
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
