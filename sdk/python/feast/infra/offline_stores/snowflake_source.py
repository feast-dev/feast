from typing import Callable, Dict, Iterable, Optional, Tuple

from feast import type_map
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.value_type import ValueType


class SnowflakeSource(DataSource):
    def __init__(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        query: Optional[str] = None,
        event_timestamp_column: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        """
        Creates a SnowflakeSource object.

        Args:
            database (optional): Snowflake database where the features are stored.
            schema (optional): Snowflake schema in which the table is located.
            table (optional): Snowflake table where the features are stored.
            event_timestamp_column (optional): Event timestamp column used for point in
                time joins of feature values.
            query (optional): The query to be executed to obtain the features.
            created_timestamp_column (optional): Timestamp column indicating when the
                row was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to column names in a feature table or view.
            date_partition_column (optional): Timestamp column used for partitioning.

        """
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

        # The default Snowflake schema is named "PUBLIC".
        _schema = "PUBLIC" if (database and table and not schema) else schema

        self._snowflake_options = SnowflakeOptions(
            database=database, schema=_schema, table=table, query=query
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
            field_mapping=dict(data_source.field_mapping),
            database=data_source.snowflake_options.database,
            schema=data_source.snowflake_options.schema,
            table=data_source.snowflake_options.table,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            query=data_source.snowflake_options.query,
        )

    def __eq__(self, other):
        if not isinstance(other, SnowflakeSource):
            raise TypeError(
                "Comparisons should only involve SnowflakeSource class objects."
            )

        return (
            self.snowflake_options.database == other.snowflake_options.database
            and self.snowflake_options.schema == other.snowflake_options.schema
            and self.snowflake_options.table == other.snowflake_options.table
            and self.snowflake_options.query == other.snowflake_options.query
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def database(self):
        """Returns the database of this snowflake source."""
        return self._snowflake_options.database

    @property
    def schema(self):
        """Returns the schema of this snowflake source."""
        return self._snowflake_options.schema

    @property
    def table(self):
        """Returns the table of this snowflake source."""
        return self._snowflake_options.table

    @property
    def query(self):
        """Returns the snowflake options of this snowflake source."""
        return self._snowflake_options.query

    @property
    def snowflake_options(self):
        """Returns the snowflake options of this snowflake source."""
        return self._snowflake_options

    @snowflake_options.setter
    def snowflake_options(self, _snowflake_options):
        """Sets the snowflake options of this snowflake source."""
        self._snowflake_options = _snowflake_options

    def to_proto(self) -> DataSourceProto:
        """
        Converts a SnowflakeSource object to its protobuf representation.

        Returns:
            A DataSourceProto object.
        """
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_SNOWFLAKE,
            field_mapping=self.field_mapping,
            snowflake_options=self.snowflake_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

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
    DataSource snowflake options used to source features from snowflake query.
    """

    def __init__(
        self,
        database: Optional[str],
        schema: Optional[str],
        table: Optional[str],
        query: Optional[str],
    ):
        self._database = database
        self._schema = schema
        self._table = table
        self._query = query

    @property
    def query(self):
        """Returns the snowflake SQL query referenced by this source."""
        return self._query

    @query.setter
    def query(self, query):
        """Sets the snowflake SQL query referenced by this source."""
        self._query = query

    @property
    def database(self):
        """Returns the database name of this snowflake table."""
        return self._database

    @database.setter
    def database(self, database):
        """Sets the database ref of this snowflake table."""
        self._database = database

    @property
    def schema(self):
        """Returns the schema name of this snowflake table."""
        return self._schema

    @schema.setter
    def schema(self, schema):
        """Sets the schema of this snowflake table."""
        self._schema = schema

    @property
    def table(self):
        """Returns the table name of this snowflake table."""
        return self._table

    @table.setter
    def table(self, table):
        """Sets the table ref of this snowflake table."""
        self._table = table

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
            database=None, schema=None, table=table_ref, query=None
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
