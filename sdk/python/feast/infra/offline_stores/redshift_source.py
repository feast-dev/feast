from typing import Callable, Dict, Iterable, Optional, Tuple

from feast import type_map
from feast.data_source import DataSource
from feast.errors import DataSourceNotFoundException, RedshiftCredentialsError
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType


class RedshiftSource(DataSource):
    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        table: Optional[str] = None,
        schema: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        query: Optional[str] = None,
    ):
        """
        Creates a RedshiftSource object.

        Args:
            event_timestamp_column (optional): Event timestamp column used for point in
                time joins of feature values.
            table (optional): Redshift table where the features are stored.
            schema (optional): Redshift schema in which the table is located.
            created_timestamp_column (optional): Timestamp column indicating when the
                row was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to column names in a feature table or view.
            date_partition_column (optional): Timestamp column used for partitioning.
            query (optional): The query to be executed to obtain the features.
        """
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

        # The default Redshift schema is named "public".
        _schema = "public" if table and not schema else schema

        self._redshift_options = RedshiftOptions(
            table=table, schema=_schema, query=query
        )

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        """
        Creates a RedshiftSource from a protobuf representation of a RedshiftSource.

        Args:
            data_source: A protobuf representation of a RedshiftSource

        Returns:
            A RedshiftSource object based on the data_source protobuf.
        """
        return RedshiftSource(
            field_mapping=dict(data_source.field_mapping),
            table=data_source.redshift_options.table,
            schema=data_source.redshift_options.schema,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            query=data_source.redshift_options.query,
        )

    def __eq__(self, other):
        if not isinstance(other, RedshiftSource):
            raise TypeError(
                "Comparisons should only involve RedshiftSource class objects."
            )

        return (
            self.redshift_options.table == other.redshift_options.table
            and self.redshift_options.schema == other.redshift_options.schema
            and self.redshift_options.query == other.redshift_options.query
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def table(self):
        """Returns the table of this Redshift source."""
        return self._redshift_options.table

    @property
    def schema(self):
        """Returns the schema of this Redshift source."""
        return self._redshift_options.schema

    @property
    def query(self):
        """Returns the Redshift options of this Redshift source."""
        return self._redshift_options.query

    @property
    def redshift_options(self):
        """Returns the Redshift options of this Redshift source."""
        return self._redshift_options

    @redshift_options.setter
    def redshift_options(self, _redshift_options):
        """Sets the Redshift options of this Redshift source."""
        self._redshift_options = _redshift_options

    def to_proto(self) -> DataSourceProto:
        """
        Converts a RedshiftSource object to its protobuf representation.

        Returns:
            A DataSourceProto object.
        """
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_REDSHIFT,
            field_mapping=self.field_mapping,
            redshift_options=self.redshift_options.to_proto(),
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
        if self.table:
            return f'"{self.schema}"."{self.table}"'
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.redshift_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """
        Returns a mapping of column names to types for this Redshift source.

        Args:
            config: A RepoConfig describing the feature repo
        """
        from botocore.exceptions import ClientError

        from feast.infra.offline_stores.redshift import RedshiftOfflineStoreConfig
        from feast.infra.utils import aws_utils

        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)

        client = aws_utils.get_redshift_data_client(config.offline_store.region)

        if self.table is not None:
            try:
                table = client.describe_table(
                    ClusterIdentifier=config.offline_store.cluster_id,
                    Database=config.offline_store.database,
                    DbUser=config.offline_store.user,
                    Table=self.table,
                    Schema=self.schema,
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "ValidationException":
                    raise RedshiftCredentialsError() from e
                raise

            # The API returns valid JSON with empty column list when the table doesn't exist
            if len(table["ColumnList"]) == 0:
                raise DataSourceNotFoundException(self.table)

            columns = table["ColumnList"]
        else:
            statement_id = aws_utils.execute_redshift_statement(
                client,
                config.offline_store.cluster_id,
                config.offline_store.database,
                config.offline_store.user,
                f"SELECT * FROM ({self.query}) LIMIT 1",
            )
            columns = aws_utils.get_redshift_statement_result(client, statement_id)[
                "ColumnMetadata"
            ]

        return [(column["name"], column["typeName"].upper()) for column in columns]


class RedshiftOptions:
    """
    DataSource Redshift options used to source features from Redshift query.
    """

    def __init__(
        self, table: Optional[str], schema: Optional[str], query: Optional[str]
    ):
        self._table = table
        self._schema = schema
        self._query = query

    @property
    def query(self):
        """Returns the Redshift SQL query referenced by this source."""
        return self._query

    @query.setter
    def query(self, query):
        """Sets the Redshift SQL query referenced by this source."""
        self._query = query

    @property
    def table(self):
        """Returns the table name of this Redshift table."""
        return self._table

    @table.setter
    def table(self, table_name):
        """Sets the table ref of this Redshift table."""
        self._table = table_name

    @property
    def schema(self):
        """Returns the schema name of this Redshift table."""
        return self._schema

    @schema.setter
    def schema(self, schema):
        """Sets the schema of this Redshift table."""
        self._schema = schema

    @classmethod
    def from_proto(cls, redshift_options_proto: DataSourceProto.RedshiftOptions):
        """
        Creates a RedshiftOptions from a protobuf representation of a Redshift option.

        Args:
            redshift_options_proto: A protobuf representation of a DataSource

        Returns:
            A RedshiftOptions object based on the redshift_options protobuf.
        """
        redshift_options = cls(
            table=redshift_options_proto.table,
            schema=redshift_options_proto.schema,
            query=redshift_options_proto.query,
        )

        return redshift_options

    def to_proto(self) -> DataSourceProto.RedshiftOptions:
        """
        Converts an RedshiftOptionsProto object to its protobuf representation.

        Returns:
            A RedshiftOptionsProto protobuf.
        """
        redshift_options_proto = DataSourceProto.RedshiftOptions(
            table=self.table, schema=self.schema, query=self.query,
        )

        return redshift_options_proto
