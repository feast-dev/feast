from typing import Callable, Dict, Iterable, Optional, Tuple

from typeguard import typechecked

from feast import type_map
from feast.data_source import DataSource
from feast.errors import (
    DataSourceNoNameException,
    DataSourceNotFoundException,
    RedshiftCredentialsError,
)
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
class RedshiftSource(DataSource):
    """A RedshiftSource object defines a data source that a RedshiftOfflineStore class can use."""

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.BATCH_REDSHIFT

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        table: Optional[str] = None,
        schema: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        query: Optional[str] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        database: Optional[str] = "",
    ):
        """
        Creates a RedshiftSource object.

        Args:
            name (optional): Name for the source. Defaults to the table if not specified, in which
                case the table must be specified.
            timestamp_field (optional): Event timestamp field used for point in time
                joins of feature values.
            table (optional): Redshift table where the features are stored. Exactly one of 'table'
                and 'query' must be specified.
            schema (optional): Redshift schema in which the table is located.
            created_timestamp_column (optional): Timestamp column indicating when the
                row was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to column names in a feature table or view.
            query (optional): The query to be executed to obtain the features. Exactly one of 'table'
                and 'query' must be specified.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the redshift source, typically the email of the primary
                maintainer.
            database (optional): The Redshift database name.
        """
        if table is None and query is None:
            raise ValueError('No "table" or "query" argument provided.')

        # The default Redshift schema is named "public".
        _schema = "public" if table and not schema else schema
        self.redshift_options = RedshiftOptions(
            table=table, schema=_schema, query=query, database=database
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
        Creates a RedshiftSource from a protobuf representation of a RedshiftSource.

        Args:
            data_source: A protobuf representation of a RedshiftSource

        Returns:
            A RedshiftSource object based on the data_source protobuf.
        """
        return RedshiftSource(
            name=data_source.name,
            timestamp_field=data_source.timestamp_field,
            table=data_source.redshift_options.table,
            schema=data_source.redshift_options.schema,
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            query=data_source.redshift_options.query,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
            database=data_source.redshift_options.database,
        )

    # Note: Python requires redefining hash in child classes that override __eq__
    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, RedshiftSource):
            raise TypeError(
                "Comparisons should only involve RedshiftSource class objects."
            )

        return (
            super().__eq__(other)
            and self.redshift_options.table == other.redshift_options.table
            and self.redshift_options.schema == other.redshift_options.schema
            and self.redshift_options.query == other.redshift_options.query
            and self.redshift_options.database == other.redshift_options.database
        )

    @property
    def table(self):
        """Returns the table of this Redshift source."""
        return self.redshift_options.table

    @property
    def schema(self):
        """Returns the schema of this Redshift source."""
        return self.redshift_options.schema

    @property
    def query(self):
        """Returns the Redshift query of this Redshift source."""
        return self.redshift_options.query

    @property
    def database(self):
        """Returns the Redshift database of this Redshift source."""
        return self.redshift_options.database

    def _to_proto_impl(self) -> DataSourceProto:
        """
        Converts a RedshiftSource object to its protobuf representation.

        Returns:
            A DataSourceProto object.
        """
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_REDSHIFT,
            field_mapping=self.field_mapping,
            redshift_options=self.redshift_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            timestamp_field=self.timestamp_field,
            created_timestamp_column=self.created_timestamp_column,
        )

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
        if self.table:
            try:
                paginator = client.get_paginator("describe_table")

                paginator_kwargs = {
                    "Database": (
                        self.database
                        if self.database
                        else config.offline_store.database
                    ),
                    "Table": self.table,
                    "Schema": self.schema,
                }

                if config.offline_store.cluster_id:
                    # Provisioned cluster
                    paginator_kwargs["ClusterIdentifier"] = (
                        config.offline_store.cluster_id
                    )
                    paginator_kwargs["DbUser"] = config.offline_store.user
                elif config.offline_store.workgroup:
                    # Redshift serverless
                    paginator_kwargs["WorkgroupName"] = config.offline_store.workgroup

                response_iterator = paginator.paginate(**paginator_kwargs)
                table = response_iterator.build_full_result()

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
                config.offline_store.workgroup,
                self.database if self.database else config.offline_store.database,
                config.offline_store.user,
                f"SELECT * FROM ({self.query}) LIMIT 1",
            )
            columns = aws_utils.get_redshift_statement_result(client, statement_id)[
                "ColumnMetadata"
            ]

        return [(column["name"], column["typeName"].upper()) for column in columns]


class RedshiftOptions:
    """
    Configuration options for a Redshift data source.
    """

    def __init__(
        self,
        table: Optional[str],
        schema: Optional[str],
        query: Optional[str],
        database: Optional[str],
    ):
        self.table = table or ""
        self.schema = schema or ""
        self.query = query or ""
        self.database = database or ""

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
            database=redshift_options_proto.database,
        )

        return redshift_options

    @property
    def fully_qualified_table_name(self) -> str:
        """
        The fully qualified table name of this Redshift table.

        Returns:
            A string in the format of <database>.<schema>.<table>
            May be empty or None if the table is not set
        """

        if not self.table:
            return ""

        # self.table may already contain the database and schema
        parts = self.table.split(".")
        if len(parts) == 3:
            database, schema, table = parts
        elif len(parts) == 2:
            database = self.database
            schema, table = parts
        elif len(parts) == 1:
            database = self.database
            schema = self.schema
            table = parts[0]
        else:
            raise ValueError(
                f"Invalid table name: {self.table} - can't determine database and schema"
            )

        if database and schema:
            return f"{database}.{schema}.{table}"
        elif schema:
            return f"{schema}.{table}"
        else:
            return table

    def to_proto(self) -> DataSourceProto.RedshiftOptions:
        """
        Converts an RedshiftOptionsProto object to its protobuf representation.

        Returns:
            A RedshiftOptionsProto protobuf.
        """
        redshift_options_proto = DataSourceProto.RedshiftOptions(
            table=self.table,
            schema=self.schema,
            query=self.query,
            database=self.database,
        )

        return redshift_options_proto


class SavedDatasetRedshiftStorage(SavedDatasetStorage):
    _proto_attr_name = "redshift_storage"

    redshift_options: RedshiftOptions

    def __init__(self, table_ref: str):
        self.redshift_options = RedshiftOptions(
            table=table_ref, schema=None, query=None, database=None
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        return SavedDatasetRedshiftStorage(
            table_ref=RedshiftOptions.from_proto(storage_proto.redshift_storage).table
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(
            redshift_storage=self.redshift_options.to_proto()
        )

    def to_data_source(self) -> DataSource:
        return RedshiftSource(table=self.redshift_options.table)


class RedshiftLoggingDestination(LoggingDestination):
    _proto_kind = "redshift_destination"

    table_name: str

    def __init__(self, *, table_name: str):
        self.table_name = table_name

    @classmethod
    def from_proto(cls, config_proto: LoggingConfigProto) -> "LoggingDestination":
        return RedshiftLoggingDestination(
            table_name=config_proto.redshift_destination.table_name,
        )

    def to_proto(self) -> LoggingConfigProto:
        return LoggingConfigProto(
            redshift_destination=LoggingConfigProto.RedshiftDestination(
                table_name=self.table_name
            )
        )

    def to_data_source(self) -> DataSource:
        return RedshiftSource(table=self.table_name)
