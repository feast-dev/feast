from typing import Callable, Dict, Iterable, List, Optional, Tuple

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
from feast.utils import get_user_agent
from feast.value_type import ValueType


@typechecked
class BigQuerySource(DataSource):
    """A BigQuerySource object defines a data source that a BigQueryOfflineStore class can use."""

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.BATCH_BIGQUERY

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        timestamp_field: Optional[str] = None,
        table: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        query: Optional[str] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """Create a BigQuerySource from an existing table or query.

        Args:
            name (optional): Name for the source. Defaults to the table if not specified, in which
                case the table must be specified.
            timestamp_field (optional): Event timestamp field used for point in time
                joins of feature values.
            table (optional): BigQuery table where the features are stored. Exactly one of 'table'
                and 'query' must be specified.
            table (optional): The BigQuery table where features can be found.
            created_timestamp_column (optional): Timestamp column when row was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data source to feature names in a feature table
                or view. Only used for feature columns, not entities or timestamp columns.
            query (optional): The query to be executed to obtain the features. Exactly one of 'table'
                and 'query' must be specified.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the bigquery source, typically the email of the primary
                maintainer.
        Example:
            >>> from feast import BigQuerySource
            >>> my_bigquery_source = BigQuerySource(table="gcp_project:bq_dataset.bq_table")
        """
        if table is None and query is None:
            raise ValueError('No "table" or "query" argument provided.')

        self.bigquery_options = BigQueryOptions(table=table, query=query)

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

    # Note: Python requires redefining hash in child classes that override __eq__
    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, BigQuerySource):
            raise TypeError(
                "Comparisons should only involve BigQuerySource class objects."
            )

        return (
            super().__eq__(other)
            and self.table == other.table
            and self.query == other.query
        )

    @property
    def table(self):
        return self.bigquery_options.table

    @property
    def query(self):
        return self.bigquery_options.query

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        assert data_source.HasField("bigquery_options")

        return BigQuerySource(
            name=data_source.name,
            field_mapping=dict(data_source.field_mapping),
            table=data_source.bigquery_options.table,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            query=data_source.bigquery_options.query,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_BIGQUERY,
            field_mapping=self.field_mapping,
            bigquery_options=self.bigquery_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            timestamp_field=self.timestamp_field,
            created_timestamp_column=self.created_timestamp_column,
        )

        return data_source_proto

    def validate(self, config: RepoConfig):
        if not self.query:
            from google.api_core.exceptions import NotFound
            from google.cloud import bigquery

            client = bigquery.Client()
            try:
                client.get_table(self.table)
            except NotFound:
                raise DataSourceNotFoundException(self.table)

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table:
            return f"`{self.table}`"
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.bq_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        try:
            from google.api_core import client_info as http_client_info
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("gcp", str(e))

        from google.cloud import bigquery

        project_id = (
            config.offline_store.billing_project_id or config.offline_store.project_id
        )
        client = bigquery.Client(
            project=project_id,
            location=config.offline_store.location,
            client_info=http_client_info.ClientInfo(user_agent=get_user_agent()),
        )
        if self.table:
            schema = client.get_table(self.table).schema
            if not isinstance(schema[0], bigquery.schema.SchemaField):
                raise TypeError("Could not parse BigQuery table schema.")
        else:
            bq_columns_query = f"SELECT * FROM ({self.query}) LIMIT 0"
            query_res = client.query(bq_columns_query).result()
            schema = query_res.schema

        name_type_pairs: List[Tuple[str, str]] = []
        for field in schema:
            bq_type_as_str = field.field_type
            if field.mode == "REPEATED":
                bq_type_as_str = "ARRAY<" + bq_type_as_str + ">"
            name_type_pairs.append((field.name, bq_type_as_str))

        return name_type_pairs


class BigQueryOptions:
    """
    Configuration options for a BigQuery data source.
    """

    def __init__(
        self,
        table: Optional[str],
        query: Optional[str],
    ):
        self.table = table or ""
        self.query = query or ""

    @classmethod
    def from_proto(cls, bigquery_options_proto: DataSourceProto.BigQueryOptions):
        """
        Creates a BigQueryOptions from a protobuf representation of a BigQuery option

        Args:
            bigquery_options_proto: A protobuf representation of a DataSource

        Returns:
            Returns a BigQueryOptions object based on the bigquery_options protobuf
        """
        bigquery_options = cls(
            table=bigquery_options_proto.table,
            query=bigquery_options_proto.query,
        )

        return bigquery_options

    def to_proto(self) -> DataSourceProto.BigQueryOptions:
        """
        Converts an BigQueryOptionsProto object to its protobuf representation.

        Returns:
            BigQueryOptionsProto protobuf
        """
        bigquery_options_proto = DataSourceProto.BigQueryOptions(
            table=self.table,
            query=self.query,
        )

        return bigquery_options_proto


class SavedDatasetBigQueryStorage(SavedDatasetStorage):
    _proto_attr_name = "bigquery_storage"

    bigquery_options: BigQueryOptions

    def __init__(self, table: str):
        self.bigquery_options = BigQueryOptions(table=table, query=None)

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        return SavedDatasetBigQueryStorage(
            table=BigQueryOptions.from_proto(storage_proto.bigquery_storage).table
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(
            bigquery_storage=self.bigquery_options.to_proto()
        )

    def to_data_source(self) -> DataSource:
        return BigQuerySource(table=self.bigquery_options.table)


class BigQueryLoggingDestination(LoggingDestination):
    _proto_kind = "bigquery_destination"

    table: str

    def __init__(self, *, table_ref):
        self.table = table_ref

    @classmethod
    def from_proto(cls, config_proto: LoggingConfigProto) -> "LoggingDestination":
        return BigQueryLoggingDestination(
            table_ref=config_proto.bigquery_destination.table_ref,
        )

    def to_data_source(self) -> DataSource:
        return BigQuerySource(table=self.table)

    def to_proto(self) -> LoggingConfigProto:
        return LoggingConfigProto(
            bigquery_destination=LoggingConfigProto.BigQueryDestination(
                table_ref=self.table
            )
        )
