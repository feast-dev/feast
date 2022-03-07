import warnings
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from feast import type_map
from feast.data_source import DataSource
from feast.errors import DataSourceNotFoundException
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.value_type import ValueType


class BigQuerySource(DataSource):
    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        table: Optional[str] = None,
        table_ref: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        query: Optional[str] = None,
        name: Optional[str] = None,
    ):
        """Create a BigQuerySource from an existing table or query.

         Args:
             table (optional): The BigQuery table where features can be found.
             table_ref (optional): (Deprecated) The BigQuery table where features can be found.
             event_timestamp_column: Event timestamp column used for point in time joins of feature values.
             created_timestamp_column (optional): Timestamp column when row was created, used for deduplicating rows.
             field_mapping: A dictionary mapping of column names in this data source to feature names in a feature table
                 or view. Only used for feature columns, not entities or timestamp columns.
             date_partition_column (optional): Timestamp column used for partitioning.
             query (optional): SQL query to execute to generate data for this data source.
             name (optional): Name for the source. Defaults to the table_ref if not specified.
         Example:
             >>> from feast import BigQuerySource
             >>> my_bigquery_source = BigQuerySource(table="gcp_project:bq_dataset.bq_table")
         """
        if table is None and table_ref is None and query is None:
            raise ValueError('No "table" or "query" argument provided.')
        if not table and table_ref:
            warnings.warn(
                (
                    "The argument 'table_ref' is being deprecated. Please use 'table' "
                    "instead. Feast 0.20 and onwards will not support the argument 'table_ref'."
                ),
                DeprecationWarning,
            )
            table = table_ref
        self.bigquery_options = BigQueryOptions(table_ref=table, query=query)

        # If no name, use the table_ref as the default name
        _name = name
        if not _name:
            if table:
                _name = table
            elif table_ref:
                _name = table_ref
            else:
                warnings.warn(
                    (
                        "Starting in Feast 0.21, Feast will require either a name for a data source (if using query) or `table`."
                    ),
                    DeprecationWarning,
                )

        super().__init__(
            _name if _name else "",
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
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
            self.name == other.name
            and self.bigquery_options.table_ref == other.bigquery_options.table_ref
            and self.bigquery_options.query == other.bigquery_options.query
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def table_ref(self):
        return self.bigquery_options.table_ref

    @property
    def query(self):
        return self.bigquery_options.query

    @staticmethod
    def from_proto(data_source: DataSourceProto):

        assert data_source.HasField("bigquery_options")

        return BigQuerySource(
            name=data_source.name,
            field_mapping=dict(data_source.field_mapping),
            table_ref=data_source.bigquery_options.table_ref,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            query=data_source.bigquery_options.query,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_BIGQUERY,
            field_mapping=self.field_mapping,
            bigquery_options=self.bigquery_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        if not self.query:
            from google.api_core.exceptions import NotFound
            from google.cloud import bigquery

            client = bigquery.Client()
            try:
                client.get_table(self.table_ref)
            except NotFound:
                raise DataSourceNotFoundException(self.table_ref)

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table_ref:
            return f"`{self.table_ref}`"
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.bq_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from google.cloud import bigquery

        client = bigquery.Client()
        if self.table_ref is not None:
            schema = client.get_table(self.table_ref).schema
            if not isinstance(schema[0], bigquery.schema.SchemaField):
                raise TypeError("Could not parse BigQuery table schema.")
        else:
            bq_columns_query = f"SELECT * FROM ({self.query}) LIMIT 1"
            queryRes = client.query(bq_columns_query).result()
            schema = queryRes.schema

        name_type_pairs: List[Tuple[str, str]] = []
        for field in schema:
            bq_type_as_str = field.field_type
            if field.mode == "REPEATED":
                bq_type_as_str = "ARRAY<" + bq_type_as_str + ">"
            name_type_pairs.append((field.name, bq_type_as_str))

        return name_type_pairs


class BigQueryOptions:
    """
    DataSource BigQuery options used to source features from BigQuery query
    """

    def __init__(
        self, table_ref: Optional[str], query: Optional[str],
    ):
        self._table_ref = table_ref
        self._query = query

    @property
    def query(self):
        """
        Returns the BigQuery SQL query referenced by this source
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Sets the BigQuery SQL query referenced by this source
        """
        self._query = query

    @property
    def table_ref(self):
        """
        Returns the table ref of this BQ table
        """
        return self._table_ref

    @table_ref.setter
    def table_ref(self, table_ref):
        """
        Sets the table ref of this BQ table
        """
        self._table_ref = table_ref

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
            table_ref=bigquery_options_proto.table_ref,
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
            table_ref=self.table_ref, query=self.query,
        )

        return bigquery_options_proto


class SavedDatasetBigQueryStorage(SavedDatasetStorage):
    _proto_attr_name = "bigquery_storage"

    bigquery_options: BigQueryOptions

    def __init__(self, table_ref: str):
        self.bigquery_options = BigQueryOptions(table_ref=table_ref, query=None)

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        return SavedDatasetBigQueryStorage(
            table_ref=BigQueryOptions.from_proto(
                storage_proto.bigquery_storage
            ).table_ref
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(
            bigquery_storage=self.bigquery_options.to_proto()
        )

    def to_data_source(self) -> DataSource:
        return BigQuerySource(table_ref=self.bigquery_options.table_ref)
