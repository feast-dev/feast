from typing import Callable, Dict, Iterable, Optional, Tuple

from feast import ValueType
from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import Trino
from feast.infra.offline_stores.contrib.trino_offline_store.trino_type_map import (
    trino_to_feast_value_type,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage


class TrinoOptions:
    """
    DataSource Trino options used to source features from Trino query
    """

    def __init__(self, table: Optional[str], query: Optional[str]):
        self._table = table
        self._query = query

    @property
    def query(self):
        """
        Returns the Trino SQL query referenced by this source
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Sets the Trino SQL query referenced by this source
        """
        self._query = query

    @property
    def table(self):
        """
        Returns the table ref of this Trino table
        """
        return self._table

    @table.setter
    def table(self, table):
        """
        Sets the table ref of this Trino table
        """
        self._table = table

    @classmethod
    def from_proto(cls, trino_options_proto: DataSourceProto.TrinoOptions):
        """
        Creates a TrinoOptions from a protobuf representation of a Trino option
        Args:
            trino_options_proto: A protobuf representation of a DataSource
        Returns:
            Returns a TrinoOptions object based on the trino_options protobuf
        """
        trino_options = cls(
            table=trino_options_proto.table,
            query=trino_options_proto.query,
        )

        return trino_options

    def to_proto(self) -> DataSourceProto.TrinoOptions:
        """
        Converts an TrinoOptionsProto object to its protobuf representation.
        Returns:
            TrinoOptionsProto protobuf
        """

        trino_options_proto = DataSourceProto.TrinoOptions(
            table=self.table,
            query=self.query,
        )

        return trino_options_proto


class TrinoSource(DataSource):
    """A TrinoSource object defines a data source that a TrinoOfflineStore class can use."""

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.BATCH_TRINO

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
        """
        Creates a TrinoSource object.

        Args:
            name (optional): Name for the source. Defaults to the table if not specified, in which
                case the table must be specified.
            timestamp_field (optional): Event timestamp field used for point in time
                joins of feature values.
            table (optional): Trino table where the features are stored. Exactly one of 'table' and
                'query' must be specified.
            created_timestamp_column (optional): Timestamp column indicating when the
                row was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to column names in a feature table or view.
            query (optional): The query to be executed to obtain the features. Exactly one of 'table'
                and 'query' must be specified.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the snowflake source, typically the email of the primary
                maintainer.
        """
        # If no name, use the table as the default name.
        if name is None and table is None:
            raise DataSourceNoNameException()
        name = name or table
        assert name

        super().__init__(
            name=name if name else "",
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

        self._trino_options = TrinoOptions(table=table, query=query)

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, TrinoSource):
            raise TypeError(
                "Comparisons should only involve TrinoSource class objects."
            )

        return (
            super().__eq__(other)
            and self.name == other.name
            and self.trino_options.table == other.trino_options.table
            and self.trino_options.query == other.trino_options.query
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
            and self.description == other.description
            and self.tags == other.tags
            and self.owner == other.owner
        )

    @property
    def table(self):
        return self._trino_options.table

    @property
    def query(self):
        return self._trino_options.query

    @property
    def trino_options(self):
        """
        Returns the Trino options of this data source
        """
        return self._trino_options

    @trino_options.setter
    def trino_options(self, trino_options):
        """
        Sets the Trino options of this data source
        """
        self._trino_options = trino_options

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        assert data_source.HasField("trino_options")

        return TrinoSource(
            name=data_source.name,
            field_mapping=dict(data_source.field_mapping),
            table=data_source.trino_options.table,
            query=data_source.trino_options.query,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_TRINO,
            field_mapping=self.field_mapping,
            trino_options=self.trino_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        self.get_table_column_names_and_types(config)

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        return self.table or self.query

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return trino_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        auth = None
        if config.offline_store.auth is not None:
            auth = config.offline_store.auth.to_trino_auth()

        client = Trino(
            catalog=config.offline_store.catalog,
            host=config.offline_store.host,
            port=config.offline_store.port,
            user=config.offline_store.user,
            source=config.offline_store.source,
            http_scheme=config.offline_store.http_scheme,
            verify=config.offline_store.verify,
            extra_credential=config.offline_store.extra_credential,
            auth=auth,
        )
        if self.table:
            table_schema = client.execute_query(
                f"SELECT * FROM {self.table} LIMIT 1"
            ).schema
        else:
            table_schema = client.execute_query(
                f"SELECT * FROM ({self.query}) LIMIT 1"
            ).schema

        return [
            (field_name, field_type) for field_name, field_type in table_schema.items()
        ]


class SavedDatasetTrinoStorage(SavedDatasetStorage):
    _proto_attr_name = "trino_storage"

    trino_options: TrinoOptions

    def __init__(self, table: Optional[str] = None, query: Optional[str] = None):
        self.trino_options = TrinoOptions(table=table, query=query)

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        # TODO: implementation is not correct. Needs fix and update to protos.
        return SavedDatasetTrinoStorage(
            table=TrinoOptions.from_proto(storage_proto.trino_storage).table
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(trino_storage=self.trino_options.to_proto())

    def to_data_source(self) -> DataSource:
        return TrinoSource(table=self.trino_options.table)
