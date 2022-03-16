import pickle
from typing import Callable, Dict, Iterable, Optional, Tuple

from feast import ValueType
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast_trino.trino_type_map import trino_to_feast_value_type
from feast_trino.trino_utils import Trino


class TrinoOptions:
    """
    DataSource Trino options used to source features from Trino query
    """

    def __init__(self, table_ref: Optional[str], query: Optional[str]):
        self._table_ref = table_ref
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
    def table_ref(self):
        """
        Returns the table ref of this Trino table
        """
        return self._table_ref

    @table_ref.setter
    def table_ref(self, table_ref):
        """
        Sets the table ref of this Trino table
        """
        self._table_ref = table_ref

    @classmethod
    def from_proto(self, trino_options_proto: DataSourceProto.CustomSourceOptions):
        """
        Creates a TrinoOptions from a protobuf representation of a Trino option
        Args:
            trino_options_proto: A protobuf representation of a DataSource
        Returns:
            Returns a TrinoOptions object based on the trino_options protobuf
        """
        trino_configuration = pickle.loads(trino_options_proto.configuration)

        trino_options = self(
            table_ref=trino_configuration.table_ref, query=trino_configuration.query
        )

        return trino_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        """
        Converts an TrinoOptionsProto object to its protobuf representation.
        Returns:
            TrinoOptionsProto protobuf
        """

        trino_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=pickle.dumps(self),
        )

        return trino_options_proto


class TrinoSource(DataSource):
    def __init__(
        self,
        name: str,
        event_timestamp_column: Optional[str] = "",
        table_ref: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        query: Optional[str] = None,
    ):
        super().__init__(
            name,
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

        self._trino_options = TrinoOptions(table_ref=table_ref, query=query)

    def __eq__(self, other):
        if not isinstance(other, TrinoSource):
            raise TypeError(
                "Comparisons should only involve TrinoSource class objects."
            )

        return (
            self.trino_options.table_ref == other.trino_options.table_ref
            and self.trino_options.query == other.trino_options.query
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def table_ref(self):
        return self._trino_options.table_ref

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

        assert data_source.HasField("custom_options")

        trino_options = TrinoOptions.from_proto(data_source.custom_options)
        return TrinoSource(
            name=data_source.name, # TODO: Validate
            field_mapping=dict(data_source.field_mapping),
            table_ref=trino_options.table_ref,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            query=trino_options.query,
        )

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            field_mapping=self.field_mapping,
            custom_options=self.trino_options.to_proto(),
        )

        data_source_proto.name = self.name # TODO: Validate
        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column
        return data_source_proto

    def validate(self, config: RepoConfig):
        self.get_table_column_names_and_types(config)

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        return self.table_ref or self.query

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return trino_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        client = Trino(
            user="user",
            catalog=config.offline_store.catalog,
            host=config.offline_store.host,
            port=config.offline_store.port,
        )
        if self.table_ref is not None:
            table_schema = client.execute_query(
                f"SELECT * FROM {self.table_ref} LIMIT 1"
            ).schema
        else:
            table_schema = client.execute_query(
                f"SELECT * FROM ({self.query}) LIMIT 1"
            ).schema

        return [
            (field_name, field_type) for field_name, field_type in table_schema.items()
        ]
