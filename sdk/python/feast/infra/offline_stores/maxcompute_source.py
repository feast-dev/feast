from typing import Callable, Dict, Iterable, Optional, Tuple

from feast import type_map
from feast.data_source import DataSource
from feast.errors import DataSourceNotFoundException
from feast.infra.utils import aliyun_utils
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType
import odps

class MaxcomputeSource(DataSource):
    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        table_ref: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        query: Optional[str] = None,
    ):
        self._maxcompute_options = MaxcomputeOptions(table_ref=table_ref, query=query)

        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

    def __eq__(self, other):
        if not isinstance(other, MaxcomputeSource):
            raise TypeError(
                "Comparisons should only involve MaxcomputeSource class objects."
            )

        return (
            self.maxcompute_options.table_ref == other.maxcompute_options.table_ref
            and self.maxcompute_options.query == other.maxcompute_options.query
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def table_ref(self):
        return self._maxcompute_options.table_ref

    @property
    def query(self):
        return self._maxcompute_options.query

    @property
    def maxcompute_options(self):
        """
        Returns the maxcompute options of this data source
        """
        return self._maxcompute_options

    @maxcompute_options.setter
    def maxcompute_options(self, maxcompute_options):
        """
        Sets the maxcompute options of this data source
        """
        self._maxcompute_options = maxcompute_options

    @staticmethod
    def from_proto(data_source: DataSourceProto):

        assert data_source.HasField("maxcompute_options")

        return MaxcomputeSource(
            field_mapping=dict(data_source.field_mapping),
            table_ref=data_source.maxcompute_options.table_ref,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            query=data_source.maxcompute_options.query,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_BIGQUERY,
            field_mapping=self.field_mapping,
            maxcompute_options=self.maxcompute_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        from odps.errors import NoSuchObject as NoSuchObject

        if not self.query:
            client = aliyun_utils.get_maxcompute_client(
                ak=config.offline_store.access_key,
                sk=config.offline_store.secret_access_key,
                project=config.offline_store.project,
                region=config.offline_store.region,
                endpoint=config.offline_store.end_point,
            )

            try:
                client.get_table(self.table_ref)
            except NoSuchObject:
                raise DataSourceNotFoundException(self.table_ref)

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table_ref:
            return f"`{self.table_ref}`"
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.mc_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        client = aliyun_utils.get_maxcompute_client(
            ak=config.offline_store.access_key,
            sk=config.offline_store.secret_access_key,
            project=config.offline_store.project,
            region=config.offline_store.region,
            endpoint=config.offline_store.end_point,
        )

        if self.table_ref is not None:
            table_schema = client.get_table(self.table_ref).schema
            if not isinstance(
                table_schema[0], odps.models.table.TableSchema.TableColumn
            ):
                raise TypeError("Could not parse Maxcompute table schema.")

            name_type_pairs = [(field.name, field.type) for field in table_schema]
        else:
            mc_columns_query = f"SELECT * FROM ({self.query}) LIMIT 1"
            queryRes = client.execute_sql(mc_columns_query)
            with queryRes.open_reader() as reader:
                for row in reader:
                    name_type_pairs = [
                        (schema.name, schema.type) for schema in row._columns
                    ]
                    break

        return name_type_pairs


class MaxcomputeOptions:
    """
    DataSource Maxcompute options used to source features from Aliyun Maxcompute query
    """

    def __init__(self, table_ref: Optional[str], query: Optional[str]):
        self._table_ref = table_ref
        self._query = query

    @property
    def query(self):
        """
        Returns the Maxcompute SQL query referenced by this source
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Sets the Maxcompute SQL query referenced by this source
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
    def from_proto(cls, maxcompute_options_proto: DataSourceProto.MaxcomputeOptions):
        """
        Creates a MaxcomputeOptions from a protobuf representation of a Maxcompute option

        Args:
            maxcompute_options_proto: A protobuf representation of a DataSource

        Returns:
            Returns a MaxcomputeOptions object based on the maxcompute_options protobuf
        """

        maxcompute_options = cls(
            table_ref=maxcompute_options_proto.table_ref,
            query=maxcompute_options_proto.query,
        )

        return maxcompute_options

    def to_proto(self) -> DataSourceProto.MaxcomputeOptions:
        """
        Converts an MaxcomputeOptionsProto object to its protobuf representation.

        Returns:
            MaxcomputeOptionsProto protobuf
        """

        maxcompute_options_proto = DataSourceProto.MaxcomputeOptions(
            table_ref=self.table_ref, query=self.query
        )

        return maxcompute_options_proto
