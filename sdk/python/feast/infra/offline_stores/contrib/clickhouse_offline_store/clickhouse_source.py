import json
from typing import Any, Callable, Iterable, Optional, Tuple, Type

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.container import Array
from clickhouse_connect.datatypes.numeric import (
    Boolean,
    Decimal,
    Float32,
    Float64,
    Int32,
    Int64,
)
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.string import String
from clickhouse_connect.datatypes.temporal import DateTime, DateTime64

from feast import RepoConfig, ValueType
from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.infra.utils.clickhouse.connection_utils import get_client
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.saved_dataset import SavedDatasetStorage


class ClickhouseOptions:
    def __init__(
        self,
        name: Optional[str],
        query: Optional[str],
        table: Optional[str],
    ):
        self._name = name or ""
        self._query = query or ""
        self._table = table or ""

    @classmethod
    def from_proto(cls, clickhouse_options_proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(clickhouse_options_proto.configuration.decode("utf8"))
        postgres_options = cls(
            name=config["name"], query=config["query"], table=config["table"]
        )

        return postgres_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        clickhouse_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {"name": self._name, "query": self._query, "table": self._table}
            ).encode()
        )
        return clickhouse_options_proto


class ClickhouseSource(DataSource):
    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        # TODO: Add ClickhouseSourceType to DataSourceProto
        return DataSourceProto.CUSTOM_SOURCE

    def __init__(
        self,
        name: Optional[str] = None,
        query: Optional[str] = None,
        table: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        self._clickhouse_options = ClickhouseOptions(
            name=name, query=query, table=table
        )

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
    def from_proto(data_source: DataSourceProto) -> Any:
        assert data_source.HasField("custom_options")

        postgres_options = json.loads(data_source.custom_options.configuration)

        return ClickhouseSource(
            name=postgres_options["name"],
            query=postgres_options["query"],
            table=postgres_options["table"],
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source.ClickhouseSource",
            field_mapping=self.field_mapping,
            custom_options=self._clickhouse_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return ch_type_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        with get_client(config.offline_store) as client:
            result = client.query(
                f"SELECT * FROM {self.get_table_query_string()} AS sub LIMIT 0"
            )
            column_types = list(zip(result.column_names, result.column_types))
            return [
                (name, _ch_type_to_ch_type_str(type_)) for name, type_ in column_types
            ]

    def get_table_query_string(self) -> str:
        if self._clickhouse_options._table:
            return f"{self._clickhouse_options._table}"
        else:
            return f"({self._clickhouse_options._query})"


class SavedDatasetClickhouseStorage(SavedDatasetStorage):
    _proto_attr_name = "custom_storage"

    clickhouse_options: ClickhouseOptions

    def __init__(self, table_ref: str):
        self.clickhouse_options = ClickhouseOptions(
            table=table_ref, name=None, query=None
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        return SavedDatasetClickhouseStorage(
            table_ref=ClickhouseOptions.from_proto(storage_proto.custom_storage)._table
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(
            custom_storage=self.clickhouse_options.to_proto()
        )

    def to_data_source(self) -> DataSource:
        return ClickhouseSource(table=self.clickhouse_options._table)


def ch_type_to_feast_value_type(type_str: str) -> ValueType:
    type_obj = get_from_name(type_str)
    type_cls = type(type_obj)
    container_type = None
    if isinstance(type_obj, Array):
        container_type = Array
    type_map: dict[
        tuple[Optional[Type[ClickHouseType]], Type[ClickHouseType]], ValueType
    ] = {
        (None, Boolean): ValueType.BOOL,
        (None, String): ValueType.STRING,
        (None, Float32): ValueType.FLOAT,
        (None, Float64): ValueType.DOUBLE,
        (None, Decimal): ValueType.DOUBLE,
        (None, Int32): ValueType.INT32,
        (None, Int64): ValueType.INT64,
        (None, DateTime): ValueType.UNIX_TIMESTAMP,
        (None, DateTime64): ValueType.UNIX_TIMESTAMP,
        (Array, Boolean): ValueType.BOOL_LIST,
        (Array, String): ValueType.STRING_LIST,
        (Array, Float32): ValueType.FLOAT_LIST,
        (Array, Float64): ValueType.DOUBLE_LIST,
        (Array, Decimal): ValueType.DOUBLE_LIST,
        (Array, Int32): ValueType.INT32_LIST,
        (Array, Int64): ValueType.INT64_LIST,
        (Array, DateTime): ValueType.UNIX_TIMESTAMP_LIST,
        (Array, DateTime64): ValueType.UNIX_TIMESTAMP_LIST,
    }
    value = type_map.get((container_type, type_cls), ValueType.UNKNOWN)
    if value == ValueType.UNKNOWN:
        print("unknown type:", type_str)
    return value


def _ch_type_to_ch_type_str(type_: ClickHouseType) -> str:
    return type_.name
