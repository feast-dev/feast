from typing import Any, Dict, Iterable, Optional, Tuple

from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.type_map import iceberg_to_feast_value_type


class IcebergSource(DataSource):
    def __init__(
        self,
        *,
        name: Optional[str] = None,
        table_identifier: Optional[str] = None,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )
        self._iceberg_options = IcebergOptions(table_identifier=table_identifier)

    @property
    def table_identifier(self):
        return self._iceberg_options.table_identifier

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        return IcebergSource(
            name=data_source.name,
            table_identifier=data_source.iceberg_options.table_identifier,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            iceberg_options=self._iceberg_options.to_proto(),
            name=self.name,
            timestamp_field=self.timestamp_field,
            created_timestamp_column=self.created_timestamp_column,
            field_mapping=self.field_mapping,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )
        return data_source_proto

    def validate(self, config: RepoConfig):
        # TODO: Add validation logic
        pass

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        # This will be implemented when we have the pyiceberg catalog setup
        pass


class IcebergOptions:
    def __init__(self, table_identifier: Optional[str]):
        self._table_identifier = table_identifier

    @property
    def table_identifier(self):
        return self._table_identifier

    @staticmethod
    def from_proto(iceberg_options_proto: Any):
        return IcebergOptions(table_identifier=iceberg_options_proto.table_identifier)

    def to_proto(self) -> Any:
        # Note: We'll need to update the protobuf definitions to support IcebergOptions
        # For now, we'll use a placeholder or custom_options
        pass
