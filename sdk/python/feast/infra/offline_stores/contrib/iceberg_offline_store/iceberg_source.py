import json
from typing import Callable, Dict, Iterable, Optional, Tuple

from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.type_map import iceberg_to_feast_value_type
from feast.value_type import ValueType


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
        iceberg_options = IcebergOptions.from_proto(data_source.custom_options.configuration)
        return IcebergSource(
            name=data_source.name,
            table_identifier=iceberg_options.table_identifier,
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
            name=self.name,
            timestamp_field=self.timestamp_field,
            created_timestamp_column=self.created_timestamp_column,
            field_mapping=self.field_mapping,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            data_source_class_type="feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source.IcebergSource",
        )
        # Use custom_options to store Iceberg-specific configuration
        data_source_proto.custom_options.configuration = self._iceberg_options.to_proto()
        return data_source_proto

    def validate(self, config: RepoConfig):
        # TODO: Add validation logic
        pass

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """
        Return schema from Iceberg table.
        """
        from pyiceberg.catalog import load_catalog

        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            IcebergOfflineStoreConfig,
        )

        if not isinstance(config.offline_store, IcebergOfflineStoreConfig):
            raise ValueError("Iceberg data source requires IcebergOfflineStoreConfig")

        # Load catalog
        catalog_props = {
            "type": config.offline_store.catalog_type,
            "uri": config.offline_store.uri,
            "warehouse": config.offline_store.warehouse,
            **config.offline_store.storage_options,
        }
        catalog_props = {k: v for k, v in catalog_props.items() if v is not None}

        catalog = load_catalog(config.offline_store.catalog_name, **catalog_props)
        table = catalog.load_table(self.table_identifier)

        # Extract schema from Iceberg table
        schema = table.schema()
        for field in schema.fields:
            # Convert Iceberg type to string representation
            iceberg_type_str = str(field.field_type).lower()
            yield (field.name, iceberg_type_str)

    def source_datatype_to_feast_value_type(self) -> Callable[[str], ValueType]:
        """
        Return the callable that maps Iceberg data types to Feast value types.
        """
        return iceberg_to_feast_value_type


class IcebergOptions:
    def __init__(self, table_identifier: Optional[str]):
        self._table_identifier = table_identifier

    @property
    def table_identifier(self):
        return self._table_identifier

    @staticmethod
    def from_proto(config_bytes: bytes):
        """Deserialize from protobuf bytes."""
        config = json.loads(config_bytes.decode('utf-8'))
        return IcebergOptions(table_identifier=config.get('table_identifier'))

    def to_proto(self) -> bytes:
        """Serialize to protobuf bytes."""
        config = {'table_identifier': self._table_identifier}
        return json.dumps(config).encode('utf-8')
