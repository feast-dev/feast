import json
import logging
from typing import Callable, Dict, Iterable, Optional, Tuple

from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType

logger = logging.getLogger(__name__)


class IcebergRestCatalogSource(DataSource):
    def __init__(
        self,
        *,
        name: str,
        endpoint: str,
        warehouse: str,
        namespace: str,
        table: str,
        timestamp_field: Optional[str] = None,
        token_env_var: str = "ICEBERG_REST_TOKEN",
        credential_vending: bool = True,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
        date_partition_column: Optional[str] = None,
        date_partition_column_format: str = "%Y-%m-%d",
    ):
        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            date_partition_column=date_partition_column,
            tags=tags,
            owner=owner,
        )
        self.endpoint = endpoint
        self.warehouse = warehouse
        self.namespace = namespace
        self._table = table
        self.token_env_var = token_env_var
        self.credential_vending = credential_vending
        self._date_partition_column_format = date_partition_column_format

    @property
    def date_partition_column_format(self):
        return self._date_partition_column_format

    @property
    def path(self):
        return None

    @property
    def file_format(self):
        return None

    @property
    def query(self):
        return None

    @property
    def full_table_name(self) -> str:
        return f"`{self.warehouse}`.`{self.namespace}`.`{self._table}`"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        from feast.type_map import spark_to_feast_value_type

        return spark_to_feast_value_type

    def get_table_query_string(self) -> str:
        return self.full_table_name

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
            get_spark_session_or_start_new_with_repoconfig,
        )

        spark_session = get_spark_session_or_start_new_with_repoconfig(
            store_config=config.offline_store
        )
        df = spark_session.sql(f"SELECT * FROM {self.get_table_query_string()}")
        return ((field.name, field.dataType.simpleString()) for field in df.schema)

    def validate(self, config: RepoConfig):
        self.get_table_column_names_and_types(config)

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> "IcebergRestCatalogSource":
        assert data_source.HasField("custom_options")
        config = json.loads(data_source.custom_options.configuration.decode("utf-8"))

        return IcebergRestCatalogSource(
            name=data_source.name,
            endpoint=config["endpoint"],
            warehouse=config["warehouse"],
            namespace=config["namespace"],
            table=config["table"],
            timestamp_field=data_source.timestamp_field or None,
            token_env_var=config.get("token_env_var", "ICEBERG_REST_TOKEN"),
            credential_vending=config.get("credential_vending", True),
            created_timestamp_column=data_source.created_timestamp_column or None,
            field_mapping=dict(data_source.field_mapping) or None,
            description=data_source.description or "",
            tags=dict(data_source.tags) or None,
            owner=data_source.owner or "",
            date_partition_column=data_source.date_partition_column or None,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto

        config = {
            "endpoint": self.endpoint,
            "warehouse": self.warehouse,
            "namespace": self.namespace,
            "table": self._table,
            "token_env_var": self.token_env_var,
            "credential_vending": self.credential_vending,
        }

        proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.spark_offline_store.iceberg_source.IcebergRestCatalogSource",
            field_mapping=self.field_mapping,
            date_partition_column=self.date_partition_column or "",
            description=self.description or "",
            tags=self.tags,
            owner=self.owner or "",
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=json.dumps(config).encode("utf-8"),
            ),
        )

        if self.timestamp_field:
            proto.timestamp_field = self.timestamp_field
        if self.created_timestamp_column:
            proto.created_timestamp_column = self.created_timestamp_column

        return proto

    def __eq__(self, other):
        base_eq = super().__eq__(other)
        if not base_eq:
            return False
        if not isinstance(other, IcebergRestCatalogSource):
            return False
        return (
            self.endpoint == other.endpoint
            and self.warehouse == other.warehouse
            and self.namespace == other.namespace
            and self._table == other._table
            and self.token_env_var == other.token_env_var
            and self.credential_vending == other.credential_vending
        )

    def __hash__(self):
        return super().__hash__()


class UnityCatalogSource(IcebergRestCatalogSource):
    def __init__(
        self,
        *,
        name: str,
        endpoint: str,
        warehouse: str,
        namespace: str,
        table: str,
        timestamp_field: Optional[str] = None,
        token_env_var: str = "DATABRICKS_TOKEN",
        credential_vending: bool = True,
        cluster_id: Optional[str] = None,
        register_as_feature_table: bool = True,
        sync_lineage: bool = True,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
        date_partition_column: Optional[str] = None,
        date_partition_column_format: str = "%Y-%m-%d",
    ):
        super().__init__(
            name=name,
            endpoint=endpoint,
            warehouse=warehouse,
            namespace=namespace,
            table=table,
            timestamp_field=timestamp_field,
            token_env_var=token_env_var,
            credential_vending=credential_vending,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
            date_partition_column=date_partition_column,
            date_partition_column_format=date_partition_column_format,
        )
        self.cluster_id = cluster_id
        self.register_as_feature_table = register_as_feature_table
        self.sync_lineage = sync_lineage

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> "UnityCatalogSource":
        assert data_source.HasField("custom_options")
        config = json.loads(data_source.custom_options.configuration.decode("utf-8"))

        return UnityCatalogSource(
            name=data_source.name,
            endpoint=config["endpoint"],
            warehouse=config["warehouse"],
            namespace=config["namespace"],
            table=config["table"],
            timestamp_field=data_source.timestamp_field or None,
            token_env_var=config.get("token_env_var", "DATABRICKS_TOKEN"),
            credential_vending=config.get("credential_vending", True),
            cluster_id=config.get("cluster_id"),
            register_as_feature_table=config.get("register_as_feature_table", True),
            sync_lineage=config.get("sync_lineage", True),
            created_timestamp_column=data_source.created_timestamp_column or None,
            field_mapping=dict(data_source.field_mapping) or None,
            description=data_source.description or "",
            tags=dict(data_source.tags) or None,
            owner=data_source.owner or "",
            date_partition_column=data_source.date_partition_column or None,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto

        config = {
            "endpoint": self.endpoint,
            "warehouse": self.warehouse,
            "namespace": self.namespace,
            "table": self._table,
            "token_env_var": self.token_env_var,
            "credential_vending": self.credential_vending,
            "cluster_id": self.cluster_id,
            "register_as_feature_table": self.register_as_feature_table,
            "sync_lineage": self.sync_lineage,
        }

        proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.spark_offline_store.iceberg_source.UnityCatalogSource",
            field_mapping=self.field_mapping,
            date_partition_column=self.date_partition_column or "",
            description=self.description or "",
            tags=self.tags,
            owner=self.owner or "",
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=json.dumps(config).encode("utf-8"),
            ),
        )

        if self.timestamp_field:
            proto.timestamp_field = self.timestamp_field
        if self.created_timestamp_column:
            proto.created_timestamp_column = self.created_timestamp_column

        return proto

    def __eq__(self, other):
        base_eq = super().__eq__(other)
        if not base_eq:
            return False
        if not isinstance(other, UnityCatalogSource):
            return False
        return (
            self.register_as_feature_table == other.register_as_feature_table
            and self.sync_lineage == other.sync_lineage
        )

    def __hash__(self):
        return super().__hash__()
