import json
import logging
from typing import Callable, Dict, Iterable, Optional, Tuple

from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType

logger = logging.getLogger(__name__)


class IcebergRestCatalogSource(DataSource):
    """A DataSource backed by a table in any Iceberg REST Catalog.

    Reading is engine‑agnostic — the catalog metadata (schema, file
    listing, credentials) is resolved via ``pyiceberg``, and each
    offline store engine (Spark, DuckDB, Trino, …) reads the actual
    data files using its native Iceberg support.
    """

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

        catalog = load_catalog_from_source(self)
        table_ref = catalog.load_table((self.namespace, self._table))
        schema = table_ref.schema()
        return ((field.name, str(field.field_type)) for field in schema.fields)

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
            date_partition_column_format=config.get(
                "date_partition_column_format", "%Y-%m-%d"
            ),
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
            "date_partition_column_format": self._date_partition_column_format,
        }

        proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.iceberg.iceberg_source.IcebergRestCatalogSource",
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
            and self._date_partition_column_format
            == other._date_partition_column_format
        )

    def __hash__(self):
        return super().__hash__()


class UnityCatalogSource(IcebergRestCatalogSource):
    """An Iceberg REST Catalog source specialised for Databricks Unity Catalog.

    Adds UC‑specific governance fields on top of the base
    ``IcebergRestCatalogSource``:

    * ``register_as_feature_table`` — whether to register this
      FeatureView as a UC feature table during ``feast apply``.
    * ``sync_lineage`` — whether to emit OpenLineage events with UC
      fully‑qualified table references during materialization.
    """

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
            date_partition_column_format=config.get(
                "date_partition_column_format", "%Y-%m-%d"
            ),
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
            "date_partition_column_format": self._date_partition_column_format,
        }

        proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.iceberg.iceberg_source.UnityCatalogSource",
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
            self.cluster_id == other.cluster_id
            and self.register_as_feature_table == other.register_as_feature_table
            and self.sync_lineage == other.sync_lineage
        )

    def __hash__(self):
        return super().__hash__()


def load_catalog_from_source(
    source: IcebergRestCatalogSource,
):
    """Build a ``pyiceberg.catalog.Catalog`` from an Iceberg source.

    This helper resolves the token from the environment, constructs
    the catalog properties, and delegates to ``pyiceberg.catalog.load_catalog()``.
    """
    import os

    from pyiceberg.catalog import load_catalog

    token = os.environ.get(source.token_env_var, "")

    catalog_type = "rest"
    if isinstance(source, UnityCatalogSource):
        catalog_type = "databricks"

    props: Dict[str, str] = {
        "uri": source.endpoint,
        "warehouse": source.warehouse,
        "token": token,
    }
    if source.credential_vending:
        props["credential-vending"] = "true"

    return load_catalog(
        name=source.warehouse,
        type=catalog_type,
        **props,
    )
