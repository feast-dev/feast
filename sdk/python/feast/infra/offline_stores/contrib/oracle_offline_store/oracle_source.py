import json
from typing import Callable, Dict, Iterable, Optional, Tuple

from feast import type_map
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType


class OracleOptions:
    """
    DataSource Oracle options used to source features from an Oracle database.
    """

    def __init__(
        self,
        table_ref: Optional[str],
    ):
        self._table_ref = table_ref

    @property
    def table_ref(self):
        """Returns the table ref of this Oracle source"""
        return self._table_ref

    @table_ref.setter
    def table_ref(self, table_ref):
        """Sets the table ref of this Oracle source"""
        self._table_ref = table_ref

    @classmethod
    def from_proto(
        cls, oracle_options_proto: DataSourceProto.CustomSourceOptions
    ) -> "OracleOptions":
        """
        Creates an OracleOptions from a protobuf representation.

        Args:
            oracle_options_proto: A protobuf representation of a DataSource

        Returns:
            An OracleOptions object based on the protobuf
        """
        options = json.loads(oracle_options_proto.configuration)
        return cls(table_ref=options.get("table_ref"))

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        """
        Converts an OracleOptions object to its protobuf representation.

        Returns:
            CustomSourceOptions protobuf
        """
        return DataSourceProto.CustomSourceOptions(
            configuration=json.dumps({"table_ref": self._table_ref}).encode("utf-8")
        )


class OracleSource(DataSource):
    """An OracleSource defines a data source backed by an Oracle database table."""

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    def __init__(
        self,
        name: str,
        table_ref: Optional[str] = None,
        event_timestamp_column: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = None,
    ):
        """Creates an OracleSource object.

        Args:
            name: Name of the source, which should be unique within a project.
            table_ref: The table reference (e.g., "TRANSACTION_FEATURES" or "SCHEMA.TABLE").
            event_timestamp_column: Event timestamp column for point-in-time joins.
            created_timestamp_column: Timestamp column indicating when the row was created
                (used for deduplicating rows).
            field_mapping: A dictionary mapping column names in this data source to feature
                names in a feature table or view.
            date_partition_column: The date partition column.
            description: A human-readable description.
            tags: A dictionary of key-value pairs to store arbitrary metadata.
            owner: The owner of the data source, typically the email of the primary maintainer.
        """
        self._oracle_options = OracleOptions(table_ref=table_ref)

        super().__init__(
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
            description=description,
            tags=tags,
            owner=owner,
            name=name,
            timestamp_field=event_timestamp_column,
        )

    def __eq__(self, other):
        if not isinstance(other, OracleSource):
            raise TypeError(
                "Comparisons should only involve OracleSource class objects."
            )

        return (
            self.name == other.name
            and self._oracle_options.table_ref == other._oracle_options.table_ref
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    def __hash__(self):
        return hash(
            (
                self.name,
                self._oracle_options.table_ref,
                self.timestamp_field,
                self.created_timestamp_column,
            )
        )

    @property
    def table_ref(self):
        return self._oracle_options.table_ref

    @property
    def oracle_options(self):
        """Returns the Oracle options of this data source"""
        return self._oracle_options

    @oracle_options.setter
    def oracle_options(self, oracle_options):
        """Sets the Oracle options of this data source"""
        self._oracle_options = oracle_options

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        options = json.loads(data_source.custom_options.configuration)
        return OracleSource(
            name=data_source.name,
            field_mapping=dict(data_source.field_mapping),
            table_ref=options.get("table_ref"),
            event_timestamp_column=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.oracle_offline_store.oracle_source.OracleSource",
            field_mapping=self.field_mapping,
            custom_options=self._oracle_options.to_proto(),
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column
        data_source_proto.name = self.name
        return data_source_proto

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        return f"{self.table_ref}"

    def validate(self, config: RepoConfig):
        """Validates the Oracle data source by checking the table exists."""
        self.get_table_column_names_and_types(config)

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.oracle_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from feast.infra.offline_stores.contrib.oracle_offline_store.oracle import (
            get_ibis_connection,
        )

        con = get_ibis_connection(config)

        schema = con.get_schema(self.table_ref)

        return [(col_name, str(col_type)) for col_name, col_type in schema.items()]
