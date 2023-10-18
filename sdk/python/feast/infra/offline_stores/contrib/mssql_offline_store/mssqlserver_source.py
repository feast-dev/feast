# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
import json
import warnings
from typing import Callable, Dict, Iterable, Optional, Tuple

import pandas
from sqlalchemy import create_engine

from feast import type_map
from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.mssql_offline_store.mssql import (
    MsSqlServerOfflineStoreConfig,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType

# Make sure azure warning doesn't raise more than once.
warnings.simplefilter("once", RuntimeWarning)


class MsSqlServerOptions:
    """
    DataSource MsSQLServer options used to source features from MsSQLServer query
    """

    def __init__(
        self,
        connection_str: Optional[str],
        table_ref: Optional[str],
    ):
        self._connection_str = connection_str
        self._table_ref = table_ref

    @property
    def table_ref(self):
        """
        Returns the table ref of this SQL Server source
        """
        return self._table_ref

    @table_ref.setter
    def table_ref(self, table_ref):
        """
        Sets the table ref of this SQL Server source
        """
        self._table_ref = table_ref

    @property
    def connection_str(self):
        """
        Returns the SqlServer SQL connection string referenced by this source
        """
        return self._connection_str

    @connection_str.setter
    def connection_str(self, connection_str):
        """
        Sets the SqlServer SQL connection string referenced by this source
        """
        self._connection_str = connection_str

    @classmethod
    def from_proto(
        cls, sqlserver_options_proto: DataSourceProto.CustomSourceOptions
    ) -> "MsSqlServerOptions":
        """
        Creates an MsSQLServerOptions from a protobuf representation of a SqlServer option
        Args:
            sqlserver_options_proto: A protobuf representation of a DataSource
        Returns:
            Returns a SQLServerOptions object based on the sqlserver_options protobuf
        """
        options = json.loads(sqlserver_options_proto.configuration)

        sqlserver_options = cls(
            table_ref=options["table_ref"],
            connection_str=options["connection_str"],
        )

        return sqlserver_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        """
        Converts a MsSQLServerOptions object to a protobuf representation.
        Returns:
            CustomSourceOptions protobuf
        """

        sqlserver_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {
                    "table_ref": self._table_ref,
                    "connection_string": self._connection_str,
                }
            ).encode("utf-8")
        )

        return sqlserver_options_proto


class MsSqlServerSource(DataSource):
    def __init__(
        self,
        name: str,
        table_ref: Optional[str] = None,
        event_timestamp_column: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        connection_str: Optional[str] = "",
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = None,
    ):
        warnings.warn(
            "The Azure Synapse + Azure SQL data source is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        self._mssqlserver_options = MsSqlServerOptions(
            connection_str=connection_str, table_ref=table_ref
        )
        self._connection_str = connection_str

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
        if not isinstance(other, MsSqlServerSource):
            raise TypeError(
                "Comparisons should only involve SqlServerSource class objects."
            )

        return (
            self.name == other.name
            and self.mssqlserver_options.connection_str
            == other.mssqlserver_options.connection_str
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    def __hash__(self):
        return hash(
            (
                self.name,
                self.mssqlserver_options.connection_str,
                self.timestamp_field,
                self.created_timestamp_column,
            )
        )

    @property
    def table_ref(self):
        return self._mssqlserver_options.table_ref

    @property
    def mssqlserver_options(self):
        """
        Returns the SQL Server options of this data source
        """
        return self._mssqlserver_options

    @mssqlserver_options.setter
    def mssqlserver_options(self, sqlserver_options):
        """
        Sets the SQL Server options of this data source
        """
        self._mssqlserver_options = sqlserver_options

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        options = json.loads(data_source.custom_options.configuration)
        return MsSqlServerSource(
            name=data_source.name,
            field_mapping=dict(data_source.field_mapping),
            table_ref=options["table_ref"],
            connection_str=options["connection_string"],
            event_timestamp_column=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.mssql_offline_store.mssqlserver_source.MsSqlServerSource",
            field_mapping=self.field_mapping,
            custom_options=self.mssqlserver_options.to_proto(),
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column
        data_source_proto.name = self.name
        return data_source_proto

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        return f"`{self.table_ref}`"

    def validate(self, config: RepoConfig):
        # As long as the query gets successfully executed, or the table exists,
        # the data source is validated. We don't need the results though.
        self.get_table_column_names_and_types(config)
        return None

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.mssql_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        assert isinstance(config.offline_store, MsSqlServerOfflineStoreConfig)
        conn = create_engine(config.offline_store.connection_string)
        self._mssqlserver_options.connection_str = (
            config.offline_store.connection_string
        )
        name_type_pairs = []
        if len(self.table_ref.split(".")) == 2:
            database, table_name = self.table_ref.split(".")
            columns_query = f"""
                SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}' and table_schema = '{database}'
            """
        else:
            columns_query = f"""
                SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{self.table_ref}'
            """

        table_schema = pandas.read_sql(columns_query, conn)
        name_type_pairs.extend(
            list(
                zip(
                    table_schema["COLUMN_NAME"].to_list(),
                    table_schema["DATA_TYPE"].to_list(),
                )
            )
        )
        return name_type_pairs
