from typing import Callable, Dict, Iterable, Optional, Tuple

import pandas

from sqlalchemy import create_engine

from feast import type_map
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.value_type import ValueType



class MsSqlServerOptions:
    """
    DataSource MsSqlServer options used to source features from MsSqlServer query
    """

    def __init__(
        self,
        connection_str: Optional[str],
        table_ref: Optional[str],
        query: Optional[str],
    ):
        self._connection_str = connection_str
        self._query = query
        self._table_ref = table_ref

    @property
    def query(self):
        """
        Returns the SQL Server SQL query referenced by this source
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Sets the SQL Server SQL query referenced by this source
        """
        self._query = query

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
    def from_proto(cls, sqlserver_options_proto: DataSourceProto.SqlServerOptions):
        """
        Creates a SqlServerOptions from a protobuf representation of a SqlServer option
        Args:
            sqlserver_options_proto: A protobuf representation of a DataSource
        Returns:
            Returns a SqlServerOptions object based on the sqlserver_options protobuf
        """

        sqlserver_options = cls(
            query=sqlserver_options_proto.query,
            table_ref=sqlserver_options_proto.table_ref,
            connection_str=sqlserver_options_proto.connection_str,
        )

        return sqlserver_options

    def to_proto(self) -> DataSourceProto.SqlServerOptions:
        """
        Converts an SqlServerOptionsProto object to its protobuf representation.
        Returns:
            SqlServerOptionsProto protobuf
        """

        sqlserver_options_proto = DataSourceProto.SqlServerOptions(
            query=self.query,
            table_ref=self.table_ref,
            connection_str=self.connection_str,
        )

        return sqlserver_options_proto


class MsSqlServerSource(DataSource):
    """
    TODO: Where does this go now?
    """
    def __init__(
        self,
        table_ref: Optional[str] = None,
        event_timestamp_column: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        query: Optional[str] = None,
        connection_str: Optional[str] = "",
    ):
        self._sqlserver_options = MsSqlServerOptions(
            cnnection_str=connection_str, query=query, table_ref=table_ref
        )
        self._connection_str = connection_str

        super().__init__(
            event_timestamp_column
            or self._infer_event_timestamp_column("TIMESTAMP|DATETIME"),
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

    def __eq__(self, other):
        if not isinstance(other, MsSqlServerSource):
            raise TypeError(
                "Comparisons should only involve SqlServerSource class objects."
            )

        return (
            self.sqlserver_options.query == other.sqlserver_options.query
            and self.sqlserver_options.connection_str
            == other.sqlserver_options.connection_str
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def table_ref(self):
        return self._sqlserver_options.table_ref

    @property
    def query(self):
        return self._sqlserver_options.query

    @property
    def sqlserver_options(self):
        """
        Returns the sqlserver options of this data source
        """
        return self._sqlserver_options

    @sqlserver_options.setter
    def sqlserver_options(self, sqlserver_options):
        """
        Sets the sqlserver options of this data source
        """
        self._sqlserver_options = sqlserver_options

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_SQLSERVER,
            field_mapping=self.field_mapping,
            sqlserver_options=self.sqlserver_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table_ref:
            return f"`{self.table_ref}`"
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.sqlserver_to_feast_value_type

    def get_table_column_names_and_types(self) -> Iterable[Tuple[str, str]]:
        conn = create_engine(self._connection_str)
        name_type_pairs = []
        database, table_name = self.table_ref.split(".")
        columns_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
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


