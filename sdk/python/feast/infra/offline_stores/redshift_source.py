from typing import Callable, Dict, Iterable, Optional, Tuple

from feast import type_map
from feast.data_source import DataSource
from feast.errors import DataSourceNotFoundException, RedshiftCredentialsError
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType


class RedshiftSource(DataSource):
    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        table: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        query: Optional[str] = None,
    ):
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

        self._redshift_options = RedshiftOptions(table=table, query=query)

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        return RedshiftSource(
            field_mapping=dict(data_source.field_mapping),
            table=data_source.redshift_options.table,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            query=data_source.redshift_options.query,
        )

    def __eq__(self, other):
        if not isinstance(other, RedshiftSource):
            raise TypeError(
                "Comparisons should only involve RedshiftSource class objects."
            )

        return (
            self.redshift_options.table == other.redshift_options.table
            and self.redshift_options.query == other.redshift_options.query
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def table(self):
        return self._redshift_options.table

    @property
    def query(self):
        return self._redshift_options.query

    @property
    def redshift_options(self):
        """
        Returns the Redshift options of this data source
        """
        return self._redshift_options

    @redshift_options.setter
    def redshift_options(self, _redshift_options):
        """
        Sets the Redshift options of this data source
        """
        self._redshift_options = _redshift_options

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_REDSHIFT,
            field_mapping=self.field_mapping,
            redshift_options=self.redshift_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        # As long as the query gets successfully executed, or the table exists,
        # the data source is validated. We don't need the results though.
        # TODO: uncomment this
        # self.get_table_column_names_and_types(config)
        print("Validate", self.get_table_column_names_and_types(config))

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table:
            return f'"{self.table}"'
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.redshift_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from botocore.exceptions import ClientError

        from feast.infra.offline_stores.redshift import RedshiftOfflineStoreConfig
        from feast.infra.utils import aws_utils

        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)

        client = aws_utils.get_redshift_data_client(config.offline_store.region)

        if self.table is not None:
            try:
                table = client.describe_table(
                    ClusterIdentifier=config.offline_store.cluster_id,
                    Database=config.offline_store.database,
                    DbUser=config.offline_store.user,
                    Table=self.table,
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "ValidationException":
                    raise RedshiftCredentialsError() from e
                raise

            # The API returns valid JSON with empty column list when the table doesn't exist
            if len(table["ColumnList"]) == 0:
                raise DataSourceNotFoundException(self.table)

            columns = table["ColumnList"]
        else:
            statement_id = aws_utils.execute_redshift_statement(
                client,
                config.offline_store.cluster_id,
                config.offline_store.database,
                config.offline_store.user,
                f"SELECT * FROM ({self.query}) LIMIT 1",
            )
            columns = aws_utils.get_redshift_statement_result(client, statement_id)[
                "ColumnMetadata"
            ]

        return [(column["name"], column["typeName"].upper()) for column in columns]


class RedshiftOptions:
    """
    DataSource Redshift options used to source features from Redshift query
    """

    def __init__(self, table: Optional[str], query: Optional[str]):
        self._table = table
        self._query = query

    @property
    def query(self):
        """
        Returns the Redshift SQL query referenced by this source
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Sets the Redshift SQL query referenced by this source
        """
        self._query = query

    @property
    def table(self):
        """
        Returns the table name of this Redshift table
        """
        return self._table

    @table.setter
    def table(self, table_name):
        """
        Sets the table ref of this Redshift table
        """
        self._table = table_name

    @classmethod
    def from_proto(cls, redshift_options_proto: DataSourceProto.RedshiftOptions):
        """
        Creates a RedshiftOptions from a protobuf representation of a Redshift option

        Args:
            redshift_options_proto: A protobuf representation of a DataSource

        Returns:
            Returns a RedshiftOptions object based on the redshift_options protobuf
        """

        redshift_options = cls(
            table=redshift_options_proto.table, query=redshift_options_proto.query,
        )

        return redshift_options

    def to_proto(self) -> DataSourceProto.RedshiftOptions:
        """
        Converts an RedshiftOptionsProto object to its protobuf representation.

        Returns:
            RedshiftOptionsProto protobuf
        """

        redshift_options_proto = DataSourceProto.RedshiftOptions(
            table=self.table, query=self.query,
        )

        return redshift_options_proto
