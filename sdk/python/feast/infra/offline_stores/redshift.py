import uuid
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa
from pydantic import StrictStr
from pydantic.typing import Literal

from feast import type_map
from feast.data_source import DataSource
from feast.errors import DataSourceNotFoundException, RedshiftCredentialsError
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.utils import aws_utils
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.value_type import ValueType


class RedshiftOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for AWS Redshift """

    type: Literal["redshift"] = "redshift"
    """ Offline store type selector"""

    cluster_id: StrictStr
    """ Redshift cluster identifier """

    region: StrictStr
    """ Redshift cluster's AWS region """

    user: StrictStr
    """ Redshift user name """

    database: StrictStr
    """ Redshift database name """

    s3_staging_location: StrictStr
    """ S3 path for importing & exporting data to Redshift """

    iam_role: StrictStr
    """ IAM Role for Redshift, granting it access to S3 """


class RedshiftOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(data_source, RedshiftSource)
        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)

        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamp_columns = [event_timestamp_column]
        if created_timestamp_column:
            timestamp_columns.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamp_columns) + " DESC"
        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamp_columns
        )

        redshift_client = aws_utils.get_redshift_data_client(
            config.offline_store.region
        )
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)

        query = f"""
            SELECT {field_string}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression}
                WHERE {event_timestamp_column} BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
            )
            WHERE _feast_row = 1
            """
        return RedshiftRetrievalJob(
            query=query,
            redshift_client=redshift_client,
            s3_resource=s3_resource,
            config=config,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        pass


class RedshiftRetrievalJob(RetrievalJob):
    def __init__(self, query: str, redshift_client, s3_resource, config: RepoConfig):
        """Initialize RedshiftRetrievalJob object.

        Args:
            query: Redshift SQL query to execute.
            redshift_client: boto3 redshift-data client
            s3_resource: boto3 s3 resource object
            config: Feast repo config
        """
        self.query = query
        self._redshift_client = redshift_client
        self._s3_resource = s3_resource
        self._config = config
        self._s3_path = (
            self._config.offline_store.s3_staging_location
            + "/unload/"
            + str(uuid.uuid4())
        )

    def to_df(self) -> pd.DataFrame:
        return aws_utils.unload_redshift_query_to_df(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            self._s3_resource,
            self._s3_path,
            self._config.offline_store.iam_role,
            self.query,
        )

    def to_arrow(self) -> pa.Table:
        return aws_utils.unload_redshift_query_to_pa(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            self._s3_resource,
            self._s3_path,
            self._config.offline_store.iam_role,
            self.query,
        )

    def to_s3(self) -> str:
        """ Export dataset to S3 in Parquet format and return path """
        aws_utils.execute_redshift_query_and_unload_to_s3(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            self._s3_path,
            self._config.offline_store.iam_role,
            self.query,
        )
        return self._s3_path

    def to_redshift(self, table_name: str) -> None:
        """ Save dataset as a new Redshift table """
        aws_utils.execute_redshift_statement(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            f'CREATE TABLE "{table_name}" AS ({self.query})',
        )


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
