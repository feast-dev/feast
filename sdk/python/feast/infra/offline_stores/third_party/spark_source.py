import pickle
import warnings
from enum import Enum
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from feast.data_source import DataSource
from feast.errors import DataSourceNotFoundException
from feast.infra.offline_stores.offline_utils import get_temp_entity_table_name
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import spark_to_feast_value_type
from feast.value_type import ValueType


class SparkSourceFormat(Enum):
    csv = "csv"
    json = "json"
    parquet = "parquet"


class SparkSource(DataSource):
    def __init__(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        path: Optional[str] = None,
        file_format: Optional[str] = None,
        event_timestamp_column: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = None,
    ):
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )
        warnings.warn(
            "The spark data source API is an experimental feature in alpha development. "
            "This API is unstable and it could and most probably will be changed in the future.",
            RuntimeWarning,
        )
        self.allowed_formats = [format.value for format in SparkSourceFormat]

        # Check that only one of the ways to load a spark dataframe can be used.
        if sum([(arg is not None) for arg in [table, query, path]]) != 1:
            raise ValueError(
                "Exactly one of params(table, query, path) must be specified."
            )

        if path is not None:
            if file_format is None:
                raise ValueError(
                    "If 'path' is specified, then 'file_format' is required."
                )
            if file_format not in self.allowed_formats:
                raise ValueError(
                    f"'file_format' should be one of {self.allowed_formats}"
                )

        self._spark_options = SparkOptions(
            table=table, query=query, path=path, file_format=file_format,
        )

    @property
    def spark_options(self):
        """
        Returns the spark options of this data source
        """
        return self._spark_options

    @spark_options.setter
    def spark_options(self, spark_options):
        """
        Sets the spark options of this data source
        """
        self._spark_options = spark_options

    @property
    def table(self):
        """
        Returns the table of this feature data source
        """
        return self._spark_options.table

    @property
    def query(self):
        """
        Returns the query of this feature data source
        """
        return self._spark_options.query

    @property
    def path(self):
        """
        Returns the path of the spark data source file.
        """
        return self._spark_options.path

    @property
    def file_format(self):
        """
        Returns the file format of this feature data source.
        """
        return self._spark_options.file_format

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:

        assert data_source.HasField("custom_options")

        spark_options = SparkOptions.from_proto(data_source.custom_options)
        return SparkSource(
            field_mapping=dict(data_source.field_mapping),
            table=spark_options.table,
            query=spark_options.query,
            path=spark_options.path,
            file_format=spark_options.file_format,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            field_mapping=self.field_mapping,
            custom_options=self.spark_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        self.get_table_column_names_and_types(config)

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return spark_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from feast.infra.offline_stores.third_party.spark import (
            get_spark_session_or_start_new_with_repoconfig,
        )

        spark_session = get_spark_session_or_start_new_with_repoconfig(
            store_config=config.offline_store
        )
        df = spark_session.sql(f"SELECT * FROM {self.get_table_query_string()}")
        try:
            return (
                (fields["name"], fields["type"])
                for fields in df.schema.jsonValue()["fields"]
            )
        except AnalysisException:
            raise DataSourceNotFoundException()  # TODO: review error handling


    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table:
            return f"`{self.table}`"
        if self.query:
            return f"({self.query})"

        # If both the table query string and the actual query are null, we can load from file.
        spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            raise AssertionError("Could not find an active spark session.")
        df = spark_session.read.format(self.file_format).load(self.path)

        tmp_table_name = get_temp_entity_table_name()
        df.createOrReplaceTempView(tmp_table_name)

        return f"`{tmp_table_name}`"


class SparkOptions:
    def __init__(
        self, table: Optional[str] = None, query: Optional[str] = None, path: Optional[str] = None, file_format: Optional[str] = None
    ):
        self._table = table
        self._query = query
        self._path = path
        self._file_format = file_format


    @property
    def table(self):
        """
        Returns the table
        """
        return self._table

    @table.setter
    def table(self, table):
        """
        Sets the table
        """
        self._table = table

    @property
    def query(self):
        """
        Returns the query
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Sets the query
        """
        self._query = query

    @property
    def path(self):
        """
        Returns the path
        """
        return self._path

    @path.setter
    def path(self, path):
        """
        Sets the path
        """
        self._path = path

    @property
    def file_format(self):
        """
        Returns the file_format
        """
        return self._file_format

    @file_format.setter
    def file_format(self, file_format):
        """
        Sets the file_format
        """
        self._file_format = file_format


    @classmethod
    def from_proto(cls, spark_options_proto: DataSourceProto.CustomSourceOptions):
        """
        Creates a SparkOptions from a protobuf representation of a spark option
        args:
            spark_options_proto: a protobuf representation of a datasource
        Returns:
            Returns a SparkOptions object based on the spark_options protobuf
        """
        spark_configuration = pickle.loads(spark_options_proto.configuration)

        spark_options = cls(
            table=spark_configuration.table,
            query=spark_configuration.query,
            path=spark_configuration.path,
            file_format=spark_configuration.file_format,
        )
        return spark_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        """
        Converts an SparkOptionsProto object to its protobuf representation.
        Returns:
            SparkOptionsProto protobuf
        """

        spark_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=pickle.dumps(self),
        )

        return spark_options_proto


class SavedDatasetSparkStorage(SavedDatasetStorage):
    _proto_attr_name = "spark_storage"

    spark_options: SparkOptions

    def __init__(self, table_ref: Optional[str] = None, query: Optional[str] = None):
        self.spark_options = SparkOptions(table=table_ref, query=query)

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        # options = SparkOptions.from_proto(
        #         storage_proto
        # )
        # spark_options = SparkOptions(table=options.table, query=options.query)
        return SavedDatasetSparkStorage(table_ref="", query=None)

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto()

    def to_data_source(self) -> DataSource:
        return SparkSource(table=self.spark_options.table)
