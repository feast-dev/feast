import logging
import traceback
import warnings
from enum import Enum
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from pyspark.sql import SparkSession

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.infra.offline_stores.offline_utils import get_temp_entity_table_name
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import spark_to_feast_value_type
from feast.value_type import ValueType

logger = logging.getLogger(__name__)


class SparkSourceFormat(Enum):
    csv = "csv"
    json = "json"
    parquet = "parquet"
    delta = "delta"
    avro = "avro"


class SparkSource(DataSource):
    def __init__(
        self,
        *,
        name: Optional[str] = None,
        table: Optional[str] = None,
        query: Optional[str] = None,
        path: Optional[str] = None,
        file_format: Optional[str] = None,
        event_timestamp_column: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        timestamp_field: Optional[str] = None,
    ):
        # If no name, use the table_ref as the default name
        _name = name
        if not _name:
            if table:
                _name = table
            else:
                raise DataSourceNoNameException()

        if date_partition_column:
            warnings.warn(
                (
                    "The argument 'date_partition_column' is not supported for Spark sources."
                    "It will be removed in Feast 0.24+"
                ),
                DeprecationWarning,
            )

        super().__init__(
            name=_name,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
            timestamp_field=timestamp_field,
        )
        warnings.warn(
            "The spark data source API is an experimental feature in alpha development. "
            "This API is unstable and it could and most probably will be changed in the future.",
            RuntimeWarning,
        )
        self.spark_options = SparkOptions(
            table=table,
            query=query,
            path=path,
            file_format=file_format,
        )

    @property
    def table(self):
        """
        Returns the table of this feature data source
        """
        return self.spark_options.table

    @property
    def query(self):
        """
        Returns the query of this feature data source
        """
        return self.spark_options.query

    @property
    def path(self):
        """
        Returns the path of the spark data source file.
        """
        return self.spark_options.path

    @property
    def file_format(self):
        """
        Returns the file format of this feature data source.
        """
        return self.spark_options.file_format

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        assert data_source.HasField("spark_options")
        spark_options = SparkOptions.from_proto(data_source.spark_options)

        return SparkSource(
            name=data_source.name,
            field_mapping=dict(data_source.field_mapping),
            table=spark_options.table,
            query=spark_options.query,
            path=spark_options.path,
            file_format=spark_options.file_format,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_SPARK,
            data_source_class_type="feast.infra.offline_stores.contrib.spark_offline_store.spark_source.SparkSource",
            field_mapping=self.field_mapping,
            spark_options=self.spark_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        self.get_table_column_names_and_types(config)

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return spark_to_feast_value_type

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
        return (
            (fields["name"], fields["type"])
            for fields in df.schema.jsonValue()["fields"]
        )

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table:
            # Backticks make sure that spark sql knows this a table reference.
            table = ".".join([f"`{x}`" for x in self.table.split(".")])
            return table
        if self.query:
            return f"({self.query})"

        # If both the table query string and the actual query are null, we can load from file.
        spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            raise AssertionError("Could not find an active spark session.")
        try:
            df = spark_session.read.format(self.file_format).load(self.path)
        except Exception:
            logger.exception(
                "Spark read of file source failed.\n" + traceback.format_exc()
            )
        tmp_table_name = get_temp_entity_table_name()
        df.createOrReplaceTempView(tmp_table_name)

        return f"`{tmp_table_name}`"


class SparkOptions:
    allowed_formats = [format.value for format in SparkSourceFormat]

    def __init__(
        self,
        table: Optional[str],
        query: Optional[str],
        path: Optional[str],
        file_format: Optional[str],
    ):
        # Check that only one of the ways to load a spark dataframe can be used. We have
        # to treat empty string and null the same due to proto (de)serialization.
        if sum([(not (not arg)) for arg in [table, query, path]]) != 1:
            raise ValueError(
                "Exactly one of params(table, query, path) must be specified."
            )
        if path:
            if not file_format:
                raise ValueError(
                    "If 'path' is specified, then 'file_format' is required."
                )
            if file_format not in self.allowed_formats:
                raise ValueError(
                    f"'file_format' should be one of {self.allowed_formats}"
                )

        self._table = table
        self._query = query
        self._path = path
        self._file_format = file_format

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, table):
        self._table = table

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, query):
        self._query = query

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path):
        self._path = path

    @property
    def file_format(self):
        return self._file_format

    @file_format.setter
    def file_format(self, file_format):
        self._file_format = file_format

    @classmethod
    def from_proto(cls, spark_options_proto: DataSourceProto.SparkOptions):
        """
        Creates a SparkOptions from a protobuf representation of a spark option
        args:
            spark_options_proto: a protobuf representation of a datasource
        Returns:
            Returns a SparkOptions object based on the spark_options protobuf
        """
        spark_options = cls(
            table=spark_options_proto.table,
            query=spark_options_proto.query,
            path=spark_options_proto.path,
            file_format=spark_options_proto.file_format,
        )

        return spark_options

    def to_proto(self) -> DataSourceProto.SparkOptions:
        """
        Converts an SparkOptionsProto object to its protobuf representation.
        Returns:
            SparkOptionsProto protobuf
        """
        spark_options_proto = DataSourceProto.SparkOptions(
            table=self.table,
            query=self.query,
            path=self.path,
            file_format=self.file_format,
        )

        return spark_options_proto


class SavedDatasetSparkStorage(SavedDatasetStorage):
    _proto_attr_name = "spark_storage"

    spark_options: SparkOptions

    def __init__(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        path: Optional[str] = None,
        file_format: Optional[str] = None,
    ):
        self.spark_options = SparkOptions(
            table=table,
            query=query,
            path=path,
            file_format=file_format,
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        spark_options = SparkOptions.from_proto(storage_proto.spark_storage)
        return SavedDatasetSparkStorage(
            table=spark_options.table,
            query=spark_options.query,
            path=spark_options.path,
            file_format=spark_options.file_format,
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(spark_storage=self.spark_options.to_proto())

    def to_data_source(self) -> DataSource:
        return SparkSource(
            table=self.spark_options.table,
            query=self.spark_options.query,
            path=self.spark_options.path,
            file_format=self.spark_options.file_format,
        )
