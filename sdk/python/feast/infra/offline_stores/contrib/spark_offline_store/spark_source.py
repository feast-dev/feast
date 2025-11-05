import logging
import traceback
import warnings
from enum import Enum
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from feast import flags_helper
from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException, DataSourceNotFoundException
from feast.infra.offline_stores.offline_utils import get_temp_entity_table_name
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.table_format import TableFormat, table_format_from_proto
from feast.type_map import spark_to_feast_value_type
from feast.value_type import ValueType

logger = logging.getLogger(__name__)


class SparkFileSourceFormat(Enum):
    csv = "csv"
    json = "json"
    parquet = "parquet"
    avro = "avro"


class SparkSource(DataSource):
    """A SparkSource object defines a data source that a Spark offline store can use"""

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.BATCH_SPARK

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        table: Optional[str] = None,
        query: Optional[str] = None,
        path: Optional[str] = None,
        file_format: Optional[str] = None,
        table_format: Optional[TableFormat] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        timestamp_field: Optional[str] = None,
        date_partition_column: Optional[str] = None,
        date_partition_column_format: Optional[str] = "%Y-%m-%d",
    ):
        """Creates a SparkSource object.

        Args:
            name: The name of the data source, which should be unique within a project.
            table: The name of a Spark table.
            query: The query to be executed in Spark.
            path: The path to file data.
            file_format: The underlying file format (parquet, avro, csv, json).
            table_format: The table metadata format (iceberg, delta, hudi, etc.).
                Optional and separate from file_format.
            created_timestamp_column: Timestamp column indicating when the row
                was created, used for deduplicating rows.
            field_mapping: A dictionary mapping of column names in this data
                source to feature names in a feature table or view.
            description: A human-readable description.
            tags: A dictionary of key-value pairs to store arbitrary metadata.
            owner: The owner of the DataSource, typically the email of the primary
                maintainer.
            timestamp_field: Event timestamp field used for point-in-time joins of
                feature values.
            date_partition_column: The column to partition the data on for faster
                retrieval. This is useful for large tables and will limit the number of
        """
        # If no name, use the table as the default name.
        if name is None and table is None:
            raise DataSourceNoNameException()
        name = name or table
        assert name

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

        if not flags_helper.is_test():
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
            date_partition_column_format=date_partition_column_format,
            table_format=table_format,
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

    @property
    def table_format(self):
        """
        Returns the table format of this feature data source.
        """
        return self.spark_options.table_format

    @property
    def date_partition_column_format(self):
        """
        Returns the date partition column format of this feature data source.
        """
        return self.spark_options.date_partition_column_format

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
            table_format=spark_options.table_format,
            date_partition_column_format=spark_options.date_partition_column_format,
            date_partition_column=data_source.date_partition_column,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_SPARK,
            data_source_class_type="feast.infra.offline_stores.contrib.spark_offline_store.spark_source.SparkSource",
            field_mapping=self.field_mapping,
            date_partition_column=self.date_partition_column,
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
        return ((field.name, field.dataType.simpleString()) for field in df.schema)

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        try:
            from pyspark.sql import SparkSession
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("spark", str(e))

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
            df = self._load_dataframe_from_path(spark_session)
        except Exception:
            logger.exception(
                "Spark read of file source failed.\n" + traceback.format_exc()
            )
            raise DataSourceNotFoundException(self.path)
        tmp_table_name = get_temp_entity_table_name()
        df.createOrReplaceTempView(tmp_table_name)

        return f"`{tmp_table_name}`"

    def _load_dataframe_from_path(self, spark_session):
        """Load DataFrame from path, considering both file format and table format."""

        if self.table_format is None:
            # No table format specified, use standard file reading with file_format
            return spark_session.read.format(self.file_format).load(self.path)

        # Build reader with table format and options
        reader = spark_session.read.format(self.table_format.format_type.value)

        # Add table format specific options
        for key, value in self.table_format.properties.items():
            reader = reader.option(key, value)

        # For catalog-based table formats like Iceberg, the path is actually a table name
        # For file-based formats, it's still a file path
        return reader.load(self.path)

    def __eq__(self, other):
        base_eq = super().__eq__(other)
        if not base_eq:
            return False
        return (
            self.table == other.table
            and self.query == other.query
            and self.path == other.path
        )

    def __hash__(self):
        return super().__hash__()


class SparkOptions:
    allowed_formats = [format.value for format in SparkFileSourceFormat]

    def __init__(
        self,
        table: Optional[str],
        query: Optional[str],
        path: Optional[str],
        file_format: Optional[str],
        date_partition_column_format: Optional[str] = "%Y-%m-%d",
        table_format: Optional[TableFormat] = None,
    ):
        # Check that only one of the ways to load a spark dataframe can be used. We have
        # to treat empty string and null the same due to proto (de)serialization.
        if sum([(not (not arg)) for arg in [table, query, path]]) != 1:
            raise ValueError(
                "Exactly one of params(table, query, path) must be specified."
            )
        if path:
            # If table_format is specified, file_format is optional (table format determines the reader)
            # If no table_format, file_format is required for basic file reading
            if not table_format and not file_format:
                raise ValueError(
                    "If 'path' is specified without 'table_format', then 'file_format' is required."
                )
            # Only validate file_format if it's provided (it's optional with table_format)
            if file_format and file_format not in self.allowed_formats:
                raise ValueError(
                    f"'file_format' should be one of {self.allowed_formats}"
                )

        self._table = table
        self._query = query
        self._path = path
        self._file_format = file_format
        self._date_partition_column_format = date_partition_column_format
        self._table_format = table_format

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

    @property
    def date_partition_column_format(self):
        return self._date_partition_column_format

    @date_partition_column_format.setter
    def date_partition_column_format(self, date_partition_column_format):
        self._date_partition_column_format = date_partition_column_format

    @property
    def table_format(self):
        return self._table_format

    @table_format.setter
    def table_format(self, table_format):
        self._table_format = table_format

    @classmethod
    def from_proto(cls, spark_options_proto: DataSourceProto.SparkOptions):
        """
        Creates a SparkOptions from a protobuf representation of a spark option
        args:
            spark_options_proto: a protobuf representation of a datasource
        Returns:
            Returns a SparkOptions object based on the spark_options protobuf
        """
        # Parse table_format if present
        table_format = None
        if spark_options_proto.HasField("table_format"):
            table_format = table_format_from_proto(spark_options_proto.table_format)

        spark_options = cls(
            table=spark_options_proto.table,
            query=spark_options_proto.query,
            path=spark_options_proto.path,
            file_format=spark_options_proto.file_format,
            date_partition_column_format=spark_options_proto.date_partition_column_format,
            table_format=table_format,
        )

        return spark_options

    def to_proto(self) -> DataSourceProto.SparkOptions:
        """
        Converts an SparkOptionsProto object to its protobuf representation.
        Returns:
            SparkOptionsProto protobuf
        """
        table_format_proto = None
        if self.table_format:
            table_format_proto = self.table_format.to_proto()

        spark_options_proto = DataSourceProto.SparkOptions(
            table=self.table,
            query=self.query,
            path=self.path,
            file_format=self.file_format,
            date_partition_column_format=self.date_partition_column_format,
        )

        if table_format_proto:
            spark_options_proto.table_format.CopyFrom(table_format_proto)

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
        table_format: Optional[TableFormat] = None,
    ):
        self.spark_options = SparkOptions(
            table=table,
            query=query,
            path=path,
            file_format=file_format,
            table_format=table_format,
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        spark_options = SparkOptions.from_proto(storage_proto.spark_storage)
        return SavedDatasetSparkStorage(
            table=spark_options.table,
            query=spark_options.query,
            path=spark_options.path,
            file_format=spark_options.file_format,
            table_format=spark_options.table_format,
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(spark_storage=self.spark_options.to_proto())

    def to_data_source(self) -> DataSource:
        return SparkSource(
            table=self.spark_options.table,
            query=self.spark_options.query,
            path=self.spark_options.path,
            file_format=self.spark_options.file_format,
            table_format=self.spark_options.table_format,
        )
