from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import urlparse

import pyarrow
from packaging import version
from pyarrow._fs import FileSystem
from pyarrow._s3fs import S3FileSystem
from pyarrow.parquet import ParquetDataset
from typeguard import typechecked

from feast import type_map
from feast.data_format import DeltaFormat, FileFormat, ParquetFormat
from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.FeatureService_pb2 import (
    LoggingConfig as LoggingConfigProto,
)
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.value_type import ValueType


@typechecked
class FileSource(DataSource):
    """A FileSource object defines a data source that a DaskOfflineStore or DuckDBOfflineStore class can use."""

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.BATCH_FILE

    def __init__(
        self,
        *,
        path: str,
        name: Optional[str] = "",
        event_timestamp_column: Optional[str] = "",
        file_format: Optional[FileFormat] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        s3_endpoint_override: Optional[str] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        timestamp_field: Optional[str] = "",
    ):
        """
        Creates a FileSource object.

        Args:
            path: File path to file containing feature data. Must contain an event_timestamp column, entity columns and
                feature columns.
            name (optional): Name for the file source. Defaults to the path.
            event_timestamp_column (optional): (Deprecated in favor of timestamp_field) Event
                timestamp column used for point in time joins of feature values.
            created_timestamp_column (optional): Timestamp column when row was created, used for deduplicating rows.
            file_format (optional): Explicitly set the file format. Allows Feast to bypass inferring the file format.
            field_mapping: A dictionary mapping of column names in this data source to feature names in a feature table
                or view. Only used for feature columns, not entities or timestamp columns.
            s3_endpoint_override (optional): Overrides AWS S3 enpoint with custom S3 storage
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the file source, typically the email of the primary
                maintainer.
            timestamp_field (optional): Event timestamp field used for point in time
                joins of feature values.

        Examples:
            >>> from feast import FileSource
            >>> file_source = FileSource(path="my_features.parquet", timestamp_field="event_timestamp")
        """
        self.file_options = FileOptions(
            file_format=file_format,
            uri=path,
            s3_endpoint_override=s3_endpoint_override,
        )

        super().__init__(
            name=name if name else path,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

    # Note: Python requires redefining hash in child classes that override __eq__
    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, FileSource):
            raise TypeError("Comparisons should only involve FileSource class objects.")

        return (
            super().__eq__(other)
            and self.path == other.path
            and self.file_options.file_format == other.file_options.file_format
            and self.file_options.s3_endpoint_override
            == other.file_options.s3_endpoint_override
        )

    @property
    def path(self) -> str:
        """Returns the path of this file data source."""
        return self.file_options.uri

    @property
    def file_format(self) -> Optional[FileFormat]:
        """Returns the file format of this file data source."""
        return self.file_options.file_format

    @property
    def s3_endpoint_override(self) -> Optional[str]:
        """Returns the s3 endpoint override of this file data source."""
        return self.file_options.s3_endpoint_override

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        return FileSource(
            name=data_source.name,
            field_mapping=dict(data_source.field_mapping),
            file_format=FileFormat.from_proto(data_source.file_options.file_format),
            path=data_source.file_options.uri,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            s3_endpoint_override=data_source.file_options.s3_endpoint_override,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_FILE,
            field_mapping=self.field_mapping,
            file_options=self.file_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        # TODO: validate a FileSource
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.pa_to_feast_value_type

    @staticmethod
    def get_uri_for_file_path(repo_path: Union[Path, str, None], uri: str) -> str:
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme and parsed_uri.netloc:
            return uri  # Keep remote URIs as they are
        if repo_path is not None and not Path(uri).is_absolute():
            return str(Path(repo_path) / uri)
        return str(Path(uri))

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        absolute_path = self.get_uri_for_file_path(
            repo_path=config.repo_path, uri=self.file_options.uri
        )
        filesystem, path = FileSource.create_filesystem_and_path(
            str(absolute_path), self.file_options.s3_endpoint_override
        )

        # TODO why None check necessary
        if self.file_format is None or isinstance(self.file_format, ParquetFormat):
            if filesystem is None:
                kwargs = (
                    {"use_legacy_dataset": False}
                    if version.parse(pyarrow.__version__) < version.parse("15.0.0")
                    else {}
                )

                schema = ParquetDataset(path, **kwargs).schema
                if hasattr(schema, "names") and hasattr(schema, "types"):
                    # Newer versions of pyarrow doesn't have this method,
                    # but this field is good enough.
                    pass
                else:
                    schema = schema.to_arrow_schema()
            else:
                schema = ParquetDataset(path, filesystem=filesystem).schema
        elif isinstance(self.file_format, DeltaFormat):
            from deltalake import DeltaTable

            storage_options = {
                "AWS_ENDPOINT_URL": str(self.s3_endpoint_override),
            }

            schema = (
                DeltaTable(self.path, storage_options=storage_options)
                .schema()
                .to_pyarrow()
            )
        else:
            raise Exception(f"Unknown FileFormat -> {self.file_format}")

        return zip(schema.names, map(str, schema.types))

    @staticmethod
    def create_filesystem_and_path(
        path: str, s3_endpoint_override: str
    ) -> Tuple[Optional[FileSystem], str]:
        if path.startswith("s3://"):
            s3fs = S3FileSystem(
                endpoint_override=s3_endpoint_override if s3_endpoint_override else None
            )
            return s3fs, path.replace("s3://", "")
        else:
            return None, path

    def get_table_query_string(self) -> str:
        raise NotImplementedError


class FileOptions:
    """
    Configuration options for a file data source.

    Attributes:
        uri: File source url, e.g. s3:// or local file.
        s3_endpoint_override: Custom s3 endpoint (used only with s3 uri).
        file_format: File source format, e.g. parquet.
    """

    uri: str
    file_format: Optional[FileFormat]
    s3_endpoint_override: str

    def __init__(
        self,
        uri: str,
        file_format: Optional[FileFormat],
        s3_endpoint_override: Optional[str],
    ):
        """
        Initializes a FileOptions object.

        Args:
            uri: File source url, e.g. s3:// or local file.
            file_format (optional): File source format, e.g. parquet.
            s3_endpoint_override (optional): Custom s3 endpoint (used only with s3 uri).
        """
        self.uri = uri
        self.file_format = file_format
        self.s3_endpoint_override = s3_endpoint_override or ""

    @classmethod
    def from_proto(cls, file_options_proto: DataSourceProto.FileOptions):
        """
        Creates a FileOptions from a protobuf representation of a file option

        Args:
            file_options_proto: a protobuf representation of a datasource

        Returns:
            Returns a FileOptions object based on the file_options protobuf
        """
        file_options = cls(
            file_format=FileFormat.from_proto(file_options_proto.file_format),
            uri=file_options_proto.uri,
            s3_endpoint_override=file_options_proto.s3_endpoint_override,
        )
        return file_options

    def to_proto(self) -> DataSourceProto.FileOptions:
        """
        Converts an FileOptionsProto object to its protobuf representation.

        Returns:
            FileOptionsProto protobuf
        """
        file_options_proto = DataSourceProto.FileOptions(
            file_format=(
                None if self.file_format is None else self.file_format.to_proto()
            ),
            uri=self.uri,
            s3_endpoint_override=self.s3_endpoint_override,
        )

        return file_options_proto


class SavedDatasetFileStorage(SavedDatasetStorage):
    _proto_attr_name = "file_storage"

    file_options: FileOptions

    def __init__(
        self,
        path: str,
        file_format: FileFormat = ParquetFormat(),
        s3_endpoint_override: Optional[str] = None,
    ):
        self.file_options = FileOptions(
            file_format=file_format,
            s3_endpoint_override=s3_endpoint_override,
            uri=path,
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        file_options = FileOptions.from_proto(storage_proto.file_storage)
        return SavedDatasetFileStorage(
            path=file_options.uri,
            file_format=file_options.file_format,
            s3_endpoint_override=file_options.s3_endpoint_override,
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(file_storage=self.file_options.to_proto())

    def to_data_source(self) -> DataSource:
        return FileSource(
            path=self.file_options.uri,
            file_format=self.file_options.file_format,
            s3_endpoint_override=self.file_options.s3_endpoint_override,
        )

    @staticmethod
    def from_data_source(data_source: DataSource) -> "SavedDatasetStorage":
        assert isinstance(data_source, FileSource)
        return SavedDatasetFileStorage(
            path=data_source.path,
            file_format=data_source.file_format
            if data_source.file_format
            else ParquetFormat(),
            s3_endpoint_override=data_source.s3_endpoint_override,
        )


class FileLoggingDestination(LoggingDestination):
    _proto_kind = "file_destination"

    path: str
    s3_endpoint_override: str
    partition_by: Optional[List[str]]

    def __init__(
        self,
        *,
        path: str,
        s3_endpoint_override="",
        partition_by: Optional[List[str]] = None,
    ):
        self.path = path
        self.s3_endpoint_override = s3_endpoint_override
        self.partition_by = partition_by

    @classmethod
    def from_proto(cls, config_proto: LoggingConfigProto) -> "LoggingDestination":
        return FileLoggingDestination(
            path=config_proto.file_destination.path,
            s3_endpoint_override=config_proto.file_destination.s3_endpoint_override,
            partition_by=list(config_proto.file_destination.partition_by)
            if config_proto.file_destination.partition_by
            else None,
        )

    def to_proto(self) -> LoggingConfigProto:
        return LoggingConfigProto(
            file_destination=LoggingConfigProto.FileDestination(
                path=self.path,
                s3_endpoint_override=self.s3_endpoint_override,
                partition_by=self.partition_by,
            )
        )

    def to_data_source(self) -> DataSource:
        return FileSource(
            path=self.path,
            file_format=ParquetFormat(),
            s3_endpoint_override=self.s3_endpoint_override,
        )
