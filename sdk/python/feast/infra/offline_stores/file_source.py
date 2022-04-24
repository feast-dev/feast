import warnings
from typing import Callable, Dict, Iterable, Optional, Tuple

from pyarrow._fs import FileSystem
from pyarrow._s3fs import S3FileSystem
from pyarrow.parquet import ParquetFile

from feast import type_map
from feast.data_format import FileFormat, ParquetFormat
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.value_type import ValueType


class FileSource(DataSource):
    def __init__(
        self,
        *args,
        path: Optional[str] = None,
        event_timestamp_column: Optional[str] = "",
        file_format: Optional[FileFormat] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        s3_endpoint_override: Optional[str] = None,
        name: Optional[str] = "",
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        timestamp_field: Optional[str] = "",
    ):
        """Create a FileSource from a file containing feature data. Only Parquet format supported.

        Args:

            path: File path to file containing feature data. Must contain an event_timestamp column, entity columns and
                feature columns.
            event_timestamp_column(optional): (Deprecated) Event timestamp column used for point in time joins of feature values.
            created_timestamp_column (optional): Timestamp column when row was created, used for deduplicating rows.
            file_format (optional): Explicitly set the file format. Allows Feast to bypass inferring the file format.
            field_mapping: A dictionary mapping of column names in this data source to feature names in a feature table
                or view. Only used for feature columns, not entities or timestamp columns.
            date_partition_column (optional): Timestamp column used for partitioning.
            s3_endpoint_override (optional): Overrides AWS S3 enpoint with custom S3 storage
            name (optional): Name for the file source. Defaults to the path.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the file source, typically the email of the primary
                maintainer.
            timestamp_field (optional): Event timestamp foe;d used for point in time
                joins of feature values.

        Examples:
            >>> from feast import FileSource
            >>> file_source = FileSource(path="my_features.parquet", timestamp_field="event_timestamp")
        """
        positional_attributes = ["path"]
        _path = path
        if args:
            if args:
                warnings.warn(
                    (
                        "File Source parameters should be specified as a keyword argument instead of a positional arg."
                        "Feast 0.23+ will not support positional arguments to construct File sources"
                    ),
                    DeprecationWarning,
                )
                if len(args) > len(positional_attributes):
                    raise ValueError(
                        f"Only {', '.join(positional_attributes)} are allowed as positional args when defining "
                        f"File sources, for backwards compatibility."
                    )
                if len(args) >= 1:
                    _path = args[0]
        if _path is None:
            raise ValueError(
                'No "path" argument provided. Please set "path" to the location of your file source.'
            )
        self.file_options = FileOptions(
            file_format=file_format,
            uri=_path,
            s3_endpoint_override=s3_endpoint_override,
        )

        if date_partition_column:
            warnings.warn(
                (
                    "The argument 'date_partition_column' is not supported for File sources."
                    "It will be removed in Feast 0.21+"
                ),
                DeprecationWarning,
            )

        super().__init__(
            name=name if name else path,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
            timestamp_field=timestamp_field,
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
    def path(self):
        """
        Returns the path of this file data source.
        """
        return self.file_options.uri

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

    def to_proto(self) -> DataSourceProto:
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

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        filesystem, path = FileSource.create_filesystem_and_path(
            self.path, self.file_options.s3_endpoint_override
        )
        schema = ParquetFile(
            path if filesystem is None else filesystem.open_input_file(path)
        ).schema_arrow
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
        pass


class FileOptions:
    """
    Configuration options for a file data source.
    """

    def __init__(
        self,
        file_format: Optional[FileFormat],
        s3_endpoint_override: Optional[str],
        uri: Optional[str],
    ):
        """
        Initializes a FileOptions object.

        Args:
            file_format (optional): File source format, e.g. parquet.
            s3_endpoint_override (optional): Custom s3 endpoint (used only with s3 uri).
            uri (optional): File source url, e.g. s3:// or local file.
        """
        self.file_format = file_format
        self.uri = uri or ""
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
