import json
import logging
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.type_map import pa_to_feast_value_type
from feast.value_type import ValueType

logger = logging.getLogger(__name__)

SUPPORTED_READER_TYPES = (
    "parquet",
    "csv",
    "json",
    "text",
    "images",
    "binary_files",
    "tfrecords",
    "webdataset",
    "huggingface",
    "mongo",
    "sql",
)


class RaySource(DataSource):
    """A RaySource object defines a data source that a RayOfflineStore can use.

    RaySource supports any format that ray.data can read: image directories, HuggingFace datasets,
    binary files, JSON, CSV, MongoDB, WebDataset shards, and more.

    The source stores only metadata (reader_type, path, reader_options). All
    data-loading logic lives in the RayOfflineStore, which reads reader_type and
    dispatches to the appropriate ray.data.read_*() or ray.data.from_*() call.

    Supported reader types:
        parquet       ray.data.read_parquet(path)
        csv           ray.data.read_csv(path)
        json          ray.data.read_json(path)
        text          ray.data.read_text(path)
        images        ray.data.read_images(path)
        binary_files  ray.data.read_binary_files(path)
        tfrecords     ray.data.read_tfrecords(path)
        webdataset    ray.data.read_webdataset(path)
        huggingface   ray.data.from_huggingface(dataset_name, split=...)
        mongo         ray.data.read_mongo(uri, database, collection)
        sql           ray.data.read_sql(sql, connection_factory)

    Examples:
        >>> # Directory of PNG images
        >>> source = RaySource(
        ...     name="cheque_images",
        ...     reader_type="images",
        ...     path="s3://bucket/cheques/",
        ...     timestamp_field="event_timestamp",
        ... )

        >>> # HuggingFace dataset
        >>> source = RaySource(
        ...     name="cheque_hf",
        ...     reader_type="huggingface",
        ...     reader_options={
        ...         "dataset_name": "cheques_sample_data",
        ...         "split": "test",
        ...     },
        ...     timestamp_field="event_timestamp",
        ... )
    """

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    def __init__(
        self,
        *,
        name: str,
        reader_type: str,
        path: Optional[str] = None,
        reader_options: Optional[Dict[str, Any]] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        timestamp_field: Optional[str] = None,
    ):
        """Creates a RaySource object.

        Args:
            name: The name of the data source, which should be unique within a project.
            reader_type: The ray.data reader to use. One of: parquet, csv, json, text,
                images, binary_files, tfrecords, webdataset, huggingface, mongo, sql.
                RayOfflineStore dispatches on this value to call the matching
                ray.data.read_*() or ray.data.from_*() function.
            path: File path or directory for file-based reader types (parquet, csv,
                json, text, images, binary_files, tfrecords, webdataset). Not required
                for huggingface, mongo, or sql.
            reader_options: Reader-type-specific connection parameters as a
                JSON-serializable dict. Examples:
                  huggingface: {"dataset_name": "org/name", "split": "train"}
                  mongo:       {"uri": "mongodb://host", "database": "db",
                                "collection": "col"}
                  sql:         {"sql": "SELECT …", "connection_url": "sqlite:///…"}
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
        """
        if reader_type not in SUPPORTED_READER_TYPES:
            raise ValueError(
                f"'reader_type' must be one of {SUPPORTED_READER_TYPES}, "
                f"got '{reader_type}'."
            )

        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

        self.ray_source_options = RaySourceOptions(
            reader_type=reader_type,
            path=path or "",
            reader_options=reader_options or {},
        )

    @property
    def reader_type(self) -> str:
        """Returns the ray.data reader type of this data source."""
        return self.ray_source_options.reader_type

    @property
    def path(self) -> str:
        """Returns the path of this data source."""
        return self.ray_source_options.path

    @property
    def reader_options(self) -> Dict[str, Any]:
        """Returns the reader-specific options of this data source."""
        return self.ray_source_options.reader_options

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        """Creates a RaySource from a protobuf representation.

        Args:
            data_source: A protobuf representation of a DataSource.

        Returns:
            A RaySource object.
        """
        assert data_source.type == DataSourceProto.CUSTOM_SOURCE
        config = json.loads(data_source.custom_options.configuration)
        return RaySource(
            name=data_source.name,
            reader_type=config["reader_type"],
            path=config.get("path") or None,
            reader_options=config.get("reader_options") or None,
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field or None,
            created_timestamp_column=data_source.created_timestamp_column or None,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type=(
                "feast.infra.offline_stores.contrib.ray_offline_store"
                ".ray_source.RaySource"
            ),
            field_mapping=self.field_mapping,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=json.dumps(
                    {
                        "reader_type": self.reader_type,
                        "path": self.path,
                        "reader_options": self.reader_options,
                    }
                ).encode()
            ),
        )
        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        return data_source_proto

    def validate(self, config: RepoConfig) -> None:
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return pa_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        # Return an empty list so that inference.py skips schema/entity inference
        # for RaySource during feast apply. Schema is resolved at materialisation
        # time by RayOfflineStore when it reads the actual Ray Dataset.
        return []

    def get_table_query_string(self) -> str:
        """Returns a string that can be used to reference this source."""
        parts = [f"reader_type={self.reader_type!r}"]
        if self.path:
            parts.append(f"path={self.path!r}")
        if self.reader_options:
            parts.append(f"reader_options={self.reader_options!r}")
        return f"RaySource({', '.join(parts)})"

    def __eq__(self, other):
        if not isinstance(other, RaySource):
            raise TypeError("Comparisons should only involve RaySource class objects.")
        base_eq = super().__eq__(other)
        if not base_eq:
            return False
        return (
            self.reader_type == other.reader_type
            and self.path == other.path
            and self.reader_options == other.reader_options
        )

    def __hash__(self):
        return super().__hash__()


class RaySourceOptions:
    """Options specific to a RaySource."""

    def __init__(
        self,
        reader_type: str,
        path: str,
        reader_options: Dict[str, Any],
    ):
        self._reader_type = reader_type
        self._path = path
        self._reader_options = reader_options

    @property
    def reader_type(self) -> str:
        """Returns the ray.data reader type."""
        return self._reader_type

    @reader_type.setter
    def reader_type(self, reader_type: str) -> None:
        self._reader_type = reader_type

    @property
    def path(self) -> str:
        """Returns the file path or directory."""
        return self._path

    @path.setter
    def path(self, path: str) -> None:
        self._path = path

    @property
    def reader_options(self) -> Dict[str, Any]:
        """Returns the reader-specific options dict."""
        return self._reader_options

    @reader_options.setter
    def reader_options(self, reader_options: Dict[str, Any]) -> None:
        self._reader_options = reader_options
