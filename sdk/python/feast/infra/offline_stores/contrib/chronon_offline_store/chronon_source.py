import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

import pyarrow.dataset as ds

from feast import type_map
from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.infra.offline_stores.file_source import FileSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType


class ChrononOptions:
    def __init__(
        self,
        *,
        materialization_path: str,
        chronon_group_by: Optional[str] = None,
        chronon_join: Optional[str] = None,
        online_endpoint: Optional[str] = None,
    ):
        self.materialization_path = materialization_path
        self.chronon_group_by = chronon_group_by or ""
        self.chronon_join = chronon_join or ""
        self.online_endpoint = online_endpoint or ""

    @classmethod
    def from_proto(cls, proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(proto.configuration.decode("utf8"))
        return cls(
            materialization_path=config["materialization_path"],
            chronon_group_by=config.get("chronon_group_by"),
            chronon_join=config.get("chronon_join"),
            online_endpoint=config.get("online_endpoint"),
        )

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        return DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {
                    "materialization_path": self.materialization_path,
                    "chronon_group_by": self.chronon_group_by,
                    "chronon_join": self.chronon_join,
                    "online_endpoint": self.online_endpoint,
                }
            ).encode()
        )


class ChrononSource(DataSource):
    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    def __init__(
        self,
        *,
        materialization_path: str,
        name: Optional[str] = None,
        chronon_group_by: Optional[str] = None,
        chronon_join: Optional[str] = None,
        online_endpoint: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        if not name and not materialization_path:
            raise DataSourceNoNameException()

        self._chronon_options = ChrononOptions(
            materialization_path=materialization_path,
            chronon_group_by=chronon_group_by,
            chronon_join=chronon_join,
            online_endpoint=online_endpoint,
        )

        super().__init__(
            name=name or materialization_path,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

    @property
    def materialization_path(self) -> str:
        return self._chronon_options.materialization_path

    @property
    def chronon_group_by(self) -> str:
        return self._chronon_options.chronon_group_by

    @property
    def chronon_join(self) -> str:
        return self._chronon_options.chronon_join

    @property
    def online_endpoint(self) -> str:
        return self._chronon_options.online_endpoint

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        assert data_source.HasField("custom_options")
        options = ChrononOptions.from_proto(data_source.custom_options)
        return ChrononSource(
            name=data_source.name,
            materialization_path=options.materialization_path,
            chronon_group_by=options.chronon_group_by,
            chronon_join=options.chronon_join,
            online_endpoint=options.online_endpoint,
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.chronon_offline_store.chronon_source.ChrononSource",
            field_mapping=self.field_mapping,
            custom_options=self._chronon_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )
        proto.timestamp_field = self.timestamp_field
        proto.created_timestamp_column = self.created_timestamp_column
        return proto

    def validate(self, config: RepoConfig):
        resolved_path = FileSource.get_uri_for_file_path(
            repo_path=config.repo_path, uri=self.materialization_path
        )
        if not Path(resolved_path).exists():
            raise FileNotFoundError(
                f"Chronon materialization path does not exist: {resolved_path}"
            )

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.pa_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        resolved_path = FileSource.get_uri_for_file_path(
            repo_path=config.repo_path, uri=self.materialization_path
        )
        schema = ds.dataset(resolved_path, format="parquet").schema
        return [(field.name, str(field.type)) for field in schema]
