import abc
from typing import TYPE_CHECKING, Dict, Optional, Type, cast

import pyarrow as pa
from pytz import UTC

from feast.data_source import DataSource
from feast.embedded_go.type_map import FEAST_TYPE_TO_ARROW_TYPE, PA_TIMESTAMP_TYPE
from feast.errors import (
    FeastObjectNotFoundException,
    FeatureViewNotFoundException,
    OnDemandFeatureViewNotFoundException,
)
from feast.protos.feast.core.FeatureService_pb2 import (
    LoggingConfig as LoggingConfigProto,
)
from feast.types import from_value_type

if TYPE_CHECKING:
    from feast import FeatureService
    from feast.registry import Registry


class LoggingSource:
    @abc.abstractmethod
    def get_schema(self, registry: "Registry") -> pa.Schema:
        """ Generate schema for logs destination. """
        raise NotImplementedError

    @abc.abstractmethod
    def get_partition_column(self, registry: "Registry") -> str:
        """ Return partition column that must exist in generated schema. """
        raise NotImplementedError

    @abc.abstractmethod
    def get_log_timestamp_column(self) -> str:
        """ Return timestamp column that must exist in generated schema. """
        raise NotImplementedError


class FeatureServiceLoggingSource(LoggingSource):
    def __init__(self, feature_service: "FeatureService", project: str):
        self._feature_service = feature_service
        self._project = project

    def get_schema(self, registry: "Registry") -> pa.Schema:
        fields: Dict[str, pa.DataType] = {}

        for projection in self._feature_service.feature_view_projections:
            for feature in projection.features:
                fields[
                    f"{projection.name_to_use()}__{feature.name}"
                ] = FEAST_TYPE_TO_ARROW_TYPE[feature.dtype]
                fields[
                    f"{projection.name_to_use()}__{feature.name}__timestamp"
                ] = PA_TIMESTAMP_TYPE
                fields[
                    f"{projection.name_to_use()}__{feature.name}__status"
                ] = pa.int32()

            try:
                feature_view = registry.get_feature_view(projection.name, self._project)
            except FeatureViewNotFoundException:
                try:
                    on_demand_feature_view = registry.get_on_demand_feature_view(
                        projection.name, self._project
                    )
                except OnDemandFeatureViewNotFoundException:
                    raise FeastObjectNotFoundException(
                        f"Can't recognize feature view with a name {projection.name}"
                    )

                for (
                    request_source
                ) in on_demand_feature_view.source_request_sources.values():
                    for field in request_source.schema:
                        fields[field.name] = FEAST_TYPE_TO_ARROW_TYPE[field.dtype]

            else:
                for entity_name in feature_view.entities:
                    entity = registry.get_entity(entity_name, self._project)
                    join_key = projection.join_key_map.get(
                        entity.join_key, entity.join_key
                    )
                    fields[join_key] = FEAST_TYPE_TO_ARROW_TYPE[
                        from_value_type(entity.value_type)
                    ]

        # system columns
        fields["request_id"] = pa.string()
        fields["log_timestamp"] = pa.timestamp("us", tz=UTC)
        fields["log_date"] = pa.date32()

        return pa.schema(
            [pa.field(name, data_type) for name, data_type in fields.items()]
        )

    def get_partition_column(self, registry: "Registry") -> str:
        return "log_date"

    def get_log_timestamp_column(self) -> str:
        return "log_timestamp"


class _DestinationRegistry(type):
    classes_by_proto_attr_name: Dict[str, Type["LoggingDestination"]] = {}

    def __new__(cls, name, bases, dct):
        kls = type.__new__(cls, name, bases, dct)
        if dct.get("_proto_attr_name"):
            cls.classes_by_proto_attr_name[dct["_proto_attr_name"]] = kls
        return kls


class LoggingDestination:
    _proto_attr_name: str

    @classmethod
    @abc.abstractmethod
    def from_proto(cls, config_proto: LoggingConfigProto) -> "LoggingDestination":
        raise NotImplementedError

    @abc.abstractmethod
    def to_proto(self) -> LoggingConfigProto:
        raise NotImplementedError

    @abc.abstractmethod
    def to_data_source(self) -> DataSource:
        raise NotImplementedError


class LoggingConfig:
    destination: LoggingDestination

    def __init__(self, destination: LoggingDestination):
        self.destination = destination

    @classmethod
    def from_proto(cls, config_proto: LoggingConfigProto) -> Optional["LoggingConfig"]:
        proto_attr_name = cast(str, config_proto.WhichOneof("destination"))
        if proto_attr_name is None:
            return

        destination_class = _DestinationRegistry.classes_by_proto_attr_name[
            proto_attr_name
        ]
        return LoggingConfig(destination=destination_class.from_proto(config_proto))

    def to_proto(self) -> LoggingConfigProto:
        proto = self.destination.to_proto()
        return proto
