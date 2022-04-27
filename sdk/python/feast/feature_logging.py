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


REQUEST_ID_FIELD = "__request_id"
LOG_TIMESTAMP_FIELD = "__log_timestamp"
LOG_DATE_FIELD = "__log_date"


class LoggingSource:
    """
    Logging source describes object that produces logs (eg, feature service produces logs of served features).
    It should be able to provide schema of produced logs table and additional metadata that describes logs data.
    """

    @abc.abstractmethod
    def get_schema(self, registry: "Registry") -> pa.Schema:
        """ Generate schema for logs destination. """
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
        fields[REQUEST_ID_FIELD] = pa.string()
        fields[LOG_TIMESTAMP_FIELD] = pa.timestamp("us", tz=UTC)

        return pa.schema(
            [pa.field(name, data_type) for name, data_type in fields.items()]
        )

    def get_log_timestamp_column(self) -> str:
        return LOG_TIMESTAMP_FIELD


class _DestinationRegistry(type):
    classes_by_proto_attr_name: Dict[str, Type["LoggingDestination"]] = {}

    def __new__(cls, name, bases, dct):
        kls = type.__new__(cls, name, bases, dct)
        if dct.get("_proto_attr_name"):
            cls.classes_by_proto_attr_name[dct["_proto_attr_name"]] = kls
        return kls


class LoggingDestination:
    """
    Logging destination contains details about where exactly logs should be written inside an offline store.
    It is implementation specific - each offline store must implement LoggingDestination subclass.

    Kind of logging destination will be determined by matching attribute name in LoggingConfig protobuf message
    and "_proto_kind" property of each subclass.
    """

    _proto_kind: str

    @classmethod
    @abc.abstractmethod
    def from_proto(cls, config_proto: LoggingConfigProto) -> "LoggingDestination":
        raise NotImplementedError

    @abc.abstractmethod
    def to_proto(self) -> LoggingConfigProto:
        raise NotImplementedError

    @abc.abstractmethod
    def to_data_source(self) -> DataSource:
        """
        Convert this object into a data source to read logs from an offline store.
        """
        raise NotImplementedError


class LoggingConfig:
    destination: LoggingDestination

    def __init__(self, destination: LoggingDestination):
        self.destination = destination

    @classmethod
    def from_proto(cls, config_proto: LoggingConfigProto) -> Optional["LoggingConfig"]:
        proto_kind = cast(str, config_proto.WhichOneof("destination"))
        if proto_kind is None:
            return

        if proto_kind == "custom_destination":
            proto_kind = config_proto.custom_destination.kind

        destination_class = _DestinationRegistry.classes_by_proto_attr_name[proto_kind]
        return LoggingConfig(destination=destination_class.from_proto(config_proto))

    def to_proto(self) -> LoggingConfigProto:
        proto = self.destination.to_proto()
        return proto
