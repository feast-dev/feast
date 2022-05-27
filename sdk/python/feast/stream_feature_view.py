import dill
from datetime import timedelta
from types import MethodType
from typing import Dict, List, Optional, Union, Callable, Tuple

from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from google.protobuf.duration_pb2 import Duration
from feast.protos.feast.core.StreamFeatureView_pb2 import StreamFeatureView as StreamFeatureViewProto
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureViewMeta as StreamFeatureViewMetaProto,
)
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureViewSpec as StreamFeatureViewSpecProto,
)
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    UserDefinedFunction as UserDefinedFunctionProto,
)
from isort import stream
from regex import R

SUPPORTED_STREAM_SOURCES = {"KafkaSource", "KinesisSource", "PushSource"}


# class Aggregation(abc.ABC):
#     column: str # Column name of the feature we are aggregating.
#     function: str # Provide built in aggregations sum, max, min, count mean
#     time_windows: Union[(timedelta, timedelta), List[tuple(timedelta, timedelta)]] # The time window and the slide


class StreamFeatureView(FeatureView):
    def __init__(
        self,
        *,
        name: Optional[str] = None,
        entities: Optional[Union[List[Entity], List[str]]] = None,
        ttl: Optional[timedelta] = None,
        tags: Optional[Dict[str, str]] = None,
        online: Optional[bool] = True,
        description: Optional[str] = "",
        owner: Optional[str] = "",
        schema: Optional[List[Field]] = None,
        source: Optional[DataSource] = None,
        #aggregations: List[Aggregation],
        mode: Optional[str] = "spark", # Mode of ingestion/transformation
        timestamp_field: Optional[str] = "", # Timestamp for aggregation
        udf: Optional[MethodType] = None,
    ):

        if source is None:
            raise ValueError("Feature views need a source specified")
        #TODO: There is a bug here with stream_source/batch_source
        # source uses the batch_source of the kafkasource in feature_view
        if (
            type(source).__name__ not in SUPPORTED_STREAM_SOURCES
            # and source.to_proto().type != DataSourceProto.SourceType.CUSTOM_SOURCE
        ):
            raise ValueError(
                f"Stream feature views need a stream source, expected one of {SUPPORTED_STREAM_SOURCES} "
                f"or CUSTOM_SOURCE, got {type(source).__name__}: {source.name} instead "
            )
        # self.aggregations = aggregations
        self.mode = mode
        self.timestamp_field = timestamp_field
        self.udf = udf

        super().__init__(
            name=name,
            entities=entities,
            ttl=ttl,
            batch_source=None,
            stream_source=source,
            tags=tags,
            online=online,
            description=description,
            owner=owner,
            schema=schema,
            source=source,
        )

    def __eq__(self, other):
        if not isinstance(other, StreamFeatureView):
            raise TypeError(
                "Comparisons should only involve StreamFeatureViews"
            )

        if not super().__eq__(other):
            return False

        if ( self.mode != other.mode or self.timestamp_field != other.timestamp_field
        or self.udf.__code__.co_code != other.udf.__code__.co_code):
            return False

        return True

    def __hash__(self):
        return super().__hash__()

    def to_proto(self):
        meta = StreamFeatureViewMetaProto()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)
        if self.last_updated_timestamp:
            meta.last_updated_timestamp.FromDatetime(self.last_updated_timestamp)

        ttl_duration = None
        if self.ttl is not None:
            ttl_duration = Duration()
            ttl_duration.FromTimedelta(self.ttl)

        if self.batch_source:
            batch_source_proto = self.batch_source.to_proto()
            batch_source_proto.data_source_class_type = f"{self.batch_source.__class__.__module__}.{self.batch_source.__class__.__name__}"

        stream_source_proto = None
        if self.stream_source:
            stream_source_proto = self.stream_source.to_proto()
            stream_source_proto.data_source_class_type = f"{self.stream_source.__class__.__module__}.{self.stream_source.__class__.__name__}"

        spec = StreamFeatureViewSpecProto(
            name=self.name,
            entities=self.entities,
            entity_columns=[field.to_proto() for field in self.entity_columns],
            features=[field.to_proto() for field in self.features],
            user_defined_function=UserDefinedFunctionProto(
                name=self.udf.__name__, body=dill.dumps(self.udf, recurse=True),
            ),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            ttl=(ttl_duration if ttl_duration is not None else None),
            online=self.online,
            batch_source=batch_source_proto,
            stream_source=stream_source_proto,
            timestamp_field=self.timestamp_field,
        )

        return StreamFeatureViewProto(spec=spec, meta=meta)

    @classmethod
    def from_proto(cls, sfv_proto: StreamFeatureViewProto):
        batch_source = (
            DataSource.from_proto(sfv_proto.spec.batch_source)
            if sfv_proto.spec.HasField("batch_source")
            else None
        )
        stream_source = (
            DataSource.from_proto(sfv_proto.spec.stream_source)
            if sfv_proto.spec.HasField("stream_source")
            else None
        )
        sfv_feature_view = cls(
            name=sfv_proto.spec.name,
            description=sfv_proto.spec.description,
            tags=dict(sfv_proto.spec.tags),
            owner=sfv_proto.spec.owner,
            online=sfv_proto.spec.online,
            ttl=(
                timedelta(days=0)
                if sfv_proto.spec.ttl.ToNanoseconds() == 0
                else sfv_proto.spec.ttl.ToTimedelta()
            ),
            source=stream_source,
            udf=dill.loads(
                sfv_feature_view.spec.user_defined_function.body
            ),
        )

        if batch_source:
            sfv_feature_view.batch_source = batch_source

        if stream_source:
            sfv_feature_view.stream_source = stream_source

        sfv_feature_view.entities = sfv_proto.spec.entities

        sfv_feature_view.features = [
            Field.from_proto(field_proto)
            for field_proto in sfv_proto.spec.features
        ]

        if sfv_proto.meta.HasField("created_timestamp"):
            sfv_feature_view.created_timestamp = (
                sfv_proto.meta.created_timestamp.ToDatetime()
            )
        if sfv_proto.meta.HasField("last_updated_timestamp"):
            sfv_feature_view.last_updated_timestamp = (
                sfv_proto.meta.last_updated_timestamp.ToDatetime()
            )

        return sfv_feature_view