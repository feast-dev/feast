import functools
import warnings
from datetime import timedelta
from types import MethodType
from typing import Dict, List, Optional, Union

import dill
from google.protobuf.duration_pb2 import Duration

from feast import utils
from feast.aggregation import Aggregation
from feast.data_source import DataSource, KafkaSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.FeatureView_pb2 import (
    MaterializationInterval as MaterializationIntervalProto,
)
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    UserDefinedFunction as UserDefinedFunctionProto,
)
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureView as StreamFeatureViewProto,
)
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureViewMeta as StreamFeatureViewMetaProto,
)
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureViewSpec as StreamFeatureViewSpecProto,
)

warnings.simplefilter("once", RuntimeWarning)

SUPPORTED_STREAM_SOURCES = {"KafkaSource", "PushSource"}


class StreamFeatureView(FeatureView):
    """
    NOTE: Stream Feature Views are not yet fully implemented and exist to allow users to register their stream sources and
    schemas with Feast.
    """

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
        aggregations: Optional[List[Aggregation]] = None,
        mode: Optional[str] = "spark",  # Mode of ingestion/transformation
        timestamp_field: Optional[str] = "",  # Timestamp for aggregation
        udf: Optional[MethodType] = None,
    ):
        warnings.warn(
            "Stream Feature Views are experimental features in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        if source is None:
            raise ValueError("Stream Feature views need a source specified")
        # source uses the batch_source of the kafkasource in feature_view
        if (
            type(source).__name__ not in SUPPORTED_STREAM_SOURCES
            and source.to_proto().type != DataSourceProto.SourceType.CUSTOM_SOURCE
        ):
            raise ValueError(
                f"Stream feature views need a stream source, expected one of {SUPPORTED_STREAM_SOURCES} "
                f"or CUSTOM_SOURCE, got {type(source).__name__}: {source.name} instead "
            )
        self.aggregations = aggregations or []
        self.mode = mode
        self.timestamp_field = timestamp_field
        self.udf = udf
        _batch_source = None
        if isinstance(source, KafkaSource):
            _batch_source = source.batch_source if source.batch_source else None

        super().__init__(
            name=name,
            entities=entities,
            ttl=ttl,
            batch_source=_batch_source,
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
            raise TypeError("Comparisons should only involve StreamFeatureViews")

        if not super().__eq__(other):
            return False

        if (
            self.mode != other.mode
            or self.timestamp_field != other.timestamp_field
            or self.udf.__code__.co_code != other.udf.__code__.co_code
            or self.aggregations != other.aggregations
        ):
            return False

        return True

    def __hash__(self):
        return super().__hash__()

    def to_proto(self):
        meta = StreamFeatureViewMetaProto(materialization_intervals=[])
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)
        if self.last_updated_timestamp:
            meta.last_updated_timestamp.FromDatetime(self.last_updated_timestamp)

        for interval in self.materialization_intervals:
            interval_proto = MaterializationIntervalProto()
            interval_proto.start_time.FromDatetime(interval[0])
            interval_proto.end_time.FromDatetime(interval[1])
            meta.materialization_intervals.append(interval_proto)

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
            features=[field.to_proto() for field in self.schema],
            user_defined_function=UserDefinedFunctionProto(
                name=self.udf.__name__, body=dill.dumps(self.udf, recurse=True),
            )
            if self.udf
            else None,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            ttl=(ttl_duration if ttl_duration is not None else None),
            online=self.online,
            batch_source=batch_source_proto or None,
            stream_source=stream_source_proto,
            timestamp_field=self.timestamp_field,
            aggregations=[agg.to_proto() for agg in self.aggregations],
            mode=self.mode,
        )

        return StreamFeatureViewProto(spec=spec, meta=meta)

    @classmethod
    def from_proto(cls, sfv_proto):
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
        udf = (
            dill.loads(sfv_proto.spec.user_defined_function.body)
            if sfv_proto.spec.HasField("user_defined_function")
            else None
        )
        sfv_feature_view = cls(
            name=sfv_proto.spec.name,
            description=sfv_proto.spec.description,
            tags=dict(sfv_proto.spec.tags),
            owner=sfv_proto.spec.owner,
            online=sfv_proto.spec.online,
            schema=[
                Field.from_proto(field_proto) for field_proto in sfv_proto.spec.features
            ],
            ttl=(
                timedelta(days=0)
                if sfv_proto.spec.ttl.ToNanoseconds() == 0
                else sfv_proto.spec.ttl.ToTimedelta()
            ),
            source=stream_source,
            mode=sfv_proto.spec.mode,
            udf=udf,
            aggregations=[
                Aggregation.from_proto(agg_proto)
                for agg_proto in sfv_proto.spec.aggregations
            ],
            timestamp_field=sfv_proto.spec.timestamp_field,
        )

        if batch_source:
            sfv_feature_view.batch_source = batch_source

        if stream_source:
            sfv_feature_view.stream_source = stream_source

        sfv_feature_view.entities = list(sfv_proto.spec.entities)

        sfv_feature_view.features = [
            Field.from_proto(field_proto) for field_proto in sfv_proto.spec.features
        ]

        if sfv_proto.meta.HasField("created_timestamp"):
            sfv_feature_view.created_timestamp = (
                sfv_proto.meta.created_timestamp.ToDatetime()
            )
        if sfv_proto.meta.HasField("last_updated_timestamp"):
            sfv_feature_view.last_updated_timestamp = (
                sfv_proto.meta.last_updated_timestamp.ToDatetime()
            )

        for interval in sfv_proto.meta.materialization_intervals:
            stream_feature_view.materialization_intervals.append(
                (
                    utils.make_tzaware(interval.start_time.ToDatetime()),
                    utils.make_tzaware(interval.end_time.ToDatetime()),
                )
            )

        return sfv_feature_view


def stream_feature_view(
    *,
    entities: Optional[Union[List[Entity], List[str]]] = None,
    ttl: Optional[timedelta] = None,
    tags: Optional[Dict[str, str]] = None,
    online: Optional[bool] = True,
    description: Optional[str] = "",
    owner: Optional[str] = "",
    schema: Optional[List[Field]] = None,
    source: Optional[DataSource] = None,
    aggregations: Optional[List[Aggregation]] = None,
    mode: Optional[str] = "spark",  # Mode of ingestion/transformation
    timestamp_field: Optional[str] = "",  # Timestamp for aggregation
):
    """
    Creates an StreamFeatureView object with the given user function as udf.
    """

    def mainify(obj):
        # Needed to allow dill to properly serialize the udf. Otherwise, clients will need to have a file with the same
        # name as the original file defining the sfv.
        if obj.__module__ != "__main__":
            obj.__module__ = "__main__"

    def decorator(user_function):
        mainify(user_function)
        stream_feature_view_obj = StreamFeatureView(
            name=user_function.__name__,
            entities=entities,
            ttl=ttl,
            source=source,
            schema=schema,
            udf=user_function,
            description=description,
            tags=tags,
            online=online,
            owner=owner,
            aggregations=aggregations,
            mode=mode,
            timestamp_field=timestamp_field,
        )
        functools.update_wrapper(wrapper=stream_feature_view_obj, wrapped=user_function)
        return stream_feature_view_obj

    return decorator
