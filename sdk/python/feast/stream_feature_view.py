import copy
import functools
import warnings
from datetime import datetime, timedelta
from types import FunctionType
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import dill
from google.protobuf.message import Message
from typeguard import typechecked

from feast import flags_helper, utils
from feast.aggregation import Aggregation
from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    UserDefinedFunction as UserDefinedFunctionProto,
)
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureView as StreamFeatureViewProto,
)
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureViewSpec as StreamFeatureViewSpecProto,
)
from feast.protos.feast.core.Transformation_pb2 import (
    FeatureTransformationV2 as FeatureTransformationProto,
)
from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProtoV2,
)
from feast.transformation.base import Transformation
from feast.transformation.mode import TransformationMode

warnings.simplefilter("once", RuntimeWarning)

SUPPORTED_STREAM_SOURCES = {"KafkaSource", "PushSource"}


@typechecked
class StreamFeatureView(FeatureView):
    """
    A stream feature view defines a logical group of features that has both a stream data source and
    a batch data source.

    Attributes:
        name: The unique name of the stream feature view.
        mode: The transformation mode to use for the stream feature view. This can be one of TransformationMode.
        entities: List of entities or entity join keys.
        ttl: The amount of time this group of features lives. A ttl of 0 indicates that
            this group of features lives forever. Note that large ttl's or a ttl of 0
            can result in extremely computationally intensive queries.
        schema: The schema of the feature view, including feature, timestamp, and entity
            columns. If not specified, can be inferred from the underlying data source.
        source: The stream source of data where this group of features is stored.
        aggregations: List of aggregations registered with the stream feature view.
        mode: The mode of execution.
        timestamp_field: Must be specified if aggregations are specified. Defines the timestamp column on which to aggregate windows.
        online: A boolean indicating whether online retrieval, and write to online store is enabled for this feature view.
        offline: A boolean indicating whether offline retrieval, and write to offline store is enabled for this feature view.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the stream feature view, typically the email of the primary maintainer.
        udf: The user defined transformation function. This transformation function should have all of the corresponding imports imported within the function.
        udf_string: The string representation of the user defined transformation function.
        feature_transformation: The transformation to apply to the features.
                Note, feature_transformation has precedence over udf and udf_string.
        stream_engine: Optional dictionary containing stream engine specific configurations.
                Note, it will override the repo-level default stream engine config defined in the yaml file.
    """

    name: str
    entities: List[str]
    ttl: Optional[timedelta]
    source: DataSource
    sink_source: Optional[DataSource] = None
    schema: List[Field]
    entity_columns: List[Field]
    features: List[Field]
    online: bool
    offline: bool
    description: str
    tags: Dict[str, str]
    owner: str
    aggregations: List[Aggregation]
    mode: Union[TransformationMode, str]
    timestamp_field: str
    materialization_intervals: List[Tuple[datetime, datetime]]
    udf: Optional[FunctionType]
    udf_string: Optional[str]
    feature_transformation: Optional[Transformation]
    stream_engine: Optional[Dict[str, Any]] = None

    def __init__(
        self,
        *,
        name: str,
        source: Union[DataSource, "StreamFeatureView", List["StreamFeatureView"]],
        sink_source: Optional[DataSource] = None,
        entities: Optional[List[Entity]] = None,
        ttl: timedelta = timedelta(days=0),
        tags: Optional[Dict[str, str]] = None,
        online: bool = True,
        offline: bool = False,
        description: str = "",
        owner: str = "",
        schema: Optional[List[Field]] = None,
        aggregations: Optional[List[Aggregation]] = None,
        mode: Union[str, TransformationMode] = TransformationMode.PYTHON,
        timestamp_field: Optional[str] = "",
        udf: Optional[FunctionType] = None,
        udf_string: Optional[str] = "",
        feature_transformation: Optional[Transformation] = None,
        stream_engine: Optional[Dict[str, Any]] = None,
    ):
        if not flags_helper.is_test():
            warnings.warn(
                "Stream feature views are experimental features in alpha development. "
                "Some functionality may still be unstable so functionality can change in the future.",
                RuntimeWarning,
            )

        if isinstance(source, DataSource) and (
            type(source).__name__ not in SUPPORTED_STREAM_SOURCES
            and source.to_proto().type != DataSourceProto.SourceType.CUSTOM_SOURCE
        ):
            raise ValueError(
                f"Stream feature views need a stream source, expected one of {SUPPORTED_STREAM_SOURCES} "
                f"or CUSTOM_SOURCE, got {type(source).__name__}: {source.name} instead "
            )

        if aggregations and not timestamp_field:
            raise ValueError(
                "aggregations must have a timestamp field associated with them to perform the aggregations"
            )

        self.aggregations = aggregations or []
        self.mode = mode
        self.timestamp_field = timestamp_field or ""
        self.udf = udf
        self.udf_string = udf_string
        self.feature_transformation = (
            feature_transformation or self.get_feature_transformation()
        )
        self.stream_engine = stream_engine

        super().__init__(
            name=name,
            entities=entities,
            ttl=ttl,
            tags=tags,
            online=online,
            offline=offline,
            description=description,
            owner=owner,
            schema=schema,
            source=source,  # type: ignore[arg-type]
            mode=mode,
            sink_source=sink_source,
        )

    def get_feature_transformation(self) -> Optional[Transformation]:
        if not self.udf:
            # TODO: Currently StreamFeatureView allow no transformation, but this should be removed in the future
            return None
        if self.mode in (
            TransformationMode.PANDAS,
            TransformationMode.PYTHON,
            TransformationMode.SPARK_SQL,
            TransformationMode.SPARK,
        ) or self.mode in ("pandas", "python", "spark_sql", "spark"):
            return Transformation(
                mode=self.mode, udf=self.udf, udf_string=self.udf_string or ""
            )
        else:
            raise ValueError(
                f"Unsupported transformation mode: {self.mode} for StreamFeatureView"
            )

    def __eq__(self, other):
        if not isinstance(other, StreamFeatureView):
            raise TypeError("Comparisons should only involve StreamFeatureViews")

        if not super().__eq__(other):
            return False

        if not self.udf:
            return not other.udf
        if not other.udf:
            return False

        if (
            self.mode != other.mode
            or self.timestamp_field != other.timestamp_field
            or self.udf.__code__.co_code != other.udf.__code__.co_code
            or self.udf_string != other.udf_string
            or self.aggregations != other.aggregations
        ):
            return False

        return True

    def __hash__(self) -> int:
        return super().__hash__()

    def to_proto(self):
        meta = self.to_proto_meta()
        ttl_duration = self.get_ttl_duration()

        batch_source_proto = None
        if self.batch_source:
            batch_source_proto = self.batch_source.to_proto()
            batch_source_proto.data_source_class_type = f"{self.batch_source.__class__.__module__}.{self.batch_source.__class__.__name__}"

        stream_source_proto = None
        if self.stream_source:
            stream_source_proto = self.stream_source.to_proto()
            stream_source_proto.data_source_class_type = f"{self.stream_source.__class__.__module__}.{self.stream_source.__class__.__name__}"

        udf_proto, feature_transformation = None, None
        if self.udf:
            udf_proto = UserDefinedFunctionProto(
                name=self.udf.__name__,
                body=dill.dumps(self.udf, recurse=True),
                body_text=self.udf_string,
            )
            udf_proto_v2 = UserDefinedFunctionProtoV2(
                name=self.udf.__name__,
                body=dill.dumps(self.udf, recurse=True),
                body_text=self.udf_string,
            )

            feature_transformation = FeatureTransformationProto(
                user_defined_function=udf_proto_v2,
            )

        mode = (
            self.mode.value if isinstance(self.mode, TransformationMode) else self.mode
        )

        spec = StreamFeatureViewSpecProto(
            name=self.name,
            entities=self.entities,
            entity_columns=[field.to_proto() for field in self.entity_columns],
            features=[field.to_proto() for field in self.schema],
            user_defined_function=udf_proto,
            feature_transformation=feature_transformation,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            ttl=ttl_duration,
            online=self.online,
            batch_source=batch_source_proto or None,
            stream_source=stream_source_proto or None,
            timestamp_field=self.timestamp_field,
            aggregations=[agg.to_proto() for agg in self.aggregations],
            mode=mode,
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
        udf_string = (
            sfv_proto.spec.user_defined_function.body_text
            if sfv_proto.spec.HasField("user_defined_function")
            else None
        )
        # feature_transformation = (
        #     sfv_proto.spec.feature_transformation.user_defined_function.body_text
        #     if sfv_proto.spec.HasField("feature_transformation")
        #     else None
        # )
        stream_feature_view = cls(
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
            udf_string=udf_string,
            aggregations=[
                Aggregation.from_proto(agg_proto)
                for agg_proto in sfv_proto.spec.aggregations
            ],
            timestamp_field=sfv_proto.spec.timestamp_field,
        )

        if batch_source:
            stream_feature_view.batch_source = batch_source

        if stream_source:
            stream_feature_view.stream_source = stream_source

        stream_feature_view.entities = list(sfv_proto.spec.entities)

        stream_feature_view.features = [
            Field.from_proto(field_proto) for field_proto in sfv_proto.spec.features
        ]
        stream_feature_view.entity_columns = [
            Field.from_proto(field_proto)
            for field_proto in sfv_proto.spec.entity_columns
        ]

        if sfv_proto.meta.HasField("created_timestamp"):
            stream_feature_view.created_timestamp = (
                sfv_proto.meta.created_timestamp.ToDatetime()
            )
        if sfv_proto.meta.HasField("last_updated_timestamp"):
            stream_feature_view.last_updated_timestamp = (
                sfv_proto.meta.last_updated_timestamp.ToDatetime()
            )

        for interval in sfv_proto.meta.materialization_intervals:
            stream_feature_view.materialization_intervals.append(
                (
                    utils.make_tzaware(interval.start_time.ToDatetime()),
                    utils.make_tzaware(interval.end_time.ToDatetime()),
                )
            )

        return stream_feature_view

    def __copy__(self):
        fv = StreamFeatureView(
            name=self.name,
            schema=self.schema,
            ttl=self.ttl,
            tags=self.tags,
            online=self.online,
            description=self.description,
            owner=self.owner,
            aggregations=self.aggregations,
            mode=self.mode,
            timestamp_field=self.timestamp_field,
            source=self.stream_source if self.stream_source else self.batch_source,
            udf=self.udf,
            udf_string=self.udf_string,
            feature_transformation=self.feature_transformation,
        )
        fv.entities = self.entities
        fv.features = copy.copy(self.features)
        fv.entity_columns = copy.copy(self.entity_columns)
        fv.projection = copy.copy(self.projection)
        return fv

    @property
    def proto_class(self) -> Type[Message]:
        return StreamFeatureViewProto


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
    mode: Optional[str] = "spark",
    timestamp_field: Optional[str] = "",
):
    """
    Creates an StreamFeatureView object with the given user function as udf.
    Please make sure that the udf contains all non-built in imports within the function to ensure that the execution
    of a deserialized function does not miss imports.
    """

    def mainify(obj):
        # Needed to allow dill to properly serialize the udf. Otherwise, clients will need to have a file with the same
        # name as the original file defining the sfv.
        if obj.__module__ != "__main__":
            obj.__module__ = "__main__"

    def decorator(user_function):
        udf_string = dill.source.getsource(user_function)
        mainify(user_function)
        stream_feature_view_obj = StreamFeatureView(
            name=user_function.__name__,
            entities=entities,
            ttl=ttl,
            source=source,
            schema=schema,
            udf=user_function,
            udf_string=udf_string,
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
