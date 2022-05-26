import abc
from datetime import timedelta
from types import MethodType
from typing import Dict, List, Optional, Union, Callable, Tuple

from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
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

