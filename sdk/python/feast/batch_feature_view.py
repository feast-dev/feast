import functools
import warnings
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import dill

from feast import flags_helper
from feast.aggregation import Aggregation
from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.transformation.base import Transformation
from feast.transformation.mode import TransformationMode

warnings.simplefilter("once", RuntimeWarning)

SUPPORTED_BATCH_SOURCES = {
    "BigQuerySource",
    "FileSource",
    "RedshiftSource",
    "SnowflakeSource",
    "SparkSource",
    "TrinoSource",
    "AthenaSource",
}


class BatchFeatureView(FeatureView):
    """
    A batch feature view defines a logical group of features that has only a batch data source.

    Attributes:
        name: The unique name of the batch feature view.
        entities: List of entities or entity join keys.
        ttl: The amount of time this group of features lives. A ttl of 0 indicates that
            this group of features lives forever. Note that large ttl's or a ttl of 0
            can result in extremely computationally intensive queries.
        schema: The schema of the feature view, including feature, timestamp, and entity
            columns. If not specified, can be inferred from the underlying data source.
        source: The batch source of data where this group of features is stored.
        online: A boolean indicating whether online retrieval and write to online store is enabled for this feature view.
        offline: A boolean indicating whether offline retrieval and write to offline store is enabled for this feature view.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the batch feature view, typically the email of the primary maintainer.
    """

    name: str
    mode: Union[TransformationMode, str]
    entities: List[str]
    ttl: Optional[timedelta]
    source: DataSource
    schema: List[Field]
    entity_columns: List[Field]
    features: List[Field]
    online: bool
    offline: bool
    description: str
    tags: Dict[str, str]
    owner: str
    timestamp_field: str
    materialization_intervals: List[Tuple[datetime, datetime]]
    udf: Optional[Callable[[Any], Any]]
    udf_string: Optional[str]
    feature_transformation: Transformation
    batch_engine: Optional[Field]
    aggregations: Optional[List[Aggregation]]

    def __init__(
        self,
        *,
        name: str,
        mode: Union[TransformationMode, str] = TransformationMode.PYTHON,
        source: DataSource,
        entities: Optional[List[Entity]] = None,
        ttl: Optional[timedelta] = None,
        tags: Optional[Dict[str, str]] = None,
        online: bool = False,
        offline: bool = True,
        description: str = "",
        owner: str = "",
        schema: Optional[List[Field]] = None,
        udf: Optional[Callable[[Any], Any]],
        udf_string: Optional[str] = "",
        feature_transformation: Optional[Transformation] = None,
        batch_engine: Optional[Field] = None,
        aggregations: Optional[List[Aggregation]] = None,
    ):
        if not flags_helper.is_test():
            warnings.warn(
                "Batch feature views are experimental features in alpha development. "
                "Some functionality may still be unstable so functionality can change in the future.",
                RuntimeWarning,
            )

        if (
            type(source).__name__ not in SUPPORTED_BATCH_SOURCES
            and source.to_proto().type != DataSourceProto.SourceType.CUSTOM_SOURCE
        ):
            raise ValueError(
                f"Batch feature views need a batch source, expected one of {SUPPORTED_BATCH_SOURCES} "
                f"or CUSTOM_SOURCE, got {type(source).__name__}: {source.name} instead "
            )

        self.mode = mode
        self.udf = udf
        self.udf_string = udf_string
        self.feature_transformation = (
            feature_transformation or self.get_feature_transformation()
        )
        self.batch_engine = batch_engine
        self.aggregations = aggregations or []

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
            source=source,
        )

    def get_feature_transformation(self) -> Transformation:
        if not self.udf:
            raise ValueError(
                "Either a UDF or a feature transformation must be provided for BatchFeatureView"
            )
        if self.mode in (
            TransformationMode.PANDAS,
            TransformationMode.PYTHON,
            TransformationMode.SQL,
        ) or self.mode in ("pandas", "python", "sql"):
            return Transformation(
                mode=self.mode, udf=self.udf, udf_string=self.udf_string or ""
            )
        else:
            raise ValueError(
                f"Unsupported transformation mode: {self.mode} for StreamFeatureView"
            )


def batch_feature_view(
    *,
    name: Optional[str] = None,
    mode: Union[TransformationMode, str] = TransformationMode.PYTHON,
    entities: Optional[List[str]] = None,
    ttl: Optional[timedelta] = None,
    source: Optional[DataSource] = None,
    tags: Optional[Dict[str, str]] = None,
    online: bool = True,
    offline: bool = True,
    description: str = "",
    owner: str = "",
    schema: Optional[List[Field]] = None,
):
    """
    Args:
        name:
        mode:
        entities:
        ttl:
        source:
        tags:
        online:
        offline:
        description:
        owner:
        schema:

    Returns:

    """

    def mainify(obj):
        # Needed to allow dill to properly serialize the udf. Otherwise, clients will need to have a file with the same
        # name as the original file defining the sfv.
        if obj.__module__ != "__main__":
            obj.__module__ = "__main__"

    def decorator(user_function):
        udf_string = dill.source.getsource(user_function)
        mainify(user_function)
        batch_feature_view_obj = BatchFeatureView(
            name=name or user_function.__name__,
            mode=mode,
            entities=entities,
            ttl=ttl,
            source=source,
            tags=tags,
            online=online,
            offline=offline,
            description=description,
            owner=owner,
            schema=schema,
            udf=user_function,
            udf_string=udf_string,
        )
        functools.update_wrapper(wrapper=batch_feature_view_obj, wrapped=user_function)
        return batch_feature_view_obj

    return decorator
