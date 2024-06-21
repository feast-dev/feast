import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

from feast import flags_helper
from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto

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
        online: A boolean indicating whether online retrieval is enabled for this feature view.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the batch feature view, typically the email of the primary maintainer.
    """

    name: str
    entities: List[str]
    ttl: Optional[timedelta]
    source: DataSource
    schema: List[Field]
    entity_columns: List[Field]
    features: List[Field]
    online: bool
    description: str
    tags: Dict[str, str]
    owner: str
    timestamp_field: str
    materialization_intervals: List[Tuple[datetime, datetime]]

    def __init__(
        self,
        *,
        name: str,
        source: DataSource,
        entities: Optional[Union[List[Entity], List[str]]] = None,
        ttl: Optional[timedelta] = None,
        tags: Optional[Dict[str, str]] = None,
        online: bool = True,
        description: str = "",
        owner: str = "",
        schema: Optional[List[Field]] = None,
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

        super().__init__(
            name=name,
            entities=entities,
            ttl=ttl,
            tags=tags,
            online=online,
            description=description,
            owner=owner,
            schema=schema,
            source=source,
        )
