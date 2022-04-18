from datetime import timedelta
from typing import Dict, List, Optional, Union

from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto

SUPPORTED_BATCH_SOURCES = {
    "BigQuerySource",
    "FileSource",
    "RedshiftSource",
    "SnowflakeSource",
    "SparkSource",
    "TrinoSource",
}


class BatchFeatureView(FeatureView):
    def __init__(
        self,
        *,
        name: Optional[str] = None,
        entities: Optional[Union[List[Entity], List[str]]] = None,
        ttl: Optional[timedelta] = None,
        tags: Optional[Dict[str, str]] = None,
        online: bool = True,
        description: str = "",
        owner: str = "",
        schema: Optional[List[Field]] = None,
        source: Optional[DataSource] = None,
    ):

        if source is None:
            raise ValueError("Feature views need a source specified")
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
            batch_source=None,
            stream_source=None,
            tags=tags,
            online=online,
            description=description,
            owner=owner,
            schema=schema,
            source=source,
        )
