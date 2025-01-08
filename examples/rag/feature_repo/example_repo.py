from datetime import timedelta

from feast import (
    FeatureView,
    Field,
    FileSource,
)
from feast.data_format import ParquetFormat
from feast.types import Float32, Array
from feast import Entity

item = Entity(name="item_id")

parquet_file_path = "./data/city_wikipedia_summaries_with_embeddings.parquet"

source = FileSource(
    file_format=ParquetFormat(),
    path=parquet_file_path,
    timestamp_field="event_timestamp",
)

city_embeddings_feature_view = FeatureView(
    name="city_embeddings",
    entities=[item],
    schema=[
        Field(name="Embeddings", dtype=Array(Float32)),
    ],
    source=source,
    ttl=timedelta(hours=2),
)