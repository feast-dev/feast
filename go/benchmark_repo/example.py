from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, FileSource, ValueType, FeatureService

generated_data_source = FileSource(
    path="data/generated_data.parquet",
    event_timestamp_column="event_timestamp",
)

entity = Entity(
    name="entity",
    value_type=ValueType.INT64,
)

feature_views = [
    FeatureView(
        name=f"feature_view_{i}",
        entities=["entity"],
        ttl=Duration(seconds=86400),
        features=[
            Feature(name=f"feature_{10 * i + j}", dtype=ValueType.INT64)
            for j in range(10)
        ],
        online=True,
        batch_source=generated_data_source,
    )
    for i in range(25)
]

feature_services = [
    FeatureService(
        name=f"feature_service_{i}",
        features=feature_views[:5*(i + 1)],
    )
    for i in range(5)
]

def add_definitions_in_globals():
    for i, fv in enumerate(feature_views):
        globals()[f"feature_view_{i}"] = fv
    for i, fs in enumerate(feature_services):
        globals()[f"feature_service_{i}"] = fs

add_definitions_in_globals()
