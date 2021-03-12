from google.protobuf.duration_pb2 import Duration

from feast import BigQuerySource, Entity, Feature, FeatureView, ValueType

driver_locations_source = BigQuerySource(
    table_ref="rh_prod.ride_hailing_co.drivers",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)


driver = Entity(
    name="driver",  # The name is derived from this argument, not object name.
    value_type=ValueType.INT64,
    description="driver id",
)


driver_locations = FeatureView(
    name="driver_locations",
    entities=["driver"],
    ttl=Duration(seconds=86400 * 1),
    features=[
        Feature(name="lat", dtype=ValueType.FLOAT),
        Feature(name="lon", dtype=ValueType.STRING),
    ],
    online=True,
    inputs=driver_locations_source,
    tags={},
)
