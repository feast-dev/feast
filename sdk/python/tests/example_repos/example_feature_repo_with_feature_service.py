from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.types import Float32, Int64, String

driver_locations_source = FileSource(
    path="data/driver_locations.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

driver = Entity(
    name="driver",  # The name is derived from this argument, not object name.
    join_keys=["driver_id"],
    description="driver id",
)

driver_locations = FeatureView(
    name="driver_locations",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="lat", dtype=Float32),
        Field(name="lon", dtype=String),
        Field(name="driver_id", dtype=Int64),
    ],
    online=True,
    source=driver_locations_source,
    tags={},
)

all_drivers_feature_service = FeatureService(
    name="driver_locations_service",
    features=[driver_locations],
    tags={"release": "production"},
)
