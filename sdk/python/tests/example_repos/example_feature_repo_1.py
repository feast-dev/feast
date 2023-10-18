from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource, PushSource
from feast.types import Float32, Int64, String

# Note that file source paths are not validated, so there doesn't actually need to be any data
# at the paths for these file sources. Since these paths are effectively fake, this example
# feature repo should not be used for historical retrieval.

driver_locations_source = FileSource(
    path="data/driver_locations.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

customer_profile_source = FileSource(
    name="customer_profile_source",
    path="data/customer_profiles.parquet",
    timestamp_field="event_timestamp",
)

customer_driver_combined_source = FileSource(
    path="data/customer_driver_combined.parquet",
    timestamp_field="event_timestamp",
)

driver_locations_push_source = PushSource(
    name="driver_locations_push",
    batch_source=driver_locations_source,
)

driver = Entity(
    name="driver",  # The name is derived from this argument, not object name.
    join_keys=["driver_id"],
    description="driver id",
)

customer = Entity(
    name="customer",  # The name is derived from this argument, not object name.
    join_keys=["customer_id"],
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

pushed_driver_locations = FeatureView(
    name="pushed_driver_locations",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="driver_lat", dtype=Float32),
        Field(name="driver_long", dtype=String),
        Field(name="driver_id", dtype=Int64),
    ],
    online=True,
    source=driver_locations_push_source,
    tags={},
)

customer_profile = FeatureView(
    name="customer_profile",
    entities=[customer],
    ttl=timedelta(days=1),
    schema=[
        Field(name="avg_orders_day", dtype=Float32),
        Field(name="name", dtype=String),
        Field(name="age", dtype=Int64),
        Field(name="customer_id", dtype=String),
    ],
    online=True,
    source=customer_profile_source,
    tags={},
)

customer_driver_combined = FeatureView(
    name="customer_driver_combined",
    entities=[customer, driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="trips", dtype=Int64),
        Field(name="driver_id", dtype=Int64),
        Field(name="customer_id", dtype=String),
    ],
    online=True,
    source=customer_driver_combined_source,
    tags={},
)


all_drivers_feature_service = FeatureService(
    name="driver_locations_service",
    features=[driver_locations],
    tags={"release": "production"},
)
