from datetime import timedelta

from feast import (
    BigQuerySource,
    Entity,
    FeatureService,
    FeatureView,
    Field,
    PushSource,
    ValueType,
)
from feast.types import Float32, Int64, String

driver_locations_source = BigQuerySource(
    table="feast-oss.public.drivers",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

driver_locations_source_query = BigQuerySource(
    query="SELECT * from feast-oss.public.drivers",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

driver_locations_source_query_2 = BigQuerySource(
    query="SELECT lat * 2 FROM feast-oss.public.drivers",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

driver_locations_source_query = BigQuerySource(
    query="SELECT * from feast-oss.public.drivers",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)

driver_locations_source_query_2 = BigQuerySource(
    query="SELECT lat * 2 FROM feast-oss.public.drivers",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)

customer_profile_source = BigQuerySource(
    name="customer_profile_source",
    table="feast-oss.public.customers",
    timestamp_field="event_timestamp",
)

customer_driver_combined_source = BigQuerySource(
    table="feast-oss.public.customer_driver", timestamp_field="event_timestamp",
)

driver_locations_push_source = PushSource(
    name="driver_locations_push", batch_source=driver_locations_source,
)

driver = Entity(
    name="driver",  # The name is derived from this argument, not object name.
    join_keys=["driver_id"],
    value_type=ValueType.INT64,
    description="driver id",
)

customer = Entity(
    name="customer",  # The name is derived from this argument, not object name.
    join_keys=["customer_id"],
    value_type=ValueType.STRING,
)


driver_locations = FeatureView(
    name="driver_locations",
    entities=["driver"],
    ttl=timedelta(days=1),
    schema=[Field(name="lat", dtype=Float32), Field(name="lon", dtype=String)],
    online=True,
    batch_source=driver_locations_source,
    tags={},
)

pushed_driver_locations = FeatureView(
    name="pushed_driver_locations",
    entities=["driver"],
    ttl=timedelta(days=1),
    schema=[
        Field(name="driver_lat", dtype=Float32),
        Field(name="driver_long", dtype=String),
    ],
    online=True,
    stream_source=driver_locations_push_source,
    tags={},
)

customer_profile = FeatureView(
    name="customer_profile",
    entities=["customer"],
    ttl=timedelta(days=1),
    schema=[
        Field(name="avg_orders_day", dtype=Float32),
        Field(name="name", dtype=String),
        Field(name="age", dtype=Int64),
    ],
    online=True,
    batch_source=customer_profile_source,
    tags={},
)

customer_driver_combined = FeatureView(
    name="customer_driver_combined",
    entities=["customer", "driver"],
    ttl=timedelta(days=1),
    schema=[Field(name="trips", dtype=Int64)],
    online=True,
    batch_source=customer_driver_combined_source,
    tags={},
)


all_drivers_feature_service = FeatureService(
    name="driver_locations_service",
    features=[driver_locations],
    tags={"release": "production"},
)
