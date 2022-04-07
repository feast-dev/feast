from datetime import timedelta

from feast import (
    BigQuerySource,
    Entity,
    Feature,
    FeatureService,
    FeatureView,
    PushSource,
    ValueType,
)

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

customer_profile_source = BigQuerySource(
    name="customer_profile_source",
    table="feast-oss.public.customers",
    timestamp_field="event_timestamp",
)

customer_driver_combined_source = BigQuerySource(
    table="feast-oss.public.customer_driver",
    timestamp_field="event_timestamp",
)

driver_locations_push_source = PushSource(
    name="driver_locations_push",
    schema={
        "driver_id": ValueType.STRING,
        "driver_lat": ValueType.FLOAT,
        "driver_long": ValueType.STRING,
    },
    batch_source=driver_locations_source,
)

driver = Entity(
    name="driver",  # The name is derived from this argument, not object name.
    join_key="driver_id",
    value_type=ValueType.INT64,
    description="driver id",
)

customer = Entity(
    name="customer",  # The name is derived from this argument, not object name.
    join_key="customer_id",
    value_type=ValueType.STRING,
)


driver_locations = FeatureView(
    name="driver_locations",
    entities=["driver"],
    ttl=timedelta(days=1),
    features=[
        Feature(name="lat", dtype=ValueType.FLOAT),
        Feature(name="lon", dtype=ValueType.STRING),
    ],
    online=True,
    batch_source=driver_locations_source,
    tags={},
)

pushed_driver_locations = FeatureView(
    name="pushed_driver_locations",
    entities=["driver"],
    ttl=timedelta(days=1),
    features=[
        Feature(name="driver_lat", dtype=ValueType.FLOAT),
        Feature(name="driver_long", dtype=ValueType.STRING),
    ],
    online=True,
    stream_source=driver_locations_push_source,
    tags={},
)

customer_profile = FeatureView(
    name="customer_profile",
    entities=["customer"],
    ttl=timedelta(days=1),
    features=[
        Feature(name="avg_orders_day", dtype=ValueType.FLOAT),
        Feature(name="name", dtype=ValueType.STRING),
        Feature(name="age", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=customer_profile_source,
    tags={},
)

customer_driver_combined = FeatureView(
    name="customer_driver_combined",
    entities=["customer", "driver"],
    ttl=timedelta(days=1),
    features=[Feature(name="trips", dtype=ValueType.INT64)],
    online=True,
    batch_source=customer_driver_combined_source,
    tags={},
)


all_drivers_feature_service = FeatureService(
    name="driver_locations_service",
    features=[driver_locations],
    tags={"release": "production"},
)
