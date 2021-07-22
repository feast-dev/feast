from datetime import timedelta

from feast import BigQuerySource, Entity, Feature, FeatureView, ValueType

nonexistent_source = BigQuerySource(
    table_ref="project.dataset.nonexistent_table", event_timestamp_column=""
)

driver = Entity(name="driver", value_type=ValueType.INT64, description="driver id",)

nonexistent_features = FeatureView(
    name="driver_locations",
    entities=["driver"],
    ttl=timedelta(days=1),
    features=[
        Feature(name="lat", dtype=ValueType.FLOAT),
        Feature(name="lon", dtype=ValueType.STRING),
    ],
    input=nonexistent_source,
)
