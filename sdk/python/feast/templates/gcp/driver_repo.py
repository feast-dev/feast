from datetime import timedelta

from feast import BigQuerySource, Entity, Feature, FeatureView, ValueType

# Define an entity for the driver. You can think of entity as a primary key used to
# fetch features.
driver = Entity(name="driver", join_key="driver_id", value_type=ValueType.INT64)

# Feature views are a grouping based on how features are stored in either the online or offline store.
driver_stats_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single project cannot have the same name.
    name="driver_stats",
    # The list of entities specifies the keys required for joining or looking up features from this feature view
    entities=["driver"],
    # The timedelta is the maximum age that each feature value may have relative to its lookup time. For historical
    # features (used in training), TTL is relative to each timestamp provided in the entity dataframe. In this case
    # we have set the TTL to be arbitrarily large, but most teams will use a value closer to days/hours.
    ttl=timedelta(weeks=52),
    # The list of features defined below act as a schema to validate features that are being served to models
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    # Inputs are used to find feature values. In the case of this feature view we will query a source table on BigQuery
    # for driver statistics features
    input=BigQuerySource(
        # The BigQuery table where features can be found
        table_ref="feast-oss.demo_data.driver_stats",
        # The event timestamp is used for point-in-time joins and for ensuring TTL isn't breached
        event_timestamp_column="datetime",
        # The (optional) created timestamp is used to ensure there are no duplicate feature rows in the offline store
        created_timestamp_column="created",
    ),
    # Tags are user defined key/value pairs that are attached to each feature view
    tags={},
)
