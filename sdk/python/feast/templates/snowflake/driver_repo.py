from datetime import timedelta

import yaml

from feast import Entity, FeatureService, FeatureView, SnowflakeSource

# Define an entity for the driver. Entities can be thought of as primary keys used to
# retrieve features. Entities are also used to join multiple tables/views during the
# construction of feature vectors
driver = Entity(
    # Name of the entity. Must be unique within a project
    name="driver",
    # The join keys of an entity describe the storage level field/column on which
    # features can be looked up. The join keys are also used to join feature
    # tables/views when building feature vectors
    join_keys=["driver_id"],
)

# Indicates a data source from which feature values can be retrieved. Sources are queried when building training
# datasets or materializing features into an online store.
project_name = yaml.safe_load(open("feature_store.yaml"))["project"]

driver_stats_source = SnowflakeSource(
    # The Snowflake table where features can be found
    database=yaml.safe_load(open("feature_store.yaml"))["offline_store"]["database"],
    table=f"{project_name}_feast_driver_hourly_stats",
    # The event timestamp is used for point-in-time joins and for ensuring only
    # features within the TTL are returned
    timestamp_field="event_timestamp",
    # The (optional) created timestamp is used to ensure there are no duplicate
    # feature rows in the offline store or when building training datasets
    created_timestamp_column="created",
)

# Feature views are a grouping based on how features are stored in either the
# online or offline store.
driver_stats_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="driver_hourly_stats",
    # The list of entities specifies the keys required for joining or looking
    # up features from this feature view. The reference provided in this field
    # correspond to the name of a defined entity (or entities)
    entities=[driver],
    # The timedelta is the maximum age that each feature value may have
    # relative to its lookup time. For historical features (used in training),
    # TTL is relative to each timestamp provided in the entity dataframe.
    # TTL also allows for eviction of keys from online stores and limits the
    # amount of historical scanning required for historical feature values
    # during retrieval
    ttl=timedelta(weeks=52),
    # Batch sources are used to find feature values. In the case of this feature
    # view we will query a source table on Redshift for driver statistics
    # features
    batch_source=driver_stats_source,
)

driver_stats_fs = FeatureService(name="driver_activity", features=[driver_stats_fv])
