# This is an example feature definition file

from datetime import timedelta
from typing import Any

import pandas as pd

from feast import (
    ConflictPolicy,
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    LabelView,
    Project,
    PushSource,
    RequestSource,
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, Json, Map, String, Struct

# Define a project for the feature repo
project = Project(name="%PROJECT_NAME%", description="A project for driver statistics")

# Define an entity for the driver. You can think of an entity as a primary key used to
# fetch features.
driver = Entity(name="driver", join_keys=["driver_id"])

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_stats_source = FileSource(
    name="driver_hourly_stats_source",
    path="%PARQUET_PATH%",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_stats_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64, description="Average daily trips"),
        Field(
            name="driver_metadata",
            dtype=Map,
            description="Driver metadata as key-value pairs",
        ),
        Field(
            name="driver_config", dtype=Json, description="Driver configuration as JSON"
        ),
        Field(
            name="driver_profile",
            dtype=Struct({"name": String, "age": String}),
            description="Driver profile as a typed struct",
        ),
    ],
    online=True,
    source=driver_stats_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"team": "driver_performance"},
    enable_validation=True,
    version="latest",
)

# Define a request data source which encodes features / information only
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name="val_to_add", dtype=Int64),
        Field(name="val_to_add_2", dtype=Int64),
    ],
)


# Define an on demand feature view which can generate new features based on
# existing feature views and RequestSource features. By default the transformation
# runs in Pandas mode (mode="pandas"): the UDF receives and returns a DataFrame.
@on_demand_feature_view(
    sources=[driver_stats_fv, input_request],
    schema=[
        Field(name="conv_rate_plus_val1", dtype=Float64),
        Field(name="conv_rate_plus_val2", dtype=Float64),
    ],
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]
    return df


# The same transformation written in native Python mode (mode="python"). The UDF
# receives a dict mapping each input feature name to a list of values (one per
# row) and returns a dict with the same shape. This avoids the Pandas overhead
# for small online requests and is often easier to reason about.
#
# Only the features the UDF needs are selected from the source feature view.
# This is required here: driver_stats_fv also has Map / Struct / Json fields, and
# Python mode feature inference cannot generate sample values for those types.
@on_demand_feature_view(
    sources=[driver_stats_fv[["conv_rate"]], input_request],
    schema=[
        Field(name="conv_rate_plus_val1_python", dtype=Float64),
        Field(name="conv_rate_plus_val2_python", dtype=Float64),
    ],
    mode="python",
)
def transformed_conv_rate_python(inputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "conv_rate_plus_val1_python": [
            conv_rate + val_to_add
            for conv_rate, val_to_add in zip(inputs["conv_rate"], inputs["val_to_add"])
        ],
        "conv_rate_plus_val2_python": [
            conv_rate + val_to_add_2
            for conv_rate, val_to_add_2 in zip(
                inputs["conv_rate"], inputs["val_to_add_2"]
            )
        ],
    }


# This groups features into a model version
driver_activity_v1 = FeatureService(
    name="driver_activity_v1",
    features=[
        driver_stats_fv[["conv_rate"]],  # Sub-selects a feature from a feature view
        transformed_conv_rate,  # Selects all features from the feature view
    ],
    logging_config=LoggingConfig(
        destination=FileLoggingDestination(path="%LOGGING_PATH%")
    ),
)
driver_activity_v2 = FeatureService(
    name="driver_activity_v2", features=[driver_stats_fv, transformed_conv_rate]
)

# Defines a way to push data (to be available offline, online or both) into Feast.
driver_stats_push_source = PushSource(
    name="driver_stats_push_source",
    batch_source=driver_stats_source,
)

# Defines a slightly modified version of the feature view from above, where the source
# has been changed to the push source. This allows fresh features to be directly pushed
# to the online store for this feature view.
driver_stats_fresh_fv = FeatureView(
    name="driver_hourly_stats_fresh",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
        Field(name="driver_metadata", dtype=Map),
        Field(name="driver_config", dtype=Json),
        Field(name="driver_profile", dtype=Struct({"name": String, "age": String})),
    ],
    online=True,
    source=driver_stats_push_source,  # Changed from above
    tags={"team": "driver_performance"},
    version="latest",
)


# Define an on demand feature view which can generate new features based on
# existing feature views and RequestSource features
@on_demand_feature_view(
    sources=[driver_stats_fresh_fv, input_request],  # relies on fresh version of FV
    schema=[
        Field(name="conv_rate_plus_val1", dtype=Float64),
        Field(name="conv_rate_plus_val2", dtype=Float64),
    ],
)
def transformed_conv_rate_fresh(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]
    return df


driver_activity_v3 = FeatureService(
    name="driver_activity_v3",
    features=[driver_stats_fresh_fv, transformed_conv_rate_fresh],
)


# The on demand feature views above run their transformation at read time, i.e.
# every time get_online_features() / get_historical_features() is called. Setting
# write_to_online_store=True instead runs the transformation at write time: the
# derived features are computed once when data is materialized or written to the
# online store, and are then served like any other pre-computed feature. This
# trades some ingestion cost for lower online retrieval latency.
#
# Because the results are persisted, the view must declare its entities and can
# only depend on other feature views (not on request-time data).
@on_demand_feature_view(
    entities=[driver],
    sources=[driver_stats_fv[["conv_rate", "acc_rate", "avg_daily_trips"]]],
    schema=[
        Field(name="conv_rate_x_acc_rate", dtype=Float64),
        Field(name="expected_daily_conversions", dtype=Float64),
    ],
    mode="pandas",
    write_to_online_store=True,
)
def transformed_conv_rate_on_write(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    # Cast explicitly so the output dtypes match the Float64 fields declared above
    # (conv_rate and acc_rate are Float32 in the source feature view).
    df["conv_rate_x_acc_rate"] = (inputs["conv_rate"] * inputs["acc_rate"]).astype(
        "float64"
    )
    df["expected_daily_conversions"] = (
        inputs["avg_daily_trips"] * inputs["conv_rate"]
    ).astype("float64")
    return df


# --- Label Views ---
# Label views manage mutable human labels for training data, RLHF, and evaluation.
# They use PushSources so labels can be submitted from the UI or external tools.

driver_quality_labels_source = PushSource(
    name="driver_quality_labels_push",
    batch_source=FileSource(
        name="driver_quality_labels_batch",
        path="%LABEL_DATA_PATH%",
        timestamp_field="event_timestamp",
    ),
)

driver_quality_labels = LabelView(
    name="driver_quality_labels",
    entities=[driver],
    schema=[
        Field(name="is_reliable", dtype=Int64),
        Field(name="quality_score", dtype=Float32),
        Field(name="reviewer_notes", dtype=String),
        Field(name="labeler", dtype=String),
    ],
    source=driver_quality_labels_source,
    labeler_field="labeler",
    conflict_policy=ConflictPolicy.LAST_WRITE_WINS,
    description="Human quality labels for drivers - used for model training and evaluation",
    tags={
        "feast.io/labeling-method": "table",
        "feast.io/field-role:is_reliable": "label",
        "feast.io/field-role:quality_score": "label",
        "feast.io/field-role:reviewer_notes": "metadata",
        "feast.io/label-values:is_reliable": "1 0",
        "feast.io/label-widget:is_reliable": "binary",
        "feast.io/label-widget:quality_score": "number",
        "feast.io/label-widget:reviewer_notes": "text",
    },
)
