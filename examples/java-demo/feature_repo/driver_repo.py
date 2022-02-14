import pandas as pd
from feast import Entity, Feature, FeatureView, FileSource, ValueType
from feast.data_source import RequestDataSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.request_feature_view import RequestFeatureView
from google.protobuf.duration_pb2 import Duration

driver_hourly_stats = FileSource(
    path="data/driver_stats_with_string.parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)
driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id",)
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=Duration(seconds=86400000),
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
        Feature(name="string_feature", dtype=ValueType.STRING),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)

# Define a request data source which encodes features / information only
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestDataSource(
    name="vals_to_add",
    schema={"val_to_add": ValueType.INT64, "val_to_add_2": ValueType.INT64},
)

# Define an on demand feature view which can generate new features based on
# existing feature views and RequestDataSource features
@on_demand_feature_view(
    inputs={
        "driver_hourly_stats": driver_hourly_stats_view,
        "vals_to_add": input_request,
    },
    features=[
        Feature(name="conv_rate_plus_val1", dtype=ValueType.DOUBLE),
        Feature(name="conv_rate_plus_val2", dtype=ValueType.DOUBLE),
    ],
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]
    return df


# Define request feature view
driver_age_request_fv = RequestFeatureView(
    name="driver_age",
    request_data_source=RequestDataSource(
        name="driver_age", schema={"driver_age": ValueType.INT64,}
    ),
)
