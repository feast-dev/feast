import pandas as pd

from google.protobuf.duration_pb2 import Duration

from feast.value_type import ValueType
from feast.feature import Feature
from feast.feature_view import FeatureView
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.on_demand_feature_view import RequestDataSource, on_demand_feature_view
from feast import FileSource


file_path = "driver_stats.parquet"
driver_hourly_stats = FileSource(
    path=file_path,
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver. You can think of entity as a primary key used to
# fetch features.
driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id",)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=Duration(seconds=86400 * 7),
    features=[
        Feature(name="conv_rate", dtype=ValueType.DOUBLE),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)


input_request = RequestDataSource(
    name="vals_to_add",
    schema={
        "val_to_add": ValueType.INT64,
        "val_to_add_2": ValueType.INT64
    }
)


@on_demand_feature_view(
   inputs={
       'driver_hourly_stats': driver_hourly_stats_view,
       'vals_to_add': input_request
   },
   features=[
     Feature(name='conv_rate_plus_val1', dtype=ValueType.DOUBLE),
     Feature(name='conv_rate_plus_val2', dtype=ValueType.DOUBLE)
   ]
)
def transformed_conv_rate(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df['conv_rate_plus_val1'] = (features_df['conv_rate'] + features_df['val_to_add'])
    df['conv_rate_plus_val2'] = (features_df['conv_rate'] + features_df['val_to_add_2'])
    return df


generated_data_source = FileSource(
    path="benchmark_data.parquet",
    event_timestamp_column="event_timestamp",
)

entity = Entity(
    name="entity",
    value_type=ValueType.STRING,
)

benchmark_feature_views = [
    FeatureView(
        name=f"feature_view_{i}",
        entities=["entity"],
        ttl=Duration(seconds=86400),
        features=[
            Feature(name=f"feature_{10 * i + j}", dtype=ValueType.INT64)
            for j in range(10)
        ],
        online=True,
        batch_source=generated_data_source,
    )
    for i in range(25)
]

benchmark_feature_service = FeatureService(
    name=f"benchmark_feature_service",
    features=benchmark_feature_views,
)
