# Copy from aws template
from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, FileSource, ValueType, FeatureService, FeatureStore
from feature_declare import driver_stats_source, driver, test_view
import pandas as pd
from datetime import datetime,timedelta
import time

fs = FeatureStore(repo_path=".")
# Deploy the feature store
fs.apply([driver, test_view])

# Create an entity dataframe. This is the dataframe that will be enriched with historical features
entity_df = pd.DataFrame(
        {
            "event_timestamp": [
                pd.Timestamp(dt, unit="ms", tz="UTC").round("ms")
                for dt in pd.date_range(
                    start=datetime.now() - timedelta(days=3),
                    end=datetime.now(),
                    periods=3,
                )
            ],
            "driver_id": [1001, 1002, 1003],
        }
    )
# Select features
features = ["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"]

training_df = fs.get_historical_features(
        entity_df=entity_df,
        features=features,
)

print(training_df.to_df())

# Export historical features to maxcompute
print(training_df.to_maxcompute("my_samples"))

training_df = fs.get_historical_features(
        entity_df="feast_driver_entity_table",
        features=features,
)

print("Loading features into the online store...")
fs.materialize_incremental(end_date=datetime.now())
