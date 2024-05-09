from datetime import datetime
from typing import Union

from feast import FeatureStore

import pandas as pd
import pyarrow as pa
import pyarrow.flight as fl

entity_df = pd.DataFrame.from_dict(
    {
        "driver_id": [1001, 1002, 1003],
        "event_timestamp": [
            datetime(2021, 4, 12, 10, 59, 42),
            datetime(2021, 4, 12, 8, 12, 10),
            datetime(2021, 4, 12, 16, 40, 26),
        ],
        "label_driver_reported_satisfaction": [1, 5, 3],
        "val_to_add": [1, 2, 3],
        "val_to_add_2": [10, 20, 30],
    }
)

record_batch_entity = pa.Table.from_pandas(entity_df)
flight_info_entity = pa.flight.FlightDescriptor.for_command("entity_df_descriptor")

features = [
    "driver_hourly_stats:conv_rate",
    "driver_hourly_stats:acc_rate",
    "driver_hourly_stats:avg_daily_trips",
    "transformed_conv_rate:conv_rate_plus_val1",
    "transformed_conv_rate:conv_rate_plus_val2",
]

features_array =  pa.array(features)
features_batch = pa.RecordBatch.from_arrays([features_array], ['features'])
flight_info_features = pa.flight.FlightDescriptor.for_command("features_descriptor")

store = FeatureStore(repo_path=".")

training_df = store.get_historical_features(entity_df, features).to_df()

print("----- Feature schema -----\n")
print(training_df.info())

print()
print("-----  Features -----\n")
print(training_df.head())

print('------training_df----')

print(training_df)