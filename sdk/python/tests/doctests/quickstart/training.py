from datetime import datetime

import pandas as pd

from feast import FeatureStore

entity_df = pd.DataFrame.from_dict(
    {
        "driver_id": [1001, 1002, 1003, 1004],
        "event_timestamp": [
            datetime(2021, 4, 12, 10, 59, 42),
            datetime(2021, 4, 12, 8, 12, 10),
            datetime(2021, 4, 12, 16, 40, 26),
            datetime(2021, 4, 12, 15, 1, 12),
        ],
    }
)


store = FeatureStore(repo_path=".")

training_df = store.get_historical_features(
    entity_df=entity_df,
    feature_refs=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
).to_df()

print(training_df.head())
