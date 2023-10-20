# Databricks notebook source
# MAGIC %%sh
# MAGIC pip install feast -U -q
# MAGIC pip install Pygments -q
# MAGIC echo "Please restart your runtime now (Runtime -> Restart runtime). This ensures that the correct dependencies are loaded."
# MAGIC

# COMMAND ----------

!feast init feature_repo

# COMMAND ----------

!pygmentize feature_store.yaml

# COMMAND ----------

import pandas as pd

pd.read_parquet("data/driver_stats.parquet")


# COMMAND ----------

# MAGIC %cd feature_repo
# MAGIC !ls -R

# COMMAND ----------

!feast apply

# COMMAND ----------

from datetime import datetime
import pandas as pd

from feast import FeatureStore

# The entity dataframe is the dataframe we want to enrich with feature values
# Note: see https://docs.feast.dev/getting-started/concepts/feature-retrieval for more details on how to retrieve
# for all entities in the offline store instead
entity_df = pd.DataFrame.from_dict(
    {
        # entity's join key -> entity values
        "driver_id": [1001, 1002, 1003],
        # "event_timestamp" (reserved key) -> timestamps
        "event_timestamp": [
            datetime(2021, 4, 12, 10, 59, 42),
            datetime(2021, 4, 12, 8, 12, 10),
            datetime(2021, 4, 12, 16, 40, 26),
        ],
        # (optional) label name -> label values. Feast does not process these
        "label_driver_reported_satisfaction": [1, 5, 3],
        # values we're using for an on-demand transformation
        "val_to_add": [1, 2, 3],
        "val_to_add_2": [10, 20, 30],
    }
)

store = FeatureStore(repo_path=".")

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
        "transformed_conv_rate:conv_rate_plus_val1",
        "transformed_conv_rate:conv_rate_plus_val2",
    ],
).to_df()

print("----- Feature schema -----\n")
print(training_df.info())

print()
print("----- Example features -----\n")
print(training_df.head())

