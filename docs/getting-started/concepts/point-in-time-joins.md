# Point-in-time joins

Feature values in Feast are modeled as time-series records. Below is an example of a driver feature view with two feature columns \(`trips_today`, and `earnings_today`\):

![](../../.gitbook/assets/image%20%2836%29.png)

The above table can be registered with Feast through the following feature view:

```python
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int64
from datetime import timedelta

driver = Entity(name="driver", join_keys=["driver_id"])

driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    schema=[
        Field(name="trips_today", dtype=Int64),
        Field(name="earnings_today", dtype=Float32),
    ],
    ttl=timedelta(hours=2),
    source=FileSource(
        path="driver_hourly_stats.parquet"
    )
)
```

Feast is able to join features from one or more feature views onto an entity dataframe in a point-in-time correct way. This means Feast is able to reproduce the state of features at a specific point in the past.

Given the following entity dataframe, imagine a user would like to join the above `driver_hourly_stats` feature view onto it, while preserving the `trip_success` column:

![Entity dataframe containing timestamps, driver ids, and the target variable](../../.gitbook/assets/image%20%2823%29.png)

The timestamps within the entity dataframe above are the events at which we want to reproduce the state of the world \(i.e., what the feature values were at those specific points in time\). In order to do a point-in-time join, a user would load the entity dataframe and run historical retrieval:

```python
# Read in entity dataframe
entity_df = pd.read_csv("entity_df.csv")

training_df = store.get_historical_features(
    entity_df=entity_df,
    features = [
        'driver_hourly_stats:trips_today',
        'driver_hourly_stats:earnings_today'
    ],
)
```

For each row within the entity dataframe, Feast will query and join the selected features from the appropriate feature view data source. Feast will scan backward in time from the entity dataframe timestamp up to a maximum of the TTL time specified.

![](../../.gitbook/assets/image%20%2831%29.png)

{% hint style="info" %}
Please note that the TTL time is relative to each timestamp within the entity dataframe. TTL is not relative to the current point in time \(when you run the query\).
{% endhint %}

Below is the resulting joined training dataframe. It contains both the original entity rows and joined feature values:

![](../../.gitbook/assets/image%20%2829%29.png)

Three feature rows were successfully joined to the entity dataframe rows. The first row in the entity dataframe was older than the earliest feature rows in the feature view and could not be joined. The last row in the entity dataframe was outside of the TTL window \(the event happened 11 hours after the feature row\) and also couldn't be joined.

