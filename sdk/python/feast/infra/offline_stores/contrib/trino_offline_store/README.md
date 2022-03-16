# TODO - Validate these instructions

# Feast Trino Support

This plugin adds Trino support for Offline Store.  

## Version compatibilities
The feast-trino plugin is tested on the following versions of python [3.7, 3.8, 3.9]

Here is also how the current feast-trino plugin has been tested against different versions of Feast and Trino

| Feast-trino | Feast                   | Trino |
|-------------|-------------------------|-------|
| 1.0.*       | From 0.15.\* to 0.16.\* | 364   |

## Quickstart

#### Install feast

```shell
pip install feast
```

#### Create a feature repository

```shell
feast init feature_repo
```

#### Edit `feature_store.yaml`

set `offline_store` type to be `feast_trino.TrinoOfflineStore`

```yaml
project: feature_repo
registry: data/registry.db
provider: local
offline_store:
    type: feast_trino.trino.TrinoOfflineStore
    host: localhost
    port: 8080
    catalog: memory
    connector:
        type: memory
online_store:
    path: data/online_store.db
```

#### Create Trino Table
<!-- TODO -->

#### Edit `feature_repo/example.py`

```python
# This is an example feature definition file using Trino source
from datetime import timedelta

import pandas as pd
from google.protobuf.duration_pb2 import Duration
from feast import Entity, Feature, FeatureView, ValueType, FeatureStore

from feast_trino.connectors.upload import upload_pandas_dataframe_to_trino
from feast_trino import TrinoSource
from feast_trino.trino_utils import Trino

store = FeatureStore(repo_path="feature_repo")

client = Trino(
    user="user",
    catalog=store.config.offline_store.catalog,
    host=store.config.offline_store.host,
    port=store.config.offline_store.port,
)
client.execute_query("CREATE SCHEMA IF NOT EXISTS feast")
client.execute_query("DROP TABLE IF EXISTS feast.driver_stats")

input_df = pd.read_parquet("./feature_repo/data/driver_stats.parquet")
upload_pandas_dataframe_to_trino(
    client=client,
    df=input_df,
    table_ref="feast.driver_stats",
    connector_args={"type": "memory"},
)

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_hourly_stats = TrinoSource(
    name="driver_hourly_stats_src",
    event_timestamp_column="event_timestamp",
    table_ref="feast.driver_stats",
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
    ttl=Duration(seconds=86400 * 1),
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)
store.apply([driver, driver_hourly_stats_view])

# Run an historical retrieval query
query_date = str(input_df["event_timestamp"].max() - timedelta(days=1))
output_df = store.get_historical_features(
    entity_df=f"""
    SELECT
        1004 AS driver_id,
        TIMESTAMP '{query_date}' AS event_timestamp
    """,
    features=["driver_hourly_stats:conv_rate"]
).to_df()
print(output_df.head())
```

#### Apply the feature definitions and run a query

```shell
python feature_repo/example.py
```
