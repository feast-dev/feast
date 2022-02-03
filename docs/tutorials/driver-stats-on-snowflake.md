---
description: >-
  Initial demonstration of Snowflake as an offline store with Feast, using the Snowflake demo template.
---

# Drivers stats on Snowflake

In the steps below, we will set up a sample Feast project that leverages Snowflake
as an offline store.

Starting with data in a Snowflake table, we will register that table to the feature store and define features associated with the columns in that table. From there, we will generate historical training data based on those feature definitions and then materialize the latest feature values into the online store. Lastly, we will retrieve the materialized feature values.

Our template will generate new data containing driver statistics. From there, we will show you code snippets that will call to the offline store for generating training datasets, and then the code for calling the online store to serve you the latest feature values to serve models in production.

## Snowflake Offline Store Example

#### Install feast-snowflake

```shell
pip install 'feast[snowflake]'
```

#### Get a Snowflake Trial Account (Optional)

[Snowflake Trial Account](http://trial.snowflake.com)

#### Create a feature repository

```shell
feast init -t snowflake {feature_repo_name}
Snowflake Deployment URL (exclude .snowflakecomputing.com):
Snowflake User Name::
Snowflake Password::
Snowflake Role Name (Case Sensitive)::
Snowflake Warehouse Name (Case Sensitive)::
Snowflake Database Name (Case Sensitive)::
Should I upload example data to Snowflake (overwrite table)? [Y/n]: Y
cd {feature_repo_name}
```

The following files will automatically be created in your project folder:

* feature_store.yaml -- This is your main configuration file
* driver_repo.py -- This is your main feature definition file
* test.py -- This is a file to test your feature store configuration

#### Inspect `feature_store.yaml`

Here you will see the information that you entered. This template will use Snowflake as an offline store and SQLite as the online store. The main thing to remember is by default, Snowflake objects have ALL CAPS names unless lower case was specified.

{% code title="feature_store.yaml" %}
```yaml
project: ...
registry: ...
provider: local
offline_store:
    type: snowflake.offline
    account: SNOWFLAKE_DEPLOYMENT_URL #drop .snowflakecomputing.com
    user: USERNAME
    password: PASSWORD
    role: ROLE_NAME #case sensitive
    warehouse: WAREHOUSE_NAME #case sensitive
    database: DATABASE_NAME #case cap sensitive
```
{% endcode %}

#### Run our test python script `test.py`

```shell
python test.py
```

## What we did in `test.py`

#### Initialize our Feature Store
{% code title="test.py" %}
```python
from datetime import datetime, timedelta

import pandas as pd
from driver_repo import driver, driver_stats_fv

from feast import FeatureStore

fs = FeatureStore(repo_path=".")

fs.apply([driver, driver_stats_fv])
```
{% endcode %}

#### Create a dummy training dataframe, then call our offline store to add additional columns
{% code title="test.py" %}
```python
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

features = ["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"]

training_df = fs.get_historical_features(
    features=features, entity_df=entity_df
).to_df()
```
{% endcode %}

#### Materialize the latest feature values into our online store
{% code title="test.py" %}
```python
fs.materialize_incremental(end_date=datetime.now())
```
{% endcode %}

#### Retrieve the latest values from our online store based on our entity key
{% code title="test.py" %}
```python
online_features = fs.get_online_features(
    features=features, entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}],
).to_dict()
```
{% endcode %}
