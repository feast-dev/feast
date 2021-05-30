# Quickstart

## Setting up Feast

Install the Feast SDK and CLI using pip:

```bash
pip install feast
```

## Create a new repository

Bootstrap a new feature repository using `feast init` and a built-in template:

{% code %}
```bash
feast init feature_repo
cd feature_repo
```
{% endcode %}

Register the feature definitions inside this repository by running `feast apply` from the command line:

{% code %}
```bash
feast apply
```
{% endcode %}

## Generating training data

{% code %}
```python
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


store = FeatureStore(repo_path="")

training_df = store.get_historical_features(
    entity_df=entity_df,
    feature_refs=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
).to_df()

training_df.head()

```
{% endcode %}

Load features into the online store using `feast materialize`:

{% code %}
```bash
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
feast materialize-incremental $CURRENT_TIME
```
{% endcode %}

## Fetching feature vectors for inference

{% code %}
```python
from pprint import pprint

from feast import FeatureStore

store = FeatureStore(repo_path="")

feature_vector = store.get_online_features(
    feature_refs=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
    entity_rows=[{"driver_id": 1001}],
).to_dict()

pprint(feature_vector)

```
{% endcode %}

```text
{
    'driver_id': [1001],
    'driver_hourly_stats__conv_rate': [0.49274],
    'driver_hourly_stats__acc_rate': [0.92743],
    'driver_hourly_stats__avg_daily_trips': [72],
}
```

## Next steps

This quickstart covered the essential workflows of using Feast in your local environment. The next step is to `pip install "feast[gcp]"` and set `provider="gcp"` in your `feature_store.yaml` file and push your work to production deployment. You can also use the `feast init -t gcp` command in the CLI to initialize a feature repository with example features in the GCP environment.

* See [Create a feature repository](how-to-guides/create-a-feature-repository.md) for more information on the workflows we covered.
* Join our[ Slack group](https://slack.com) to talk to other Feast users and the maintainers!