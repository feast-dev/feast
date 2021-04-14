# Feature Repository

Feast stores its configuration as code. This code These feature definitions are defined as code \(Python objects and stored within a feature repository.

A feature repository is the declarative source of truth for what the desired state of a feature store should be. The Feast CLI uses a feature repository to configure your infrastructure, e.g., migrate tables.

## What is a feature repository?

A feature repository consists of

* A collection of Python files containing feature declarations
* A `feature_store.yaml` file containing infrastructural configuration.

{% hint style="info" %}
Typically, users store their feature repositories in a Git repository. Using Git is not a requirement.
{% endhint %}

## Structure of a feature repository

```text
$ tree
.
├── data
│   └── driver_stats.parquet
├── driver_features.py
└── feature_store.yaml

1 directory, 3 files
```

## The feature\_store.yaml configuration file

The configuration for a feature store is stored in a file named `feature_store.yaml` at the root of a feature repository.

{% code title="feature\_store.yaml" %}
```yaml
project: my_feature_repo_1
registry: data/metadata.db
provider: local
online_store:
    path: data/online_store.db
```
{% endcode %}

* **Project**: A unique identifier for your project. The project name is used to isolate multiple feature stores when deploying to the same infrastructure. 
* **Registry**: The registry is used to persist feature definitions and related metadata. The registry is updated when the `apply` command is run to update infrastructure. The registry is read when users try to build training datasets or try to read from an online store. A registry can either be a local file or a file on an object store \(if it needs to be shared\).
* **Provider**: The provider defines the target environment that will be used to configure your infrastructure. By selecting `local`, Feast will configure local infrastructure for storing and serving features. By selecting `gcp`, Feast will configure cloud infrastructure for storing and serving features.
* **Online Store**: This option allows teams to configure the destination online store that should be used to store and serve online features. The type of online store that can be selected and configured depends on the `provider`. A provider may allow more than one online store to be configured.

## Feature definitions

A feature repository can also contain one or more Python files that contain feature definitions

{% code title="driver\_features.py" %}
```python
from datetime import timedelta

from feast import BigQuerySource, Entity, Feature, FeatureView, ValueType

driver_locations_source = BigQuerySource(
    table_ref="rh_prod.ride_hailing_co.drivers",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)

driver = Entity(
    name="driver",
    value_type=ValueType.INT64,
    description="driver id",
)

driver_locations = FeatureView(
    name="driver_locations",
    entities=["driver"],
    ttl=timedelta(days=1),
    features=[
        Feature(name="lat", dtype=ValueType.FLOAT),
        Feature(name="lon", dtype=ValueType.STRING),
    ],
    input=driver_locations_source,
)
```
{% endcode %}

The way to declare feature definitions \(Feature Views, Entities, Data Sources\) in a feature repository is to simply write Python code to instantiate the objects.

There are no restrictions on how Python feature definition files can be named, as long as they have valid Python module names \(so no dashes\).

Have a look at [Create a feature repository](../how-to-guides/create-a-feature-repository.md) to get started with an example feature repository

