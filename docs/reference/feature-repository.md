# Feature repository

Feast manages two important sets of configuration: feature definitions, and configuration about how to run the feature store. With Feast, this configuration can be written declaratively and stored as code in a central location. This central location is called a feature repository, and it's essentially just a directory that contains some code files.

The feature repository is the declarative source of truth for what the desired state of a feature store should be. The Feast CLI uses the feature repository to configure your infrastructure, e.g., migrate tables.

## What is a feature repository?

A feature repository consists of:

* A collection of Python files containing feature declarations.
* A `feature_store.yaml` file containing infrastructural configuration.
* A `.feastignore` file containing paths in the feature repository to ignore.

{% hint style="info" %}
Typically, users store their feature repositories in a Git repository, especially when working in teams. However, using Git is not a requirement.
{% endhint %}

## Structure of a feature repository

The structure of a feature repository is as follows:

* The root of the repository should contain a `feature_store.yaml` file and may contain a `.feastignore` file.
* The repository should contain Python files that contain feature definitions. 
* The repository can contain other files as well, including documentation and potentially data files.

An example structure of a feature repository is shown below:

```text
$ tree -a
.
├── data
│   └── driver_stats.parquet
├── driver_features.py
├── feature_store.yaml
└── .feastignore

1 directory, 4 files
```

A couple of things to note about the feature repository:

* Feast reads _all_ Python files recursively when `feast apply` is ran, including subdirectories, even if they don't contain feature definitions.
* It's recommended to add `.feastignore` and add paths to all imperative scripts if you need to store them inside the feature registry.

## The feature\_store.yaml configuration file

The configuration for a feature store is stored in a file named `feature_store.yaml` , which must be located at the root of a feature repository. An example `feature_store.yaml` file is shown below:

{% code title="feature\_store.yaml" %}
```yaml
project: my_feature_repo_1
registry: data/metadata.db
provider: local
online_store:
    path: data/online_store.db
```
{% endcode %}

The `feature_store.yaml` file configures how the feature store should run. See [feature\_store.yaml](feature-store-yaml.md) for more details.

## The .feastignore file

This file contains paths that should be ignored when running `feast apply`. An example `.feastignore` is shown below:

{% code title=".feastignore" %}
```text
# Ignore virtual environment
venv

# Ignore a specific Python file
scripts/foo.py

# Ignore all Python files directly under scripts directory
scripts/*.py

# Ignore all "foo.py" anywhere under scripts directory
scripts/**/foo.py
```
{% endcode %}

See [.feastignore](feast-ignore.md) for more details.

## Feature definitions

A feature repository can also contain one or more Python files that contain feature definitions. An example feature definition file is shown below:

{% code title="driver\_features.py" %}
```python
from datetime import timedelta

from feast import BigQuerySource, Entity, Feature, FeatureView, Field
from feast.types import Float32, Int64, String

driver_locations_source = BigQuerySource(
    table="rh_prod.ride_hailing_co.drivers",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

driver = Entity(
    name="driver",
    description="driver id",
)

driver_locations = FeatureView(
    name="driver_locations",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="lat", dtype=Float32),
        Field(name="lon", dtype=String),
        Field(name="driver", dtype=Int64),
    ],
    source=driver_locations_source,
)
```
{% endcode %}

To declare new feature definitions, just add code to the feature repository, either in existing files or in a new file. For more information on how to define features, see [Feature Views](../getting-started/concepts/feature-view.md).

### Next steps

* See [Create a feature repository](../how-to-guides/feast-snowflake-gcp-aws/README.md) to get started with an example feature repository.
* See [feature\_store.yaml](feature-store-yaml.md), [.feastignore](feast-ignore.md) or [Feature Views](../getting-started/concepts/feature-view.md) for more information on the configuration files that live in a feature registry.

