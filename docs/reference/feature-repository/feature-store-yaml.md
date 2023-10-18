# feature_store.yaml

## Overview

`feature_store.yaml` is used to configure a feature store. The file must be located at the root of a [feature repository](./). An example `feature_store.yaml` is shown below:

{% code title="feature_store.yaml" %}
```yaml
project: loyal_spider
registry: data/registry.db
provider: local
online_store:
    type: sqlite
    path: data/online_store.db
```
{% endcode %}

## Options

The following top-level configuration options exist in the `feature_store.yaml` file.

* **provider**  — Configures the environment in which Feast will deploy and operate.
* **registry** — Configures the location of the feature registry.
* **online_store** — Configures the online store.
* **offline_store** — Configures the offline store.
* **project** — Defines a namespace for the entire feature store. Can be used to isolate multiple deployments in a single installation of Feast. Should only contain letters, numbers, and underscores.
* **engine** - Configures the batch materialization engine.

Please see the [RepoConfig](https://rtd.feast.dev/en/latest/#feast.repo_config.RepoConfig) API reference for the full list of configuration options.
