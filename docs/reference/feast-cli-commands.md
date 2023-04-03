# Feast CLI reference

## Overview

The Feast CLI comes bundled with the Feast Python package. It is immediately available after [installing Feast](../how-to-guides/feast-snowflake-gcp-aws/install-feast.md).

```text
Usage: feast [OPTIONS] COMMAND [ARGS]...

  Feast CLI

  For more information, see our public docs at https://docs.feast.dev/

  For any questions, you can reach us at https://slack.feast.dev/

Options:
  -c, --chdir TEXT  Switch to a different feature repository directory before
                    executing the given subcommand.

  --help            Show this message and exit.

Commands:
  apply                    Create or update a feature store deployment
  entities                 Access entities
  feature-views            Access feature views
  init                     Create a new Feast repository
  materialize              Run a (non-incremental) materialization job to...
  materialize-incremental  Run an incremental materialization job to ingest...
  registry-dump            Print contents of the metadata registry
  teardown                 Tear down deployed feature store infrastructure
  version                  Display Feast SDK version
```

## Global Options

The Feast CLI provides one global top-level option that can be used with other commands

**chdir \(-c, --chdir\)**

This command allows users to run Feast CLI commands in a different folder from the current working directory.

```text
feast -c path/to/my/feature/repo apply
```

## Apply

Creates or updates a feature store deployment

```bash
feast apply
```

**What does Feast apply do?**

1. Feast will scan Python files in your feature repository and find all Feast object definitions, such as feature views, entities, and data sources.
2. Feast will validate your feature definitions (e.g. for uniqueness of features)
3. Feast will sync the metadata about Feast objects to the registry. If a registry does not exist, then it will be instantiated. The standard registry is a simple protobuf binary file that is stored on disk \(locally or in an object store\).
4. Feast CLI will create all necessary feature store infrastructure. The exact infrastructure that is deployed or configured depends on the `provider` configuration that you have set in `feature_store.yaml`. For example, setting `local` as your provider will result in a `sqlite` online store being created. 

{% hint style="warning" %}
`feast apply` \(when configured to use cloud provider like `gcp` or `aws`\) will create cloud infrastructure. This may incur costs.
{% endhint %}

## Entities

List all registered entities

```text
feast entities list
```

```text
NAME       DESCRIPTION    TYPE
driver_id  driver id      ValueType.INT64
```

## Feature views

List all registered feature views

```text
feast feature-views list
```

```text
NAME                 ENTITIES
driver_hourly_stats  ['driver_id']
```

## Init

Creates a new feature repository

```text
feast init my_repo_name
```

```text
Creating a new Feast repository in /projects/my_repo_name.
```

```text
.
├── data
│   └── driver_stats.parquet
├── example.py
└── feature_store.yaml
```

It's also possible to use other templates

```text
feast init -t gcp my_feature_repo
```

or to set the name of the new project

```text
feast init -t gcp my_feature_repo
```

## Materialize

Load data from feature views into the online store between two dates

```bash
feast materialize 2020-01-01T00:00:00 2022-01-01T00:00:00
```

Load data for specific feature views into the online store between two dates

```text
feast materialize -v driver_hourly_stats 2020-01-01T00:00:00 2022-01-01T00:00:00
```

```text
Materializing 1 feature views from 2020-01-01 to 2022-01-01

driver_hourly_stats:
100%|██████████████████████████| 5/5 [00:00<00:00, 5949.37it/s]
```

## Materialize incremental

Load data from feature views into the online store, beginning from either the previous `materialize` or `materialize-incremental` end date, or the beginning of time.

```text
feast materialize-incremental 2022-01-01T00:00:00
```

## Teardown

Tear down deployed feature store infrastructure

```text
feast teardown
```

## Version

Print the current Feast version

```text
feast version
```

