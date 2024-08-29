# Feast CLI reference

## Overview

The Feast CLI comes bundled with the Feast Python package. It is immediately available after [installing Feast](../how-to-guides/feast-snowflake-gcp-aws/install-feast.md).

```text
Usage: feast [OPTIONS] COMMAND [ARGS]...

  Feast CLI

  For more information, see our public docs at https://docs.feast.dev/

Options:
  -c, --chdir TEXT               Switch to a different feature repository
                                 directory before executing the given
                                 subcommand.
  --log-level TEXT               The logging level. One of DEBUG, INFO,
                                 WARNING, ERROR, and CRITICAL (case-
                                 insensitive).
  -f, --feature-store-yaml TEXT  Override the directory where the CLI should
                                 look for the feature_store.yaml file.
  --help                         Show this message and exit.

Commands:
  apply                    Create or update a feature store deployment
  data-sources             Access data sources
  endpoint                 Display feature server endpoints
  entities                 Access entities
  feature-services         Access feature services
  feature-views            Access feature views
  init                     Create a new Feast repository
  listen                   Start a gRPC feature server to ingest...
  materialize              Run a (non-incremental) materialization job to...
  materialize-incremental  Run an incremental materialization job to...
  on-demand-feature-views  [Experimental] Access on demand feature views
  permissions              Access permissions
  plan                     Create or update a feature store deployment
  projects                 Access projects
  registry-dump            Print contents of the metadata registry
  saved-datasets           [Experimental] Access saved datasets
  serve                    Start a feature server locally on a given port.
  serve_offline            Start a remote server locally on a given host,...
  serve_registry           Start a registry server locally on a given port.
  serve_transformations    [Experimental] Start a feature consumption...
  stream-feature-views     [Experimental] Access stream feature views
  tag                      [Experimental] Tag objects
  teardown                 Tear down deployed feature store infrastructure
  ui                       Shows the Feast UI over the current directory
  validate                 Perform validation of logged features...
  validation-references    [Experimental] Access validation references
  version                  Display Feast SDK version
```

## Global Options

The Feast CLI provides three global top-level options that can be used with other commands

**chdir \(-c, --chdir\)**

This command allows users to run Feast CLI commands in a different folder from the current working directory.

**log-level \(--log-level\)**

This command allows users to change the logging level.

**feature-store-yaml \(-f, --feature-store-yaml\)**

This command allows users to override the directory where the CLI should look for the `feature_store.yaml` file.
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

Options:
  --tags TEXT  Filter by tags (e.g. --tags 'key:value' --tags 'key:value,
               key:value, ...'). Items return when ALL tags match.
```

```text
NAME       DESCRIPTION    TYPE
driver_id  driver id      ValueType.INT64
```

## Feature views

List all registered feature views

```text
feast feature-views list

Options:
  --tags TEXT  Filter by tags (e.g. --tags 'key:value' --tags 'key:value,
               key:value, ...'). Items return when ALL tags match.
```

```text
NAME                 ENTITIES    TYPE
driver_hourly_stats  {'driver'}  FeatureView
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

## Permissions

### List permissions
List all registered permission

```text
feast permissions list

Options:
  --tags TEXT    Filter by tags (e.g. --tags 'key:value' --tags 'key:value,
                 key:value, ...'). Items return when ALL tags match.
  -v, --verbose  Print the resources matching each configured permission
```

```text
+-----------------------+-------------+-----------------------+-----------+----------------+-------------------------+
| NAME                  | TYPES       | NAME_PATTERN          | ACTIONS   | ROLES          | REQUIRED_TAGS           |
+=======================+=============+=======================+===========+================+================+========+
| reader_permission1234 | FeatureView | transformed_conv_rate | DESCRIBE  | reader         | -                       |
+-----------------------+-------------+-----------------------+-----------+----------------+-------------------------+
| writer_permission1234 | FeatureView | transformed_conv_rate | CREATE    | writer         | -                       |
+-----------------------+-------------+-----------------------+-----------+----------------+-------------------------+
| special               | FeatureView | special.*             | DESCRIBE  | admin          | test-key2 : test-value2 |
|                       |             |                       | UPDATE    | special-reader | test-key : test-value   |
+-----------------------+-------------+-----------------------+-----------+----------------+-------------------------+
```

`verbose` option describes the resources matching each configured permission: 

```text
feast permissions list -v
```

```text
Permissions:

permissions
├── reader_permission1234 ['reader']
│   └── FeatureView: none
└── writer_permission1234 ['writer']
    ├── FeatureView: none
    │── OnDemandFeatureView: ['transformed_conv_rate_fresh', 'transformed_conv_rate']
    └── BatchFeatureView: ['driver_hourly_stats', 'driver_hourly_stats_fresh']
```

### Describe a permission
Describes the provided permission

```text
feast permissions describe permission-name
name: permission-name
types:
- FEATURE_VIEW
namePattern: transformed_conv_rate
requiredTags:
  required1: required-value1
  required2: required-value2
actions:
- DESCRIBE
policy:
  roleBasedPolicy:
    roles:
    - reader
tags:
  key1: value1
  key2: value2

```

### List of the configured roles
List all the configured roles

```text
feast permissions list-roles

Options:
  --verbose Print the resources and actions permitted to each configured
            role
```

```text
ROLE NAME
admin
reader
writer
```

`verbose` option describes the resources and actions permitted to each managed role: 

```text
feast permissions list-roles -v
```

```text            
ROLE NAME          RESOURCE NAME               RESOURCE TYPE    PERMITTED ACTIONS
admin              driver_hourly_stats_source  FileSource       CREATE
                                                                DELETE
                                                                QUERY_OFFLINE
                                                                QUERY_ONLINE
                                                                DESCRIBE
                                                                UPDATE
admin              vals_to_add                 RequestSource    CREATE
                                                                DELETE
                                                                QUERY_OFFLINE
                                                                QUERY_ONLINE
                                                                DESCRIBE
                                                                UPDATE
admin              driver_stats_push_source    PushSource       CREATE
                                                                DELETE
                                                                QUERY_OFFLINE
                                                                QUERY_ONLINE
                                                                DESCRIBE
                                                                UPDATE
admin              driver_hourly_stats_source  FileSource       CREATE
                                                                DELETE
                                                                QUERY_OFFLINE
                                                                QUERY_ONLINE
                                                                DESCRIBE
                                                                UPDATE
admin              vals_to_add                 RequestSource    CREATE
                                                                DELETE
                                                                QUERY_OFFLINE
                                                                QUERY_ONLINE
                                                                DESCRIBE
                                                                UPDATE
admin              driver_stats_push_source    PushSource       CREATE
                                                                DELETE
                                                                QUERY_OFFLINE
                                                                QUERY_ONLINE
                                                                DESCRIBE
                                                                UPDATE
reader             driver_hourly_stats         FeatureView      DESCRIBE
reader             driver_hourly_stats_fresh   FeatureView      DESCRIBE
...
```

## Tag
Update tags for the following objects types:
- data-source
- entity
- feature-service
- feature-view
- on-demand-feature-view
- permission
- project
- saved-dataset
- stream-feature-view
- validation-reference

```text
% feast tag --help
Usage: feast tag [OPTIONS] OBJECT_TYPE NAME TAGS...

  [Experimental] Tag objects

  *  A tag key and value must begin with a letter or number, and may contain
  letters, numbers, hyphens, dots, and underscores, up to 63 characters each.

  *  Optionally, the key can begin with a DNS subdomain prefix and a single
  '/', like example.com/my-app. The 'feast.dev/' prefix is reserved.

  Examples:

      # Update data source 'foo' with the tag 'test' and the value 'true'.

      feast tag data-source foo test:true

      # Update feature view 'foo' by removing a tag named 'bar' if it exists.
      Does not require the --overwrite flag.

      feast tag feature-view foo bar-

Options:
  --overwrite  If true, allow tags to be overwritten, otherwise reject tag
               updates that overwrite existing tags.
  --help       Show this message and exit.
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

