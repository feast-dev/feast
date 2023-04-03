# Automated upgrades for Feast 0.20+

## Overview

Starting with Feast 0.20, the APIs of many core objects (e.g. feature views and entities) have been changed.
For example, many parameters have been renamed.
These changes were made in a backwards-compatible fashion; existing Feast repositories will continue to work until Feast 0.23, without any changes required.
However, Feast 0.24 will fully deprecate all of the old parameters, so in order to use Feast 0.24+ users must modify their Feast repositories.

There are currently deprecation warnings that indicate to users exactly how to modify their repos.
In order to make the process somewhat easier, Feast 0.23 also introduces a new CLI command, `repo-upgrade`, that will partially automate the process of upgrading Feast repositories.

The upgrade command aims to automatically modify the object definitions in a feature repo to match the API required by Feast 0.24+. When running the command, the Feast CLI analyzes the source code in the feature repo files using [bowler](https://pybowler.io/), and attempted to rewrite the files in a best-effort way. It's possible for there to be parts of the API that are not upgraded automatically.

The `repo-upgrade` command is specifically meant for upgrading Feast repositories that were initially created in versions 0.23 and below to be compatible with versions 0.24 and above.
It is not intended to work for any future upgrades.

## Usage

At the root of a feature repo, you can run `feast repo-upgrade`. By default, the CLI only echos the changes it's planning on making, and does not modify any files in place. If the changes look reasonably, you can specify the `--write` flag to have the changes be written out to disk.

An example:
```bash
$ feast repo-upgrade --write
--- /Users/achal/feast/prompt_dory/example.py
+++ /Users/achal/feast/prompt_dory/example.py
@@ -13,7 +13,6 @@
     path="/Users/achal/feast/prompt_dory/data/driver_stats.parquet",
     event_timestamp_column="event_timestamp",
     created_timestamp_column="created",
-    date_partition_column="created"
 )

 # Define an entity for the driver. You can think of entity as a primary key used to
--- /Users/achal/feast/prompt_dory/example.py
+++ /Users/achal/feast/prompt_dory/example.py
@@ -3,7 +3,7 @@
 from google.protobuf.duration_pb2 import Duration
 import pandas as pd

-from feast import Entity, Feature, FeatureView, FileSource, ValueType, FeatureService, OnDemandFeatureView
+from feast import Entity, FeatureView, FileSource, ValueType, FeatureService, OnDemandFeatureView

 # Read data from parquet files. Parquet is convenient for local development mode. For
 # production, you can use your favorite DWH, such as BigQuery. See Feast documentation
--- /Users/achal/feast/prompt_dory/example.py
+++ /Users/achal/feast/prompt_dory/example.py
@@ -4,6 +4,7 @@
 import pandas as pd

 from feast import Entity, Feature, FeatureView, FileSource, ValueType, FeatureService, OnDemandFeatureView
+from feast import Field

 # Read data from parquet files. Parquet is convenient for local development mode. For
 # production, you can use your favorite DWH, such as BigQuery. See Feast documentation
--- /Users/achal/feast/prompt_dory/example.py
+++ /Users/achal/feast/prompt_dory/example.py
@@ -28,9 +29,9 @@
     entities=[driver_id],
     ttl=Duration(seconds=86400 * 365),
     features=[
-        Feature(name="conv_rate", dtype=ValueType.FLOAT),
-        Feature(name="acc_rate", dtype=ValueType.FLOAT),
-        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
+        Field(name="conv_rate", dtype=ValueType.FLOAT),
+        Field(name="acc_rate", dtype=ValueType.FLOAT),
+        Field(name="avg_daily_trips", dtype=ValueType.INT64),
     ],
     online=True,
     batch_source=driver_hourly_stats,
```
---
To write these changes out, you can run the same command with the `--write` flag:
```bash
$ feast repo-upgrade  --write
```

You should see the same output, but also see the changes reflected in your feature repo on disk.