# Automated upgrades for Feast 0.20+

## Overview

Starting with feast version 0.22, the feast cli supports a new command, `repo-upgrade` to automate the process of upgrading feast repositories to use the latest supported API.

The upgrade command aims to change the Feast API used in the feature repo to the latest version offered by Feast. When running the command, the feast CLI analyzes the source code in the feature repo files using [bowler](https://pybowler.io/), and attempted to rewrite the files in a best-effort way. It's possible for there to be parts of the API that are not upgraded automatically. To make this command better, we welcome feedback and code contributions to the upgrade implementation!


## Usage

At the root of a feature repo, you can run `feast repo-upgrade`. By default, the CLI only echos the changes it's planning on making, and does not modify any files in place. If the changes look reasonably, you can specify the `--write` flag to have the changes be written out to disk.

An example:
```bash
$ feast repo-upgrade --write
/Users/achal/tecton/feast/.direnv/python-3.8.12/lib/python3.8/site-packages/pkg_resources/_vendor/packaging/specifiers.py:273: DeprecationWarning: Creating a LegacyVersion has been deprecated and will be removed in the next major release
--- /Users/achal/tecton/feast/prompt_dory/example.py
+++ /Users/achal/tecton/feast/prompt_dory/example.py
@@ -13,7 +13,6 @@
     path="/Users/achal/tecton/feast/prompt_dory/data/driver_stats.parquet",
     event_timestamp_column="event_timestamp",
     created_timestamp_column="created",
-    date_partition_column="created"
 )

 # Define an entity for the driver. You can think of entity as a primary key used to
--- /Users/achal/tecton/feast/prompt_dory/example.py
+++ /Users/achal/tecton/feast/prompt_dory/example.py
@@ -3,7 +3,7 @@
 from google.protobuf.duration_pb2 import Duration
 import pandas as pd

-from feast import Entity, Feature, FeatureView, FileSource, ValueType, FeatureService, OnDemandFeatureView
+from feast import Entity, FeatureView, FileSource, ValueType, FeatureService, OnDemandFeatureView

 # Read data from parquet files. Parquet is convenient for local development mode. For
 # production, you can use your favorite DWH, such as BigQuery. See Feast documentation
--- /Users/achal/tecton/feast/prompt_dory/example.py
+++ /Users/achal/tecton/feast/prompt_dory/example.py
@@ -4,6 +4,7 @@
 import pandas as pd

 from feast import Entity, Feature, FeatureView, FileSource, ValueType, FeatureService, OnDemandFeatureView
+from feast import Field

 # Read data from parquet files. Parquet is convenient for local development mode. For
 # production, you can use your favorite DWH, such as BigQuery. See Feast documentation
--- /Users/achal/tecton/feast/prompt_dory/example.py
+++ /Users/achal/tecton/feast/prompt_dory/example.py
@@ -28,9 +29,9 @@
     entities=["driver_id"],
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