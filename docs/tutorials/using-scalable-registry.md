---
description: >-
  Tutorial on how to use the SQL registry for scalable registry updates
---

# Using Scalable Registry

## Overview

By default, the registry Feast uses a file-based registry implementation, which stores the protobuf representation of the registry as a serialized file. This registry file can be stored in a local file system, or in cloud storage (in, say, S3 or GCS).

However, there's inherent limitations with a file-based registry, since changing a single field in the registry requires re-writing the whole registry file. With multiple concurrent writers, this presents a risk of data loss, or bottlenecks writes to the registry since all changes have to be serialized (e.g. when running materialization for multiple feature views or time ranges concurrently).

An alternative to the file-based registry is the [SQLRegistry](https://rtd.feast.dev/en/latest/feast.infra.registry_stores.html#feast.infra.registry_stores.sql.SqlRegistry) which ships with Feast. This implementation stores the registry in a relational database, and allows for changes to individual objects atomically.
Under the hood, the SQL Registry implementation uses [SQLAlchemy](https://docs.sqlalchemy.org/en/14/) to abstract over the different databases. Consequently, any [database supported](https://docs.sqlalchemy.org/en/14/core/engines.html#supported-databases) by SQLAlchemy can be used by the SQL Registry.
The following databases are supported and tested out of the box:
- PostgreSQL
- MySQL
- Sqlite

Feast can use the SQL Registry via a config change in the feature_store.yaml file. An example of how to configure this would be:

```yaml
project: <your project name>
provider: <provider name>
online_store: redis
offline_store: file
registry:
    registry_type: sql
    path: postgresql://postgres:mysecretpassword@127.0.0.1:55001/feast
```

Specifically, the registry_type needs to be set to sql in the registry config block. On doing so, the path should refer to the [Database URL](https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls) for the database to be used, as expected by SQLAlchemy. No other additional commands are currently needed to configure this registry.

There are some things to note about how the SQL registry works:
- Once instantiated, the Registry ensures the tables needed to store data exist, and creates them if they do not.
- Upon tearing down the feast project, the registry ensures that the tables are dropped from the database.
- The schema for how data is laid out in tables can be found . It is intentionally simple, storing the serialized protobuf versions of each Feast object keyed by its name.

## Example Usage: Concurrent materialization
The SQL Registry should be used when materializing feature views concurrently to ensure correctness of data in the registry. This can be achieved by simply running feast materialize or feature_store.materialize multiple times using a correctly configured feature_store.yaml. This will make each materialization process talk to the registry database concurrently, and ensure the metadata updates are serialized.