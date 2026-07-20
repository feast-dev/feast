# SQL Registry

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
    cache_ttl_seconds: 60
    sqlalchemy_config_kwargs:
        echo: false
        pool_pre_ping: true
```

Specifically, the registry_type needs to be set to sql in the registry config block. On doing so, the path should refer to the [Database URL](https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls) for the database to be used, as expected by SQLAlchemy. No other additional commands are currently needed to configure this registry.

Should you choose to use a database technology that is compatible with one of
Feast's supported registry backends, but which speaks a different dialect (e.g.
`cockroachdb`, which is compatible with `postgres`) then some further
intervention may be required on your part.

`SQLAlchemy`, used by the registry, may not be able to detect your database
version without first updating your DSN scheme to the appropriate
[DBAPI/dialect combination](https://docs.sqlalchemy.org/en/14/glossary.html#term-DBAPI).
When this happens, your database is likely using what is referred to as an
[external dialect](https://docs.sqlalchemy.org/en/14/dialects/#external-dialects)
in `SQLAlchemy` terminology. See your database's documentation for examples on
how to set its scheme in the Database URL.

`Psycopg`, which is the database library leveraged by the online and offline
stores, is not impacted by the need to speak a particular dialect, and so the
following only applies to the registry.

If you are not running Feast in a container, to accomodate `SQLAlchemy`'s need
to speak an external dialect, install additional Python modules like we do as
follows using `cockroachdb` for example:

```shell
pip install sqlalchemy-cockroachdb
```

If you are running Feast in a container, you will need to create a custom image
like we do as follows, again using `cockroachdb` as an example:

```shell
cat <<'EOF' >Dockerfile
ARG QUAY_IO_FEASTDEV_FEATURE_SERVER
FROM quay.io/feastdev/feature-server:${QUAY_IO_FEASTDEV_FEATURE_SERVER}
ARG PYPI_ORG_PROJECT_SQLALCHEMY_COCKROACHDB
RUN pip install -I --no-cache-dir \
      sqlalchemy-cockroachdb==${PYPI_ORG_PROJECT_SQLALCHEMY_COCKROACHDB}
EOF

export QUAY_IO_FEASTDEV_FEATURE_SERVER=0.27.1
export PYPI_ORG_PROJECT_SQLALCHEMY_COCKROACHDB=1.4.4

docker build \
  --build-arg QUAY_IO_FEASTDEV_FEATURE_SERVER \
  --build-arg PYPI_ORG_PROJECT_SQLALCHEMY_COCKROACHDB \
  --tag ${MY_REGISTRY}/feastdev/feature-server:${QUAY_IO_FEASTDEV_FEATURE_SERVER} .
```

If you are running Feast in Kubernetes, set the `image.repository` and
`imagePullSecrets` Helm values accordingly to utilize your custom image.

There are some things to note about how the SQL registry works:
- Once instantiated, the Registry ensures the tables needed to store data exist, and creates them if they do not.
- Upon tearing down the feast project, the registry ensures that the tables are dropped from the database.
- The schema for how data is laid out in tables can be found in the table definitions in [`sdk/python/feast/infra/registry/sql.py`](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/registry/sql.py). It is intentionally simple, storing the serialized protobuf versions of each Feast object keyed by its name.

## MySQL: serialized-proto columns use `LONGBLOB`

The registry stores each Feast object as a serialized protobuf in a binary
column. On MySQL these columns are created as `LONGBLOB` (up to 4 GB). Earlier
versions created them as `BLOB`, which caps at 64 KB — a single `FeatureView`
proto routinely exceeds that, so MySQL would silently truncate the write and the
registry would later fail to load with a protobuf `DecodeError` (for example,
`feast serve` failing to start). Other dialects (PostgreSQL, SQLite) were never
affected.

New deployments get the correct schema automatically — the registry creates its
tables as `LONGBLOB` on first use. When an existing MySQL/MariaDB registry still
has `BLOB` columns, the registry logs an error at startup listing the affected
columns (it does not refuse to start — a registry whose protos all fit in 64 KB
is unaffected). **Existing deployments are not migrated automatically**: the
registry only creates tables that do not already exist, and it has no
schema-migration step, so previously created `BLOB` columns remain `BLOB`. To
upgrade an existing MySQL registry, alter each serialized-proto column to
`LONGBLOB`, for example:

> ⚠️ **Run the migration carefully on a live registry.** A `BLOB`→`LONGBLOB`
> change is a column *data-type* change, which MySQL InnoDB performs with
> `ALGORITHM=COPY` — a full table rebuild under a metadata lock that blocks
> readers and writers for the duration (potentially minutes on a large table
> such as `feature_view_version_history`). `ALGORITHM=INPLACE` is **not**
> generally supported for this change and is rejected with
> `ER_ALTER_OPERATION_NOT_SUPPORTED_REASON` on most builds — do not rely on it.
>
> **Before running any `ALTER TABLE`:**
>
> 1. **Stop all `feast apply` and materialization jobs.** This is required, not
>    optional — a write of a `>64 KB` proto to a not-yet-widened `BLOB` column
>    truncates silently with no error, and concurrent writes also extend the
>    `ALTER`'s lock duration.
> 2. Confirm there are no active writers (e.g. `SHOW PROCESSLIST`).
> 3. Verify you have a backup of the registry database.
>
> Then, to minimize the lock window:
>
> - On large tables, or on managed MySQL (AWS RDS, Aurora) without shell access,
>   use an online schema-change tool —
>   [`pt-online-schema-change`](https://docs.percona.com/percona-toolkit/pt-online-schema-change.html)
>   (Percona Toolkit) or [`gh-ost`](https://github.com/github/gh-ost) — which
>   rebuild the table without a long-held lock. For small tables a plain
>   `ALTER TABLE` in the maintenance window is fine.
> - Apply one table at a time so a failure is easy to isolate and re-run.
> - Resume jobs only after all `ALTER TABLE` statements complete successfully.
> - Rollback is safe (revert `MODIFY ... BLOB`) **only** while no stored proto
>   exceeds 64 KB; otherwise a revert re-introduces truncation.

```sql
ALTER TABLE projects               MODIFY project_proto              LONGBLOB NOT NULL;
ALTER TABLE entities               MODIFY entity_proto               LONGBLOB NOT NULL;
ALTER TABLE data_sources           MODIFY data_source_proto          LONGBLOB NOT NULL;
ALTER TABLE feature_views          MODIFY materialized_intervals     LONGBLOB,
                                   MODIFY feature_view_proto         LONGBLOB NOT NULL,
                                   MODIFY user_metadata              LONGBLOB;
ALTER TABLE stream_feature_views   MODIFY feature_view_proto         LONGBLOB NOT NULL,
                                   MODIFY user_metadata              LONGBLOB;
ALTER TABLE on_demand_feature_views MODIFY feature_view_proto        LONGBLOB NOT NULL,
                                   MODIFY user_metadata              LONGBLOB;
ALTER TABLE label_views            MODIFY feature_view_proto         LONGBLOB NOT NULL,
                                   MODIFY user_metadata              LONGBLOB;
ALTER TABLE feature_services       MODIFY feature_service_proto      LONGBLOB NOT NULL;
ALTER TABLE saved_datasets         MODIFY saved_dataset_proto        LONGBLOB NOT NULL;
ALTER TABLE validation_references  MODIFY validation_reference_proto LONGBLOB NOT NULL;
ALTER TABLE managed_infra          MODIFY infra_proto                LONGBLOB NOT NULL;
ALTER TABLE permissions            MODIFY permission_proto           LONGBLOB NOT NULL;
-- LARGE TABLE: one row per versioned apply — likely the slowest ALTER. Use
-- pt-online-schema-change or gh-ost if this registry has significant history.
ALTER TABLE feature_view_version_history MODIFY feature_view_proto   LONGBLOB NOT NULL;
```

Any object whose proto already exceeded 64 KB before the upgrade may have been
stored truncated; re-run `feast apply` for those objects after altering the
columns so the full proto is rewritten.

## Example Usage: Concurrent materialization
The SQL Registry should be used when materializing feature views concurrently to ensure correctness of data in the registry. This can be achieved by simply running feast materialize or feature_store.materialize multiple times using a correctly configured feature_store.yaml. This will make each materialization process talk to the registry database concurrently, and ensure the metadata updates are serialized.
