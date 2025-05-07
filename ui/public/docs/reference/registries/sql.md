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
- The schema for how data is laid out in tables can be found . It is intentionally simple, storing the serialized protobuf versions of each Feast object keyed by its name.

## Example Usage: Concurrent materialization
The SQL Registry should be used when materializing feature views concurrently to ensure correctness of data in the registry. This can be achieved by simply running feast materialize or feature_store.materialize multiple times using a correctly configured feature_store.yaml. This will make each materialization process talk to the registry database concurrently, and ensure the metadata updates are serialized.
