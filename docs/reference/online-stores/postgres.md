# PostgreSQL (contrib)

## Description

The PostgreSQL online store provides support for materializing feature values into a PostgreSQL database for serving online features.

* Only the latest feature values are persisted

* sslmode, sslkey_path, sslcert_path, and sslrootcert_path are optional

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: postgres
    host: DB_HOST
    port: DB_PORT
    database: DB_NAME
    db_schema: DB_SCHEMA
    user: DB_USERNAME
    password: DB_PASSWORD
    sslmode: verify-ca
    sslkey_path: /path/to/client-key.pem
    sslcert_path: /path/to/client-cert.pem
    sslrootcert_path: /path/to/server-ca.pem
```
{% endcode %}

Configuration options are available [here](https://rtd.feast.dev/en/latest/#feast.repo_config.SqliteOnlineStoreConfig).
