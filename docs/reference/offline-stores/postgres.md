# PostgreSQL (contrib)

## Description

The PostgreSQL offline store is an offline store that provides support for reading [PostgreSQL](../data-sources/postgres.md) data sources.


**DISCLAIMER**: This PostgreSQL offline store still does not achieve full test coverage.

* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. Pandas dataframes will be converted to a Spark dataframe and processed as a temporary view.
* A `SparkRetrievalJob` is returned when calling `get_historical_features()`.
  * This allows you to call
     * `to_df` to retrieve the pandas dataframe.
     * `to_arrow` to retrieve the dataframe as a PyArrow table.

* sslmode, sslkey_path, sslcert_path, and sslrootcert_path are optional

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
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
online_store:
    path: data/online_store.db
```
{% endcode %}
