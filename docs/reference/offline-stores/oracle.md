# Oracle offline store (contrib)

## Description

The Oracle offline store provides support for reading [OracleSources](../data-sources/oracle.md).
* Entity dataframes can be provided as a SQL query or as a Pandas dataframe.
* Uses the [ibis](https://ibis-project.org/) Oracle backend (`ibis.oracle`) for all database interactions.
* Only one of `service_name`, `sid`, or `dsn` may be set in the configuration.

## Disclaimer

The Oracle offline store does not achieve full test coverage.
Please do not assume complete stability.

## Getting started

Install the Oracle extras:

```bash
pip install 'feast[oracle]'
```

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
  type: oracle
  host: DB_HOST
  port: 1521
  user: DB_USERNAME
  password: DB_PASSWORD
  service_name: ORCL
online_store:
  path: data/online_store.db
```
{% endcode %}

Connection can alternatively use `sid` or `dsn` instead of `service_name`:

```yaml
# Using SID
offline_store:
  type: oracle
  host: DB_HOST
  port: 1521
  user: DB_USERNAME
  password: DB_PASSWORD
  sid: ORCL

# Using DSN
offline_store:
  type: oracle
  host: DB_HOST
  port: 1521
  user: DB_USERNAME
  password: DB_PASSWORD
  dsn: "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=DB_HOST)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCL)))"
```

### Configuration reference

| Parameter      | Required | Default     | Description                                              |
| :------------- | :------- | :---------- | :------------------------------------------------------- |
| `type`         | yes      | —           | Must be set to `oracle`                                  |
| `user`         | yes      | —           | Oracle database user                                     |
| `password`     | yes      | —           | Oracle database password                                 |
| `host`         | no       | `localhost` | Oracle database host                                     |
| `port`         | no       | `1521`      | Oracle database port                                     |
| `service_name` | no       | —           | Oracle service name (mutually exclusive with sid and dsn) |
| `sid`          | no       | —           | Oracle SID (mutually exclusive with service_name and dsn) |
| `database`     | no       | —           | Oracle database name                                     |
| `dsn`          | no       | —           | Oracle DSN string (mutually exclusive with service_name and sid) |

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Oracle offline store.

|                                                                    | Oracle |
| :----------------------------------------------------------------- | :----- |
| `get_historical_features` (point-in-time correct join)             | yes    |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes    |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes    |
| `offline_write_batch` (persist dataframes to offline store)        | yes    |
| `write_logged_features` (persist logged features to offline store) | yes    |

Below is a matrix indicating which functionality is supported by `OracleRetrievalJob`.

|                                                       | Oracle |
| ----------------------------------------------------- | ------ |
| export to dataframe                                   | yes    |
| export to arrow table                                 | yes    |
| export to arrow batches                               | no     |
| export to SQL                                         | no     |
| export to data lake (S3, GCS, etc.)                   | no     |
| export to data warehouse                              | no     |
| export as Spark dataframe                             | no     |
| local execution of Python-based on-demand transforms  | yes    |
| remote execution of Python-based on-demand transforms | no     |
| persist results in the offline store                  | yes    |
| preview the query plan before execution               | no     |
| read partitioned data                                 | no     |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).

