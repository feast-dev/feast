# Trino offline store (contrib)

## Description

The Trino offline store provides support for reading [TrinoSources](../data-sources/trino.md).
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. A Pandas dataframes will be uploaded to Trino as a table in order to complete join operations.

## Disclaimer

The Trino offline store does not achieve full test coverage.
Please do not assume complete stability.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[trino]'`. You can then run `feast init`, then swap out `feature_store.yaml` with the below example to connect to Trino.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: feature_repo
registry: data/registry.db
provider: local
offline_store:
    type: feast_trino.trino.TrinoOfflineStore
    host: localhost
    port: 8080
    catalog: memory
    connector:
        type: memory
    user: trino
    source: feast-trino-offline-store
    http-scheme: https
    ssl-verify: false
    x-trino-extra-credential-header: foo=bar, baz=qux

    # enables authentication in Trino connections, pick the one you need
    # if you don't need authentication, you can safely remove the whole auth block
    auth:
        # Basic Auth
        type: basic
        config:
            username: foo
            password: $FOO

        # Certificate
        type: certificate
        config:
            cert-file: /path/to/cert/file
            key-file: /path/to/key/file

        # JWT
        type: jwt
        config:
            token: $JWT_TOKEN

        # OAuth2 (no config required)
        type: oauth2

        # Kerberos
        type: kerberos
        config:
            config-file: /path/to/kerberos/config/file
            service-name: foo
            mutual-authentication: true
            force-preemptive: true
            hostname-override: custom-hostname
            sanitize-mutual-error-response: true
            principal: principal-name
            delegate: true
            ca_bundle: /path/to/ca/bundle/file
online_store:
    path: data/online_store.db
```
{% endcode %}

The full set of configuration options is available in [TrinoOfflineStoreConfig](https://rtd.feast.dev/en/master/#trino-offline-store).

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Trino offline store.

|                                                                    | Trino |
| :----------------------------------------------------------------- | :---- |
| `get_historical_features` (point-in-time correct join)             | yes   |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes   |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes   |
| `offline_write_batch` (persist dataframes to offline store)        | no    |
| `write_logged_features` (persist logged features to offline store) | no    |

Below is a matrix indicating which functionality is supported by `TrinoRetrievalJob`.

|                                                       | Trino |
| ----------------------------------------------------- | ----- |
| export to dataframe                                   | yes   |
| export to arrow table                                 | yes   |
| export to arrow batches                               | no    |
| export to SQL                                         | yes   |
| export to data lake (S3, GCS, etc.)                   | no    |
| export to data warehouse                              | no    |
| export as Spark dataframe                             | no    |
| local execution of Python-based on-demand transforms  | yes   |
| remote execution of Python-based on-demand transforms | no    |
| persist results in the offline store                  | no    |
| preview the query plan before execution               | yes   |
| read partitioned data                                 | yes   |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
