# SQLite online store

## Description

The [SQLite](https://www.sqlite.org/index.html) online store provides support for materializing feature values into an SQLite database for serving online features.

* All feature values are stored in an on-disk SQLite database
* Only the latest feature values are persisted

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: sqlite
  path: data/online_store.db
```
{% endcode %}

The full set of configuration options is available in [SqliteOnlineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.sqlite.SqliteOnlineStoreConfig).

## Upgrading from local-time timestamps

The SQLite online store now reads and writes the `event_ts` / `created_ts` Unix timestamps as UTC. Older versions resolved timezone-naive timestamps in the process local timezone when writing, so what is already on disk depends on where the rows were written:

* Environments that already ran in the UTC timezone are not affected, since the stored epochs already match UTC.
* On non-UTC environments the epochs of previously written rows are shifted by the local UTC offset. The old writer's timezone cannot be recovered from the stored integers, so an in-place upgrade cannot repair existing SQLite database files automatically. Rebuild the SQLite database file, or run a full backfill with `feast materialize <start_ts> <end_ts>` covering the time range of the existing data to rewrite the old rows as UTC.
* `feast materialize-incremental` only writes the interval after the last completed materialization and does not rewrite rows stored before it, so it cannot recover legacy rows on its own.

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Sqlite online store.

| | Sqlite |
| :-------------------------------------------------------- | :-- |
| write feature values to the online store                  | yes |
| read feature values from the online store                 | yes |
| update infrastructure (e.g. tables) in the online store   | yes |
| teardown infrastructure (e.g. tables) in the online store | yes |
| generate a plan of infrastructure changes                 | yes |
| support for on-demand transforms                          | yes |
| readable by Python SDK                                    | yes |
| readable by Java                                          | no  |
| readable by Go                                            | yes |
| support for entityless feature views                      | yes |
| support for concurrent writing to the same key            | no  |
| support for ttl (time to live) at retrieval               | no  |
| support for deleting expired data                         | no  |
| collocated by feature view                                | yes |
| collocated by feature service                             | no  |
| collocated by entity key                                  | no  |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
