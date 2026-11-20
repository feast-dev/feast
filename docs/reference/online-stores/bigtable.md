# Bigtable online store

## Description

The [Bigtable](https://cloud.google.com/bigtable) online store provides support for
materializing feature values into Cloud Bigtable. The data model used to store feature
values in Bigtable is described in more detail
[here](../../specs/online_store_format.md#google-bigtable-online-store-format).

## Getting started

In order to use this online store, you'll need to run `pip install 'feast[gcp]'`. You
can then get started with the command `feast init REPO_NAME -t gcp`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: gcp
online_store:
  type: bigtable
  project_id: my_gcp_project
  instance: my_bigtable_instance
```
{% endcode %}

The full set of configuration options is available in
[BigtableOnlineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.bigtable.BigtableOnlineStoreConfig).

## Batched Multi-Feature-View Reads

Feature views keyed on the same entities share one Bigtable table. When a request spans several feature views, the rows of all the views in a table are fetched with a single `ReadRows` call, so a request for 10 feature views on the same entities costs **1** call instead of 10. Requests larger than `max_batched_result_rows` (entity rows × feature views, default 2048) read one feature view at a time instead. The row filter of that call matches each view's rows by row key and selects only the features requested from that view, so a batched read returns the same cells as the per-view reads. Requests whose filter would come close to Bigtable's 20 KB row filter limit, which takes several hundred requested features in one table, read one feature view at a time, and so do requests naming a feature view whose name is not plain ASCII. A single-view request is never batched. Set `max_batched_result_rows: 0` to turn batching off.

## Table creation and writes

These settings change how the Python SDK creates tables and writes to them. Each is off unless set, so a
`feature_store.yaml` without them behaves as before. The Go feature server ignores all of them except
`app_profile`, which it uses for its reads too.

{% code title="feature_store.yaml" %}
```yaml
online_store:
  type: bigtable
  project_id: my_gcp_project
  instance: my_bigtable_instance
  app_profile: serving
  write_app_profile: materialization
  initial_tablets: 128
```
{% endcode %}

| Option | Default | Description |
| ------ | ------- | ----------- |
| `app_profile` | the instance's default app profile | App profile for online reads, by the Python SDK and the Go feature server. |
| `write_app_profile` | the instance's default app profile | App profile for the Python SDK's writes (`feast materialize` and push). It does not fall back to `app_profile`, so setting `app_profile` alone never moves writes onto the serving profile. Point it at an app profile with low request priority so that materialization does not compete with serving reads. |
| `initial_tablets` | unset, no split | When set, `feast apply` creates each new table pre-split into this many tablets, from 2 to 256, at evenly spaced row key prefixes. Existing tables are not changed. |

Pre-splitting matters for small tables. A new table starts as a single tablet, and Bigtable may keep a
small table on one tablet, served by one node, so every read of a small table, such as one keyed by
`driver`, can land on that node however many nodes the cluster has. Row keys start with a uniform hex
hash, so split points at even hex prefixes give tablets of about the same size. `initial_tablets: 16`
splits at `10`, `20`, ... `f0`. As a rule of thumb, use a few tablets per node at the node count you
expect to run. The maximum is 256.
To pre-split a table without this setting, for example with an older SDK, create it with
[`cbt`](https://cloud.google.com/bigtable/docs/cbt-overview) before `feast apply`, which then leaves the
table as it is and only adds the `features` column family:

```bash
cbt -project my_gcp_project -instance my_bigtable_instance createtable my_feature_repo.driver "splits=10,20,30,40,50,60,70,80,90,a0,b0,c0,d0,e0,f0"
```

The table name is `<project>.<entity names, sorted and joined by "-">`, or `<project>.__dummy` for a
feature view with no entities, and is at most 50 characters. An entity part longer than 24 characters is
replaced by the first 24 hex characters of its md5 hash. A project too long for the rest is replaced by
its first 10 characters, `_`, and as much of its md5 hash in hex as fits.

## Go feature server

The Go feature server reads the tables that the Python SDK materializes into Bigtable. It reads the
same `online_store` section of `feature_store.yaml`, plus a few read settings that only it uses. The
Python SDK accepts these settings so that one `feature_store.yaml` works for both, but ignores them.
Python SDK releases from before these options were added reject them (`Extra inputs are not permitted`),
so with an older SDK keep these keys out of the `feature_store.yaml` that `feast apply` and
`feast materialize` read.

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: gcp
entity_key_serialization_version: 3
online_store:
  type: bigtable
  project_id: my_gcp_project
  instance: my_bigtable_instance
  max_versions: 1
  app_profile: serving
  keys_per_request: 5
  max_concurrent_chunks: 50
  read_timeout_ms: 250
  max_inflight_reads: 300
  max_inflight_wait_ms: 10
  adaptive_chunking: false
  disable_metrics: false
```
{% endcode %}

| Option | Default | Description |
| ------ | ------- | ----------- |
| `project_id` | `GOOGLE_CLOUD_PROJECT`, then `GCLOUD_PROJECT`, then the project of the default credentials | GCP project of the instance. When unset, the Go feature server takes the first of these that is set, the same order as the Python client, and fails at startup if none is. User credentials from `gcloud auth application-default login` carry no project, so set `project_id` or `GOOGLE_CLOUD_PROJECT` with them and with the emulator. |
| `instance` | required | The Bigtable instance's ID. |
| `max_versions` | `2` | Ignored by the Go feature server, which accepts any value and always reads the latest cell version. The Python SDK uses it as the column family's garbage collection rule. For online serving, `1` is enough. `feast apply` sets the rule when it creates the column family; to change an existing table's rule, use `cbt setgcpolicy`. |
| `app_profile` | the instance's default app profile | Bigtable app profile used for reads, for example one with high request priority, apart from `write_app_profile`. The Python SDK's online reads use it too. The profile must already exist on the instance. |
| `keys_per_request` | `5` | Number of row keys in one `ReadRows` request, from 1 to 100. Each read is split into chunks of this size. Higher values make fewer calls and use less CPU, at the cost of latency. See below. |
| `max_concurrent_chunks` | `50` | Maximum number of chunks that one online read sends to Bigtable at the same time. It limits each read call, not the whole server; `max_inflight_reads` limits the server. Lower values reduce the load on a small instance at the cost of latency for large reads. |
| `read_timeout_ms` | `60000` | Time budget in milliseconds for one online read, including the client's retries and any wait for `max_inflight_reads` slots. The caller's deadline still applies, and the shorter of the two wins. The 60 s default is a ceiling that stops the client retrying without end, not a latency target. Set it near your serving target, for example 250 ms, so that an overloaded server fails reads quickly instead of holding them in memory. |
| `max_inflight_reads` | unset, no limit | Maximum number of `ReadRows` calls in flight across all online reads of the server. A read reserves all the slots it needs, the smallest of its chunk count, `max_concurrent_chunks` and `max_inflight_reads`, before it sends anything, and holds them until it ends. When they are not free within `max_inflight_wait_ms`, the read is rejected with `RESOURCE_EXHAUSTED` and sends nothing to Bigtable. Rejecting early keeps the admitted reads well inside their deadline, so that under overload the server keeps serving what the instance can handle instead of every read slowing down until `read_timeout_ms` fails it. `RESOURCE_EXHAUSTED` is the gRPC code; the HTTP endpoint returns 500 with the same message. Like any failed read, each rejection is logged at error level. Size it to the concurrent calls your instance serves within your latency target, divided by the number of servers, and keep it several times `max_concurrent_chunks`: a read reserves up to `max_concurrent_chunks` slots at once, so a cap near that value rejects large reads whenever anything else is in flight. |
| `max_inflight_wait_ms` | `0` | How long, in milliseconds, a read waits for free `max_inflight_reads` slots before it is rejected. `0` rejects it at once when the cap is full. A short wait, such as 10 to 20, absorbs bursts at the cost of that much latency for the reads that wait. Keep it well below `read_timeout_ms`, which stays the last bound on a read. Applies only when `max_inflight_reads` is set. |
| `adaptive_chunking` | `false` | When `true`, `keys_per_request` is the smallest chunk size, and a read that would need more than `max_concurrent_chunks` chunks gets larger chunks, up to 100 keys, so it fits in that many calls where it can. With the defaults, that is a read of more than 250 row keys. Large reads make fewer calls, but each larger chunk is slower. See below. |
| `disable_metrics` | `false` | When `true`, the Bigtable client exports no client-side metrics to Cloud Monitoring, so the serving identity needs no `monitoring.timeSeries.create` permission, and the server spends less CPU. The startup log's `metrics` field shows which applies. |

`keys_per_request`, `max_concurrent_chunks`, `read_timeout_ms` and `max_inflight_reads` must be positive
whole numbers and `max_inflight_wait_ms` a whole number from 0, at most 2147483647 and written unquoted:
a quoted value such as `"25"` or a boolean is rejected by `feast apply` and by the Go feature server.
`adaptive_chunking` and `disable_metrics` must be unquoted `true` or `false`. An invalid value fails the
Go feature server at startup rather than at the first request.

### Startup and permissions

At startup the Bigtable client primes its connections with `PingAndWarm`, so bad credentials or an
unreachable instance fail startup. The Go feature server then pings the instance once more, within 10
seconds, which gives the emulator the same check. Two answers only log a warning. One is `NotFound`,
which Bigtable returns for an instance with no tables yet; a mistyped instance, and possibly a mistyped
app profile, also returns it, and reads then fail, so check for this warning. The other is
`Unimplemented` from an emulator too old to support the ping.

The serving identity needs `bigtable.tables.readRows` and `bigtable.instances.ping`, both in
`roles/bigtable.reader`. The client library also exports client-side metrics to Cloud Monitoring, which
needs `monitoring.timeSeries.create`; without it the client logs one permission error and keeps serving.
Set `disable_metrics: true` to turn the export off.

### How the Go feature server reads

* Each online read builds one row key per entity and feature view, splits the row keys into chunks of
  `keys_per_request`, and reads up to `max_concurrent_chunks` chunks in parallel. With
  `max_inflight_reads` set, the read is first admitted or rejected as a whole (see the table above). The
  first chunk that fails cancels the others and fails the read.
* A `ReadRows` request with many row keys can take longer than the same keys sent as several smaller
  requests in parallel, so splitting a read into small chunks can cut its latency. The defaults, chunks
  of 5 keys with 50 in flight, favor latency on an instance with many nodes. Each chunk is a separate
  call, so small chunks cost more CPU in the Go feature server and more requests on Bigtable per read.
  On a small instance or at high request rates, raise `keys_per_request`, lower `max_concurrent_chunks`
  and set `max_inflight_reads`.
* With `adaptive_chunking: true`, a large read goes out in one round of larger chunks instead of
  several rounds of `keys_per_request`. With the defaults, 500 row keys go out as 50 calls of 10 keys
  rather than 100 calls of 5 in two rounds, and 1,500 row keys as 50 calls of 30. Chunks never shrink
  below `keys_per_request`. The size comes from the read's total row keys and each table is cut
  separately, so a read over several tables can plan a few more chunks than `max_concurrent_chunks`;
  the extra ones wait for a free slot. A very large read is still slower than one round of small
  chunks, because each larger call takes longer; raising `max_concurrent_chunks`
  is the other route, at a CPU cost in the Go feature server. The gain also depends on the floor: with
  a small `keys_per_request`, such as 2, the extra rounds are short, and a large read can come out
  slightly slower with `adaptive_chunking` on, so measure it at your own settings.
* Bigtable's [in-memory tier](https://cloud.google.com/bigtable/docs/in-memory-overview) (Enterprise
  Plus edition, in Preview) serves only single-row point reads, so a `ReadRows` request with two or more
  row keys is always read from storage. To let the Go feature server use it, set `keys_per_request: 1`,
  leave `adaptive_chunking` off so that large reads do not grow back into multi-key requests, and set
  `app_profile` to an app profile configured for the in-memory tier. Each row key is then its own request,
  which costs more CPU in the Go feature server, so check `max_concurrent_chunks` and
  `max_inflight_reads` against the larger request count. The store reads only the newest cell of each
  column, so its reads are not the historical-cell reads that the tier never serves.
* Only the latest version of each cell is read, so the Go feature server returns the same values as the
  Python SDK whatever `max_versions` is set to.
* Entity join keys must be of type `STRING`, `BYTES`, `INT32` or `INT64`. Other types, such as
  `UNIX_TIMESTAMP` or `UUID`, return an error.
* The HTTP endpoint types entity values by their JSON form, not by the entity's declared type: strings
  arrive as `STRING` and whole numbers as `INT64`. So `BYTES` and `INT32` join keys are found only through
  the gRPC endpoint, with the value sent as `bytes_val` or `int32_val`; over HTTP they return `NOT_FOUND`.
  This applies to every Go online store.
* `entity_key_serialization_version` must be `3`, the Python SDK's default. When it is omitted, the Go
  feature server uses `3`. Any other value fails at startup.

### Monitoring and operation

* The server exports `feast_bigtable_online_read_duration_seconds` by `status` (`ok`, `error`, or
  `rejected` for reads that `max_inflight_reads` turned away), and
  `feast_bigtable_online_read_chunks_total` (`ReadRows` calls sent) and
  `feast_bigtable_online_read_rows_found_total` by `table`, on its metrics port.
* Errors from Bigtable keep their gRPC code, so a gRPC client can also get `RESOURCE_EXHAUSTED` from
  Bigtable itself, for example from a quota. Those count under `status="error"`, not `rejected`, and their
  message names the table read rather than `max_inflight_reads`.
* With tracing on, each read's `bigtable.OnlineRead` span records its row keys, chunk size, chunks, the
  planned rounds of chunks, rows found, and the time it waited for its own `max_concurrent_chunks` slots
  (`bigtable.call_slot_wait_ms`) and for admission to `max_inflight_reads` (`bigtable.inflight_wait_ms`),
  which separates queueing from time in Bigtable.
* Consider pre-splitting small tables, or every read of one may land on a single node. Set
  `initial_tablets` before the first `feast apply`; see
  [Table creation and writes](#table-creation-and-writes).

### Data written by older Feast releases

The Go feature server reads the row keys that current Python
writes, so rows written with a different entity key encoding are not found (each entity returns
`NOT_FOUND`, with no error). Re-materialize such tables with a current release:

* Before 0.50, `entity_key_serialization_version` defaulted to `1`. Tables written by those releases with
  the setting omitted use version 1 keys.
* Before 0.44, version `3` keys had no entity count prefix (and before 0.40 an explicit `3` wrote
  version 2 keys), so tables written with an explicit `3` by any release before 0.44 differ too.
* Before 0.61, the length prefix of each join key name counted characters, not bytes, so tables with
  non-ASCII join key names written by 0.44 to 0.60 differ.

Table names, the column family, the `event_ts` qualifier and its format are unchanged since the Bigtable
online store was added.

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Bigtable online store.

|                                                           | Bigtable |
|-----------------------------------------------------------|----------|
| write feature values to the online store                  | yes      |
| read feature values from the online store                 | yes      |
| update infrastructure (e.g. tables) in the online store   | yes      |
| teardown infrastructure (e.g. tables) in the online store | yes      |
| generate a plan of infrastructure changes                 | no       |
| support for on-demand transforms                          | yes      |
| readable by Python SDK                                    | yes      |
| readable by Java                                          | no       |
| readable by Go                                            | yes      |
| support for entityless feature views                      | yes      |
| support for concurrent writing to the same key            | yes      |
| support for ttl (time to live) at retrieval               | no       |
| support for deleting expired data                         | no       |
| collocated by feature view                                | yes      |
| collocated by feature service                             | no       |
| collocated by entity key                                  | yes      |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
