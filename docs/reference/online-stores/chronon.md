# Chronon online store (contrib)

## Description

The [Chronon](https://chronon.ai/) online store lets Feast read online features from a Chronon service.

This integration is a bridge, not a Feast-managed online store. Chronon owns materialization and serving. Feast builds Chronon request URLs from `ChrononSource` metadata and converts Chronon responses into Feast values for `get_online_features`.

## Getting started

Configure the Chronon online store with the base URL of your Chronon service:

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: chronon
offline_store:
  type: chronon
online_store:
  type: chronon
  path: http://localhost:8080
  timeout: 30
  verify_ssl: true
```
{% endcode %}

Each online feature view must use a `ChrononSource` with exactly one of `chronon_join` or `chronon_group_by`:

```python
source = ChrononSource(
    name="driver_stats_source",
    materialization_path="data/chronon/driver_stats",
    chronon_join="team/driver_stats.v1",
    online_endpoint="https://chronon.example.com",
    timestamp_field="event_timestamp",
)
```

If `online_endpoint` is set on the source, it overrides the online store `path` for that feature view.

## Request routing

Feast builds Chronon URLs from the source metadata:

| Source field       | URL shape |
| :----------------- | :-------- |
| `chronon_join`     | `/v1/features/join/{quoted_chronon_join}` |
| `chronon_group_by` | `/v1/features/groupby/{quoted_chronon_group_by}` |

The request body is a list of entity rows. Chronon responses are expected to be JSON objects with a `results` list whose length matches the number of requested entity keys. Rows with non-success statuses are returned as missing feature rows.

## Local service testing

The repository includes `infra/scripts/chronon/start-local-chronon-service.sh` for live integration testing against Chronon's quickstart service. The script expects a local [airbnb/chronon](https://github.com/airbnb/chronon) checkout with the quickstart Mongo implementation and service jars built.

Use `CHRONON_REPO` when the Chronon checkout is not located at `<feast repo>/chronon`:

```bash
CHRONON_REPO=/path/to/chronon infra/scripts/chronon/start-local-chronon-service.sh
```

For a preflight-only check:

```bash
CHRONON_PREFLIGHT_ONLY=1 CHRONON_REPO=/path/to/chronon infra/scripts/chronon/start-local-chronon-service.sh
```

## Configuration reference

| Parameter    | Required | Default                 | Description |
| :----------- | :------- | :---------------------- | :---------- |
| `type`       | yes      | `chronon`               | Must be set to `chronon`. |
| `path`       | no       | `http://localhost:8080` | Chronon online service base URL. |
| `timeout`    | no       | `30`                    | HTTP request timeout in seconds. |
| `verify_ssl` | no       | `true`                  | Whether TLS certificates are verified for HTTPS requests. |

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Chronon online store.

|                                                           | Chronon |
| :-------------------------------------------------------- | :------ |
| write feature values to the online store                  | no      |
| read feature values from the online store                 | yes     |
| update infrastructure (e.g. tables) in the online store   | no      |
| teardown infrastructure (e.g. tables) in the online store | no      |
| generate a plan of infrastructure changes                 | no      |
| support for on-demand transforms                          | no      |
| readable by Python SDK                                    | yes     |
| readable by Java                                          | no      |
| readable by Go                                            | no      |
| support for entityless feature views                      | no      |
| support for concurrent writing to the same key            | n/a     |
| support for ttl (time to live) at retrieval               | Chronon |
| support for deleting expired data                         | Chronon |
| collocated by feature view                                | Chronon |
| collocated by feature service                             | Chronon |
| collocated by entity key                                  | Chronon |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
