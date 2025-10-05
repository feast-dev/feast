# Python feature server

## Overview

The Python feature server is an HTTP endpoint that serves features with JSON I/O. This enables users to write and read features from the online store using any programming language that can make HTTP requests.

## CLI

There is a CLI command that starts the server: `feast serve`. By default, Feast uses port 6566; the port be overridden with a `--port` flag.

## Deploying as a service

See [this](../../how-to-guides/running-feast-in-production.md#id-4.2.-deploy-feast-feature-servers-on-kubernetes) for an example on how to run Feast on Kubernetes using the Operator.

## Example

### Initializing a feature server

Here's an example of how to start the Python feature server with a local feature repo:

```bash
$ feast init feature_repo
Creating a new Feast repository in /home/tsotne/feast/feature_repo.

$ cd feature_repo

$ feast apply
Created entity driver
Created feature view driver_hourly_stats
Created feature service driver_activity

Created sqlite table feature_repo_driver_hourly_stats

$ feast materialize-incremental $(date +%Y-%m-%d)
Materializing 1 feature views to 2021-09-09 17:00:00-07:00 into the sqlite online store.

driver_hourly_stats from 2021-09-09 16:51:08-07:00 to 2021-09-09 17:00:00-07:00:
100%|████████████████████████████████████████████████████████████████| 5/5 [00:00<00:00, 295.24it/s]

$ feast serve
09/10/2021 10:42:11 AM INFO:Started server process [8889]
INFO:     Waiting for application startup.
09/10/2021 10:42:11 AM INFO:Waiting for application startup.
INFO:     Application startup complete.
09/10/2021 10:42:11 AM INFO:Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:6566 (Press CTRL+C to quit)
09/10/2021 10:42:11 AM INFO:Uvicorn running on http://127.0.0.1:6566 (Press CTRL+C to quit)
```

### Retrieving features

After the server starts, we can execute cURL commands from another terminal tab:

```bash
$  curl -X POST \
  "http://localhost:6566/get-online-features" \
  -d '{
    "features": [
      "driver_hourly_stats:conv_rate",
      "driver_hourly_stats:acc_rate",
      "driver_hourly_stats:avg_daily_trips"
    ],
    "entities": {
      "driver_id": [1001, 1002, 1003]
    }
  }' | jq
{
  "metadata": {
    "feature_names": [
      "driver_id",
      "conv_rate",
      "avg_daily_trips",
      "acc_rate"
    ]
  },
  "results": [
    {
      "values": [
        1001,
        0.7037263512611389,
        308,
        0.8724706768989563
      ],
      "statuses": [
        "PRESENT",
        "PRESENT",
        "PRESENT",
        "PRESENT"
      ],
      "event_timestamps": [
        "1970-01-01T00:00:00Z",
        "2021-12-31T23:00:00Z",
        "2021-12-31T23:00:00Z",
        "2021-12-31T23:00:00Z"
      ]
    },
    {
      "values": [
        1002,
        0.038169607520103455,
        332,
        0.48534533381462097
      ],
      "statuses": [
        "PRESENT",
        "PRESENT",
        "PRESENT",
        "PRESENT"
      ],
      "event_timestamps": [
        "1970-01-01T00:00:00Z",
        "2021-12-31T23:00:00Z",
        "2021-12-31T23:00:00Z",
        "2021-12-31T23:00:00Z"
      ]
    },
    {
      "values": [
        1003,
        0.9665873050689697,
        779,
        0.7793770432472229
      ],
      "statuses": [
        "PRESENT",
        "PRESENT",
        "PRESENT",
        "PRESENT"
      ],
      "event_timestamps": [
        "1970-01-01T00:00:00Z",
        "2021-12-31T23:00:00Z",
        "2021-12-31T23:00:00Z",
        "2021-12-31T23:00:00Z"
      ]
    }
  ]
}
```

It's also possible to specify a feature service name instead of the list of features:

```
curl -X POST \
  "http://localhost:6566/get-online-features" \
  -d '{
    "feature_service": <feature-service-name>,
    "entities": {
      "driver_id": [1001, 1002, 1003]
    }
  }' | jq
```

### Pushing features to the online and offline stores

The Python feature server also exposes an endpoint for [push sources](../data-sources/push.md). This endpoint allows you to push data to the online and/or offline store.

The request definition for `PushMode` is a string parameter `to` where the options are: \[`"online"`, `"offline"`, `"online_and_offline"`].

**Note:** timestamps need to be strings, and might need to be timezone aware (matching the schema of the offline store)

```
curl -X POST "http://localhost:6566/push" -d '{
    "push_source_name": "driver_stats_push_source",
    "df": {
            "driver_id": [1001],
            "event_timestamp": ["2022-05-13 10:59:42+00:00"],
            "created": ["2022-05-13 10:59:42"],
            "conv_rate": [1.0],
            "acc_rate": [1.0],
            "avg_daily_trips": [1000]
    },
    "to": "online_and_offline"
  }' | jq
```

or equivalently from Python:

```python
import json
import requests
from datetime import datetime

event_dict = {
    "driver_id": [1001],
    "event_timestamp": [str(datetime(2021, 5, 13, 10, 59, 42))],
    "created": [str(datetime(2021, 5, 13, 10, 59, 42))],
    "conv_rate": [1.0],
    "acc_rate": [1.0],
    "avg_daily_trips": [1000],
    "string_feature": "test2",
}
push_data = {
    "push_source_name":"driver_stats_push_source",
    "df":event_dict,
    "to":"online",
}
requests.post(
    "http://localhost:6566/push",
    data=json.dumps(push_data))
```

### Materializing features

The Python feature server also exposes an endpoint for materializing features from the offline store to the online store.

**Standard materialization with timestamps:**
```bash
curl -X POST "http://localhost:6566/materialize" -d '{
    "start_ts": "2021-01-01T00:00:00",
    "end_ts": "2021-01-02T00:00:00",
    "feature_views": ["driver_hourly_stats"]
}' | jq
```

**Materialize all data without event timestamps:**
```bash
curl -X POST "http://localhost:6566/materialize" -d '{
    "feature_views": ["driver_hourly_stats"],
    "disable_event_timestamp": true
}' | jq
```

When `disable_event_timestamp` is set to `true`, the `start_ts` and `end_ts` parameters are not required, and all available data is materialized using the current datetime as the event timestamp. This is useful when your source data lacks proper event timestamp columns.

Or from Python:
```python
import json
import requests

# Standard materialization
materialize_data = {
    "start_ts": "2021-01-01T00:00:00",
    "end_ts": "2021-01-02T00:00:00",
    "feature_views": ["driver_hourly_stats"]
}

# Materialize without event timestamps
materialize_data_no_timestamps = {
    "feature_views": ["driver_hourly_stats"],
    "disable_event_timestamp": True
}

requests.post(
    "http://localhost:6566/materialize",
    data=json.dumps(materialize_data))
```

## Starting the feature server in TLS(SSL) mode

Enabling TLS mode ensures that data between the Feast client and server is transmitted securely. For an ideal production environment, it is recommended to start the feature server in TLS mode.

### Obtaining a self-signed TLS certificate and key
In development mode we can generate a self-signed certificate for testing. In an actual production environment it is always recommended to get it from a trusted TLS certificate provider.

```shell
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes
```

The above command will generate two files
* `key.pem` : certificate private key
* `cert.pem`: certificate public key

### Starting the Online Server in TLS(SSL) Mode
To start the feature server in TLS mode, you need to provide the private and public keys using the `--key` and `--cert` arguments with the `feast serve` command.

```shell
feast serve --key /path/to/key.pem --cert /path/to/cert.pem
```

# Online Feature Server Permissions and Access Control

## API Endpoints and Permissions

| Endpoint                   | Resource Type                   | Permission                                            | Description                                                    |
|----------------------------|---------------------------------|-------------------------------------------------------|----------------------------------------------------------------|
| /get-online-features       | FeatureView,OnDemandFeatureView | Read Online                                           | Get online features from the feature store                     |
| /retrieve-online-documents | FeatureView                     | Read Online                                           | Retrieve online documents from the feature store for RAG       |
| /push                      | FeatureView                     | Write Online, Write Offline, Write Online and Offline | Push features to the feature store (online, offline, or both)  |
| /write-to-online-store     | FeatureView                     | Write Online                                          | Write features to the online store                             |
| /materialize               | FeatureView                     | Write Online                                          | Materialize features within a specified time range             |
| /materialize-incremental   | FeatureView                     | Write Online                                          | Incrementally materialize features up to a specified timestamp |

## How to configure Authentication and Authorization ?

Please refer the [page](./../../../docs/getting-started/concepts/permission.md) for more details on how to configure authentication and authorization.