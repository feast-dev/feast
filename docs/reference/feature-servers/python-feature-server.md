# Python feature server

## Overview

The Python feature server is an HTTP endpoint that serves features with JSON I/O. This enables users to write and read features from the online store using any programming language that can make HTTP requests.

## CLI

There is a CLI command that starts the server: `feast serve`. By default, Feast uses port 6566; the port be overridden with a `--port` flag.

## Deploying as a service

One can deploy a feature server by building a docker image that bundles in the project's `feature_store.yaml`. See this [helm chart](https://github.com/feast-dev/feast/blob/master/infra/charts/feast-feature-server) for an example on how to run Feast on Kubernetes.

A [remote feature server](alpha-aws-lambda-feature-server.md) on AWS Lambda is also available.

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

The Python feature server also exposes an endpoint for [push sources](../../data-sources/push.md). This endpoint allows you to push data to the online and/or offline store.

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
