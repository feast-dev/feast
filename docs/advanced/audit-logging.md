# Audit Logging

{% hint style="warning" %}
This page applies to Feast 0.7. The content may be out of date for Feast 0.8+
{% endhint %}

## Introduction

Feast provides audit logging functionality in order to debug problems and to trace the lineage of events.

## Audit Log Types

Audit Logs produced by Feast come in three favors:

| Audit Log Type | Description |
| :--- | :--- |
| Message Audit Log | Logs service calls that can be used to track Feast request handling. Currently only gRPC request/response is supported. Enabling Message Audit Logs can be resource intensive and significantly increase latency, as such is not recommended on Online Serving. |
| Transition Audit Log | Logs transitions in status in resources managed by Feast \(ie an Ingestion Job becoming RUNNING\). |
| Action Audit Log | Logs actions performed on a specific resource managed by Feast \(ie an Ingestion Job is aborted\). |

## Configuration

| Audit Log Type | Description |
| :--- | :--- |
| Message Audit Log | Enabled when both `feast.logging.audit.enabled` and `feast.logging.audit.messageLogging.enabled` is set to `true` |
| Transition Audit Log | Enabled when `feast.logging.audit.enabled` is set to `true` |
| Action Audit Log | Enabled when `feast.logging.audit.enabled` is set to `true` |

## JSON Format

Audit Logs produced by Feast are written to the console similar to normal logs but in a structured, machine parsable JSON. Example of a Message Audit Log JSON entry produced:

```text
{
  "message": {
    "logType": "FeastAuditLogEntry",
    "kind": "MESSAGE",
    "statusCode": "OK",
    "request": {
      "filter": {
        "project": "dummy",
      }
    },
    "application": "Feast",
    "response": {},
    "method": "ListFeatureTables",
    "identity": "105960238928959148073",
    "service": "CoreService",
    "component": "feast-core",
    "id": "45329ea9-0d48-46c5-b659-4604f6193711",
    "version": "0.10.0-SNAPSHOT"
  },
  "hostname": "feast.core"
  "timestamp": "2020-10-20T04:45:24Z",
  "severity": "INFO",
}
```

## Log Entry Schema

Fields common to all Audit Log Types:

| Field | Description |
| :--- | :--- |
| `logType` | Log Type. Always set to `FeastAuditLogEntry`. Useful for filtering out Feast audit logs. |
| `application` | Application. Always set to `Feast`. |
| `component` | Feast Component producing the Audit Log. Set to `feast-core` for Feast Core and `feast-serving` for Feast Serving. Use to filtering out Audit Logs by component. |
| `version` | Version of Feast producing this Audit Log. Use to filtering out Audit Logs by version. |

Fields in Message Audit Log Type

| Field | Description |
| :--- | :--- |
| `id` | Generated UUID that uniquely identifies the service call. |
| `service` | Name of the Service that handled the service call. |
| `method` | Name of the Method that handled the service call. Useful for filtering Audit Logs by method \(ie `ApplyFeatureTable` calls\) |
| `request` | Full request submitted by client in the service call as JSON. |
| `response` | Full response returned to client by the service after handling the service call as JSON. |
| `identity` | Identity of the client making the service call as an user Id. Only set when Authentication is enabled. |
| `statusCode` | The status code returned by the service handling the service call \(ie `OK` if service call handled without error\). |

Fields in Action Audit Log Type

| Field | Description |
| :--- | :--- |
| `action` | Name of the action taken on the resource. |
| `resource.type` | Type of resource of which the action was taken on \(i.e `FeatureTable`\) |
| resource.id | Identifier specifying the specific resource of which the action was taken on. |

Fields in Transition Audit Log Type

| Field | Description |
| :--- | :--- |
| `status` | The new status that the resource transitioned to |
| `resource.type` | Type of resource of which the transition occurred \(i.e `FeatureTable`\) |
| `resource.id` | Identifier specifying the specific resource of which the transition occurred. |

## Log Forwarder

Feast currently only supports forwarding Request/Response \(Message Audit Log Type\) logs to an external fluentD service with `feast.**` Fluentd tag.

### Request/Response Log Example

```text
{
  "id": "45329ea9-0d48-46c5-b659-4604f6193711",
  "service": "CoreService"
  "status_code": "OK",
  "identity": "105960238928959148073",
  "method": "ListProjects",
  "request": {},
  "response": {
    "projects": [
      "default", "project1", "project2"
    ]
  }
  "release_name": 506.457.14.512
}
```

### Configuration

The Fluentd Log Forwarder configured with the with the following configuration options in `application.yml`:

| Settings | Description |
| :--- | :--- |
| `feast.logging.audit.messageLogging.destination` | `fluentd` |
| `feast.logging.audit.messageLogging.fluentdHost` | `localhost` |
| `feast.logging.audit.messageLogging.fluentdPort` | `24224` |

When using Fluentd as the Log forwarder, a Feast `release_name` can be logged instead of the IP address \(eg. IP of Kubernetes pod deployment\), by setting an environment variable `RELEASE_NAME` when deploying Feast.

