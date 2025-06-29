# Registry server

## Description

The Registry server supports both gRPC and REST interfaces for interacting with feature metadata. While gRPC remains the default protocol—enabling clients in any language with gRPC support—the REST API allows users to interact with the registry over standard HTTP using any REST-capable tool or language.

Feast supports running the Registry Server in three distinct modes:

| Mode        | Command Example                             | Description                            |
| ----------- | ------------------------------------------- | -------------------------------------- |
| gRPC only   | `feast serve_registry`                      | Default behavior for SDK and clients   |
| REST + gRPC | `feast serve_registry --rest-api`           | Enables both interfaces                |
| REST only   | `feast serve_registry --rest-api --no-grpc` | Used for REST-only clients like the UI |

## REST API Endpoints

The REST API provides HTTP/JSON endpoints for accessing all registry metadata. All endpoints are prefixed with `/api/v1` and return JSON responses.

### Authentication

The REST API supports Bearer token authentication. Include your token in the Authorization header:

```bash
Authorization: Bearer <your-token>
```

### Common Query Parameters

Most endpoints support these common query parameters:

- `project` (required for most endpoints): The project name
- `allow_cache` (optional, default: `true`): Whether to allow cached data
- `tags` (optional): Filter results by tags in key=value format

### Entities

#### List Entities
- **Endpoint**: `GET /api/v1/entities`
- **Description**: Retrieve all entities in a project
- **Parameters**:
  - `project` (required): Project name
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/entities?project=my_project"
  ```

#### Get Entity
- **Endpoint**: `GET /api/v1/entities/{name}`
- **Description**: Retrieve a specific entity by name
- **Parameters**:
  - `name` (path): Entity name
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/entities/user_id?project=my_project"
  ```

### Data Sources

#### List Data Sources
- **Endpoint**: `GET /api/v1/data_sources`
- **Description**: Retrieve all data sources in a project
- **Parameters**:
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
  - `tags` (optional): Filter by tags
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/data_sources?project=my_project"
  ```

#### Get Data Source
- **Endpoint**: `GET /api/v1/data_sources/{name}`
- **Description**: Retrieve a specific data source by name
- **Parameters**:
  - `name` (path): Data source name
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/data_sources/user_data?project=my_project"
  ```

### Feature Views

#### List Feature Views
- **Endpoint**: `GET /api/v1/feature_views`
- **Description**: Retrieve all feature views in a project
- **Parameters**:
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
  - `tags` (optional): Filter by tags
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views?project=my_project"
  ```

#### Get Feature View
- **Endpoint**: `GET /api/v1/feature_views/{name}`
- **Description**: Retrieve a specific feature view by name
- **Parameters**:
  - `name` (path): Feature view name
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views/user_features?project=my_project"
  ```

### Feature Services

#### List Feature Services
- **Endpoint**: `GET /api/v1/feature_services`
- **Description**: Retrieve all feature services in a project
- **Parameters**:
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
  - `tags` (optional): Filter by tags
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_services?project=my_project"
  ```

#### Get Feature Service
- **Endpoint**: `GET /api/v1/feature_services/{name}`
- **Description**: Retrieve a specific feature service by name
- **Parameters**:
  - `name` (path): Feature service name
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_services/recommendation_service?project=my_project"
  ```

### Lineage and Relationships

#### Get Registry Lineage
- **Endpoint**: `GET /api/v1/lineage/registry`
- **Description**: Retrieve complete registry lineage with relationships and indirect relationships
- **Parameters**:
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
  - `filter_object_type` (optional): Filter by object type (`dataSource`, `entity`, `featureView`, `featureService`)
  - `filter_object_name` (optional): Filter by object name
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/lineage/registry?project=my_project"
  ```

#### Get Object Relationships
- **Endpoint**: `GET /api/v1/lineage/objects/{object_type}/{object_name}`
- **Description**: Retrieve relationships for a specific object
- **Parameters**:
  - `object_type` (path): Type of object (`dataSource`, `entity`, `featureView`, `featureService`)
  - `object_name` (path): Name of the object
  - `project` (required): Project name
  - `include_indirect` (optional): Whether to include indirect relationships
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/lineage/objects/featureView/user_features?project=my_project&include_indirect=true"
  ```

#### Get Complete Registry Data
- **Endpoint**: `GET /api/v1/lineage/complete`
- **Description**: Retrieve complete registry data including all objects and relationships (optimized for UI consumption)
- **Parameters**:
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/lineage/complete?project=my_project"
  ```

### Permissions

#### List Permissions
- **Endpoint**: `GET /api/v1/permissions`
- **Description**: Retrieve all permissions in a project
- **Parameters**:
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/permissions?project=my_project"
  ```

#### Get Permission
- **Endpoint**: `GET /api/v1/permissions/{name}`
- **Description**: Retrieve a specific permission by name
- **Parameters**:
  - `name` (path): Permission name
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/permissions/read_features?project=my_project"
  ```

### Projects

#### List Projects
- **Endpoint**: `GET /api/v1/projects`
- **Description**: Retrieve all projects
- **Parameters**:
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/projects"
  ```

#### Get Project
- **Endpoint**: `GET /api/v1/projects/{name}`
- **Description**: Retrieve a specific project by name
- **Parameters**:
  - `name` (path): Project name
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/projects/my_project"
  ```

### Saved Datasets

#### List Saved Datasets
- **Endpoint**: `GET /api/v1/saved_datasets`
- **Description**: Retrieve all saved datasets in a project
- **Parameters**:
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
  - `tags` (optional): Filter by tags
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/saved_datasets?project=my_project"
  ```

#### Get Saved Dataset
- **Endpoint**: `GET /api/v1/saved_datasets/{name}`
- **Description**: Retrieve a specific saved dataset by name
- **Parameters**:
  - `name` (path): Saved dataset name
  - `project` (required): Project name
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/saved_datasets/training_data?project=my_project"
  ```

### Response Formats

All endpoints return JSON responses with the following general structure:

- **Success (200)**: Returns the requested data
- **Bad Request (400)**: Invalid parameters or request format
- **Unauthorized (401)**: Missing or invalid authentication token
- **Not Found (404)**: Requested resource does not exist
- **Internal Server Error (500)**: Server-side error

### Interactive API Documentation

When the REST API server is running, you can access interactive documentation at:

- **Swagger UI**: `http://localhost:6572/` (root path)
- **ReDoc**: `http://localhost:6572/docs`
- **OpenAPI Schema**: `http://localhost:6572/openapi.json`

## How to configure the server

## CLI

There is a CLI command that starts the Registry server: `feast serve_registry`. By default, remote Registry Server uses port 6570, the port can be overridden with a `--port` flag.
To start the Registry Server in TLS mode, you need to provide the private and public keys using the `--key` and `--cert` arguments.
More info about TLS mode can be found in [feast-client-connecting-to-remote-registry-sever-started-in-tls-mode](../../how-to-guides/starting-feast-servers-tls-mode.md#starting-feast-registry-server-in-tls-mode)

To enable REST API support along with gRPC, start the registry server with REST mode enabled : 

`feast serve_registry --rest-api`

This launches both the gRPC and REST servers concurrently. The REST server listens on port 6572 by default.

To run a REST-only server (no gRPC):

`feast serve_registry --rest-api --no-grpc`


## How to configure the client

Please see the detail how to configure Remote Registry client [remote.md](../registries/remote.md)

# Registry Server Permissions and Access Control

Please refer the [page](./../registry/registry-permissions.md) for more details on API Endpoints and Permissions.

## How to configure Authentication and Authorization ?

Please refer the [page](./../../../docs/getting-started/concepts/permission.md) for more details on how to configure authentication and authorization.