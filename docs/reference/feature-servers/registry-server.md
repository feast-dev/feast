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

**Global `/all` Endpoints**

- Endpoints with `/all` (e.g., `/entities/all`) aggregate results across all projects.
- Each object in the response includes a `project` field.
- Support global pagination, sorting, and relationships (where applicable).
- No `project` parameter is required for `/all` endpoints.

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

#### Relationship Parameters
- `include_relationships` (optional, default: `false`): Include all relationships (both direct and indirect) for the object(s) in the response

#### Pagination Parameters (List Endpoints Only)
- `page` (optional): Page number (starts from 1)
- `limit` (optional, max: 100): Number of items per page
- `sort_by` (optional): Field to sort by
- `sort_order` (optional): Sort order: "asc" or "desc" (default: "asc")

### Entities

#### List Entities
- **Endpoint**: `GET /api/v1/entities`
- **Description**: Retrieve all entities in a project
- **Parameters**:
  - `project` (required): Project name
  - `include_relationships` (optional): Include relationships for each entity
  - `allow_cache` (optional): Whether to allow cached data
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
- **Examples**:
  ```bash
  # Basic list
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/entities?project=my_project"
  
  # With pagination
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/entities?project=my_project&page=1&limit=10&sort_by=name"
  
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/entities?project=my_project&include_relationships=true"
  ```

#### Get Entity
- **Endpoint**: `GET /api/v1/entities/{name}`
- **Description**: Retrieve a specific entity by name
- **Parameters**:
  - `name` (path): Entity name
  - `project` (required): Project name
  - `include_relationships` (optional): Include relationships for this entity
  - `allow_cache` (optional): Whether to allow cached data
- **Examples**:
  ```bash
  # Basic get
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/entities/user_id?project=my_project"
  
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/entities/user_id?project=my_project&include_relationships=true"
  ```

#### List All Entities (All Projects)
- **Endpoint**: `GET /api/v1/entities/all`
- **Description**: Retrieve all entities across all projects. Each entity includes a `project` field.
- **Parameters**:
  - `allow_cache` (optional): Whether to allow cached data
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
  - `include_relationships` (optional): Include relationships for each entity
- **Examples**:
  ```bash
  # List all entities across all projects
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/entities/all?page=1&limit=10&sort_by=name"
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/entities/all?include_relationships=true"
  ```
- **Response Example**:
  ```json
  {
    "entities": [
      { "name": "customer_id", "project": "project1", ... },
      { "name": "driver_id", "project": "project2", ... }
    ],
    "pagination": { "page": 1, "limit": 10, "total": 25, "totalPages": 3 },
    "relationships": { ... }
  }
  ```

### Data Sources

#### List Data Sources
- **Endpoint**: `GET /api/v1/data_sources`
- **Description**: Retrieve all data sources in a project
- **Parameters**:
  - `project` (required): Project name
  - `include_relationships` (optional): Include relationships for each data source
  - `allow_cache` (optional): Whether to allow cached data
  - `tags` (optional): Filter by tags
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
- **Examples**:
  ```bash
  # Basic list
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/data_sources?project=my_project"
  
  # With pagination and relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/data_sources?project=my_project&include_relationships=true&page=1&limit=10"
  ```

#### Get Data Source
- **Endpoint**: `GET /api/v1/data_sources/{name}`
- **Description**: Retrieve a specific data source by name
- **Parameters**:
  - `name` (path): Data source name
  - `project` (required): Project name
  - `include_relationships` (optional): Include relationships for this data source
  - `allow_cache` (optional): Whether to allow cached data
- **Examples**:
  ```bash
  # Basic get
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/data_sources/user_data?project=my_project"
  
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/data_sources/user_data?project=my_project&include_relationships=true"
  ```

#### List All Data Sources (All Projects)
- **Endpoint**: `GET /api/v1/data_sources/all`
- **Description**: Retrieve all data sources across all projects. Each data source includes a `project` field.
- **Parameters**:
  - `allow_cache` (optional): Whether to allow cached data
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
  - `include_relationships` (optional): Include relationships for each data source
- **Examples**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/data_sources/all?page=1&limit=10&sort_by=name"
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/data_sources/all?include_relationships=true"
  ```
- **Response Example**:
  ```json
  {
    "dataSources": [
      { "name": "user_data", "project": "project1", ... },
      { "name": "item_data", "project": "project2", ... }
    ],
    "pagination": { "page": 1, "limit": 10, "total": 25, "totalPages": 3 },
    "relationships": { ... }
  }
  ```

### Feature Views

#### List Feature Views
- **Endpoint**: `GET /api/v1/feature_views`
- **Description**: Retrieve all feature views in a project
- **Parameters**:
  - `project` (required): Project name
  - `include_relationships` (optional): Include relationships for each feature view
  - `allow_cache` (optional): Whether to allow cached data
  - `tags` (optional): Filter by tags
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
- **Examples**:
  ```bash
  # Basic list
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views?project=my_project"
  
  # With pagination and relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views?project=my_project&include_relationships=true&page=1&limit=5&sort_by=name"
  ```

#### Get Feature View
- **Endpoint**: `GET /api/v1/feature_views/{name}`
- **Description**: Retrieve a specific feature view by name
- **Parameters**:
  - `name` (path): Feature view name
  - `project` (required): Project name
  - `include_relationships` (optional): Include relationships for this feature view
  - `allow_cache` (optional): Whether to allow cached data
- **Examples**:
  ```bash
  # Basic get
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views/user_features?project=my_project"
  
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views/user_features?project=my_project&include_relationships=true"
  ```

#### List All Feature Views (All Projects)
- **Endpoint**: `GET /api/v1/feature_views/all`
- **Description**: Retrieve all feature views across all projects. Each feature view includes a `project` field.
- **Parameters**:
  - `allow_cache` (optional): Whether to allow cached data
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
  - `include_relationships` (optional): Include relationships for each feature view
- **Examples**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views/all?page=1&limit=10&sort_by=name"
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views/all?include_relationships=true"
  ```
- **Response Example**:
  ```json
  {
    "featureViews": [
      { "name": "user_features", "project": "project1", ... },
      { "name": "item_features", "project": "project2", ... }
    ],
    "pagination": { "page": 1, "limit": 10, "total": 25, "totalPages": 3 },
    "relationships": { ... }
  }
  ```

### Feature Services

#### List Feature Services
- **Endpoint**: `GET /api/v1/feature_services`
- **Description**: Retrieve all feature services in a project
- **Parameters**:
  - `project` (required): Project name
  - `include_relationships` (optional): Include relationships for each feature service
  - `allow_cache` (optional): Whether to allow cached data
  - `tags` (optional): Filter by tags
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
- **Examples**:
  ```bash
  # Basic list
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_services?project=my_project"
  
  # With pagination and relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_services?project=my_project&include_relationships=true&page=1&limit=10"
  ```

#### Get Feature Service
- **Endpoint**: `GET /api/v1/feature_services/{name}`
- **Description**: Retrieve a specific feature service by name
- **Parameters**:
  - `name` (path): Feature service name
  - `project` (required): Project name
  - `include_relationships` (optional): Include relationships for this feature service
  - `allow_cache` (optional): Whether to allow cached data
- **Examples**:
  ```bash
  # Basic get
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_services/recommendation_service?project=my_project"
  
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_services/recommendation_service?project=my_project&include_relationships=true"
  ```

#### List All Feature Services (All Projects)
- **Endpoint**: `GET /api/v1/feature_services/all`
- **Description**: Retrieve all feature services across all projects. Each feature service includes a `project` field.
- **Parameters**:
  - `allow_cache` (optional): Whether to allow cached data
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
  - `include_relationships` (optional): Include relationships for each feature service
- **Examples**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_services/all?page=1&limit=10&sort_by=name"
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_services/all?include_relationships=true"
  ```
- **Response Example**:
  ```json
  {
    "featureServices": [
      { "name": "recommendation_service", "project": "project1", ... },
      { "name": "scoring_service", "project": "project2", ... }
    ],
    "pagination": { "page": 1, "limit": 10, "total": 25, "totalPages": 3 },
    "relationships": { ... }
  }
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

#### Get Registry Lineage (All Projects)
- **Endpoint**: `GET /api/v1/lineage/registry/all`
- **Description**: Retrieve registry lineage (relationships and indirect relationships) for all projects.
- **Parameters**:
  - `allow_cache` (optional): Whether to allow cached data
  - `filter_object_type` (optional): Filter by object type (`dataSource`, `entity`, `featureView`, `featureService`)
  - `filter_object_name` (optional): Filter by object name
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/lineage/registry/all"
  ```
- **Response Example**:
  ```json
  {
    "relationships": [ { ... , "project": "project1" }, ... ],
    "indirect_relationships": [ { ... , "project": "project2" }, ... ]
  }
  ```

#### Get Complete Registry Data (All Projects)
- **Endpoint**: `GET /api/v1/lineage/complete/all`
- **Description**: Retrieve complete registry data, including all objects and relationships, for all projects. Optimized for UI consumption.
- **Parameters**:
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/lineage/complete/all"
  ```
- **Response Example**:
  ```json
  {
    "projects": [
      {
        "project": "project1",
        "objects": {
          "entities": [ ... ],
          "dataSources": [ ... ],
          "featureViews": [ ... ],
          "featureServices": [ ... ]
        },
        "relationships": [ ... ],
        "indirectRelationships": [ ... ]
      },
      { "project": "project2", ... }
    ]
  }
  ```

### Permissions

#### List Permissions
- **Endpoint**: `GET /api/v1/permissions`
- **Description**: Retrieve all permissions in a project
- **Parameters**:
  - `project` (required): Project name
  - `include_relationships` (optional): Include relationships for each permission
  - `allow_cache` (optional): Whether to allow cached data
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
- **Examples**:
  ```bash
  # Basic list
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/permissions?project=my_project"
  
  # With pagination and relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/permissions?project=my_project&include_relationships=true&page=1&limit=10"
  ```

#### Get Permission
- **Endpoint**: `GET /api/v1/permissions/{name}`
- **Description**: Retrieve a specific permission by name
- **Parameters**:
  - `name` (path): Permission name
  - `project` (required): Project name
  - `include_relationships` (optional): Include relationships for this permission
  - `allow_cache` (optional): Whether to allow cached data
- **Examples**:
  ```bash
  # Basic get
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/permissions/read_features?project=my_project"
  
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/permissions/read_features?project=my_project&include_relationships=true"
  ```

### Projects

#### List Projects
- **Endpoint**: `GET /api/v1/projects`
- **Description**: Retrieve all projects
- **Parameters**:
  - `allow_cache` (optional): Whether to allow cached data
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
- **Examples**:
  ```bash
  # Basic list
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/projects"
  
  # With pagination
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/projects?page=1&limit=10&sort_by=name"
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
  - `include_relationships` (optional): Include relationships for each saved dataset
  - `allow_cache` (optional): Whether to allow cached data
  - `tags` (optional): Filter by tags
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
- **Examples**:
  ```bash
  # Basic list
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/saved_datasets?project=my_project"
  
  # With pagination and relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/saved_datasets?project=my_project&include_relationships=true&page=1&limit=10"
  ```

#### Get Saved Dataset
- **Endpoint**: `GET /api/v1/saved_datasets/{name}`
- **Description**: Retrieve a specific saved dataset by name
- **Parameters**:
  - `name` (path): Saved dataset name
  - `project` (required): Project name
  - `include_relationships` (optional): Include relationships for this saved dataset
  - `allow_cache` (optional): Whether to allow cached data
- **Examples**:
  ```bash
  # Basic get
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/saved_datasets/training_data?project=my_project"
  
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/saved_datasets/training_data?project=my_project&include_relationships=true"
  ```

#### List All Saved Datasets (All Projects)
- **Endpoint**: `GET /api/v1/saved_datasets/all`
- **Description**: Retrieve all saved datasets across all projects. Each saved dataset includes a `project` field.
- **Parameters**:
  - `allow_cache` (optional): Whether to allow cached data
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
  - `include_relationships` (optional): Include relationships for each saved dataset
- **Examples**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/saved_datasets/all?page=1&limit=10&sort_by=name"
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/saved_datasets/all?include_relationships=true"
  ```
- **Response Example**:
  ```json
  {
    "savedDatasets": [
      { "name": "training_data", "project": "project1", ... },
      { "name": "validation_data", "project": "project2", ... }
    ],
    "pagination": { "page": 1, "limit": 10, "total": 25, "totalPages": 3 },
    "relationships": { ... }
  }
  ```

### Response Formats

All endpoints return JSON responses with the following general structure:

- **Success (200)**: Returns the requested data
- **Bad Request (400)**: Invalid parameters or request format
- **Unauthorized (401)**: Missing or invalid authentication token
- **Not Found (404)**: Requested resource does not exist
- **Internal Server Error (500)**: Server-side error

#### Enhanced Response Formats

The REST API now supports enhanced response formats for relationships and pagination:

**Single Object Endpoints (GET `/resource/{name}`):**

Without relationships:
```json
{
  "entity": { ... }
}
```

With relationships:
```json
{
  "entity": { ... },
  "relationships": [
    {
      "source": {
        "type": "entity",
        "name": "customer_id"
      },
      "target": {
        "type": "featureView",
        "name": "customer_features"
      }
    }
  ]
}
```

**List Endpoints (GET `/resources`):**

Without relationships or pagination:
```json
{
  "entities": [
    { "name": "customer_id", ... },
    { "name": "driver_id", ... }
  ]
}
```

With pagination only:
```json
{
  "entities": [
    { "name": "customer_id", ... },
    { "name": "driver_id", ... }
  ],
  "pagination": {
    "page": 1,
    "limit": 10,
    "total": 25,
    "totalPages": 3
  }
}
```

With relationships and pagination:
```json
{
  "entities": [
    { "name": "customer_id", ... },
    { "name": "driver_id", ... }
  ],
  "pagination": {
    "page": 1,
    "limit": 10,
    "total": 25,
    "totalPages": 3
  },
  "relationships": {
    "customer_id": [
      {
        "source": {
          "type": "entity",
          "name": "customer_id"
        },
        "target": {
          "type": "featureView",
          "name": "customer_features"
        }
      }
    ],
    "driver_id": [
      {
        "source": {
          "type": "entity",
          "name": "driver_id"
        },
        "target": {
          "type": "featureView",
          "name": "driver_features"
        }
      }
    ]
  }
}
```

### Relationships and Pagination

The REST API has been enhanced with comprehensive support for relationships and pagination across all endpoints.

#### Relationships

Relationships show how different Feast objects connect to each other, providing insight into dependencies and data lineage. When the `include_relationships` parameter is set to `true`, the API returns both direct and indirect relationships.

**Supported Object Types:**
- `entity` - Feast entities
- `dataSource` - Data sources
- `featureView` - Feature views (including regular, on-demand, and stream)
- `featureService` - Feature services
- `permission` - Permissions
- `savedDataset` - Saved datasets

**Common Relationship Patterns:**
- Feature Views → Data Sources (feature views depend on data sources)
- Feature Views → Entities (feature views use entities as join keys)
- Feature Services → Feature Views (feature services consume feature views)
- Entities → Data Sources (entities connect to data sources through feature views)
- Entities → Feature Services (entities connect to feature services through feature views)

#### Pagination

All list endpoints support pagination to improve performance and manageability of large datasets:

- **`page`** - Page number (starts from 1)
- **`limit`** - Number of items per page (maximum 100)
- **`sort_by`** - Field to sort by
- **`sort_order`** - Sort order: "asc" (ascending) or "desc" (descending)

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