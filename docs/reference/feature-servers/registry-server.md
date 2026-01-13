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
  - `entity` (optional): Filter feature views by entity name
  - `feature` (optional): Filter feature views by feature name
  - `feature_service` (optional): Filter feature views by feature service name
  - `data_source` (optional): Filter feature views by data source name
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
  
  # Filter by entity
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views?project=my_project&entity=user"
  
  # Filter by feature
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views?project=my_project&feature=age"
  
  # Filter by data source
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views?project=my_project&data_source=user_profile_source"
  
  # Filter by feature service
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views?project=my_project&feature_service=user_service"
  
  # Multiple filters combined
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_views?project=my_project&entity=user&feature=age"
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

### Features

#### List Features
- **Endpoint**: `GET /api/v1/features`
- **Description**: Retrieve all features in a project
- **Parameters**:
  - `project` (required): Project name
  - `feature_view` (optional): Filter by feature view name
  - `name` (optional): Filter by feature name
  - `include_relationships` (optional): Include relationships for each feature (relationships are keyed by feature name)
  - `allow_cache` (optional): Whether to allow cached data
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
- **Examples**:
  ```bash
  # Basic list
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/features?project=my_project"

  # With pagination and relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/features?project=my_project&include_relationships=true&page=1&limit=5"
  ```
- **Response Example**:
  ```json
  {
    "features": [
      { "name": "conv_rate", "featureView": "driver_hourly_stats_fresh", "type": "Float32" },
      { "name": "acc_rate", "featureView": "driver_hourly_stats_fresh", "type": "Float32" },
      { "name": "avg_daily_trips", "featureView": "driver_hourly_stats_fresh", "type": "Int64" },
      { "name": "conv_rate", "featureView": "driver_hourly_stats", "type": "Float32" },
      { "name": "acc_rate", "featureView": "driver_hourly_stats", "type": "Float32" }
    ],
    "pagination": {
      "page": 1,
      "limit": 5,
      "totalCount": 10,
      "totalPages": 2,
      "hasNext": true
    },
    "relationships": {
      "conv_rate": [
        { "source": { "type": "feature", "name": "conv_rate" }, "target": { "type": "featureView", "name": "driver_hourly_stats_fresh" } },
        { "source": { "type": "feature", "name": "conv_rate" }, "target": { "type": "featureView", "name": "driver_hourly_stats" } },
        { "source": { "type": "feature", "name": "conv_rate" }, "target": { "type": "featureService", "name": "driver_activity_v1" } }
      ]
    }
  }
  ```

#### Get Feature
- **Endpoint**: `GET /api/v1/features/{feature_view}/{name}`
- **Description**: Retrieve a specific feature by feature view and name
- **Parameters**:
  - `feature_view` (path): Feature view name
  - `name` (path): Feature name
  - `project` (required): Project name
  - `include_relationships` (optional): Include relationships for this feature
  - `allow_cache` (optional): Whether to allow cached data
- **Examples**:
  ```bash
  # Basic get
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/features/driver_hourly_stats/conv_rate?project=my_project"

  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/features/driver_hourly_stats/conv_rate?project=my_project&include_relationships=true"
  ```
- **Response Example**:
  ```json
  {
    "name": "conv_rate",
    "featureView": "driver_hourly_stats",
    "type": "Float32",
    "relationships": [
      { "source": { "type": "feature", "name": "conv_rate" }, "target": { "type": "featureView", "name": "driver_hourly_stats_fresh" } },
      { "source": { "type": "feature", "name": "conv_rate" }, "target": { "type": "featureView", "name": "driver_hourly_stats" } },
      { "source": { "type": "feature", "name": "conv_rate" }, "target": { "type": "featureService", "name": "driver_activity_v1" } }
    ]
  }
  ```

#### List All Features (All Projects)
- **Endpoint**: `GET /api/v1/features/all`
- **Description**: Retrieve all features across all projects. Each feature includes a `project` field.
- **Parameters**:
  - `page` (optional): Page number for pagination
  - `limit` (optional): Number of items per page
  - `sort_by` (optional): Field to sort by
  - `sort_order` (optional): Sort order ("asc" or "desc")
  - `include_relationships` (optional): Include relationships for each feature
  - `allow_cache` (optional): Whether to allow cached data
- **Examples**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/features/all?page=1&limit=5&sort_by=name"
  # With relationships
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/features/all?include_relationships=true"
  ```
- **Response Example**:
  ```json
  {
    "features": [
      { "name": "conv_rate", "featureView": "driver_hourly_stats_fresh", "type": "Float32", "project": "multiproject" },
      { "name": "acc_rate", "featureView": "driver_hourly_stats_fresh", "type": "Float32", "project": "multiproject" },
      { "name": "avg_daily_trips", "featureView": "driver_hourly_stats_fresh", "type": "Int64", "project": "multiproject" },
      { "name": "conv_rate", "featureView": "driver_hourly_stats", "type": "Float32", "project": "multiproject" },
      { "name": "acc_rate", "featureView": "driver_hourly_stats", "type": "Float32", "project": "multiproject" }
    ],
    "pagination": {
      "page": 1,
      "limit": 5,
      "total_count": 20,
      "total_pages": 4,
      "has_next": true,
      "has_previous": false
    },
    "relationships": {
      "conv_rate": [
        { "source": { "type": "feature", "name": "conv_rate" }, "target": { "type": "featureView", "name": "driver_hourly_stats" } },
        { "source": { "type": "feature", "name": "conv_rate" }, "target": { "type": "featureView", "name": "driver_hourly_stats_fresh" } },
        { "source": { "type": "feature", "name": "conv_rate" }, "target": { "type": "featureService", "name": "driver_activity_v3" } }
      ]
    }
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
  - `feature_view` (optional): Filter feature services by feature view name
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
  
  # Filter by feature view
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/feature_services?project=my_project&feature_view=user_profile"
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
  - `filter_object_type` (optional): Filter by object type (`dataSource`, `entity`, `featureView`, `featureService`, `feature`)
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
  - `object_type` (path): Type of object (`dataSource`, `entity`, `featureView`, `featureService`, `feature`)
  - `object_name` (path): Name of the object
  - `project` (required): Project name
  - `include_indirect` (optional): Whether to include indirect relationships
  - `allow_cache` (optional): Whether to allow cached data
- **Example**:
  ```bash
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/lineage/objects/feature/conv_rate?project=my_project&include_indirect=true"
  ```
- **Response Example**:
  ```json
  {
    "relationships": [
      { "source": { "type": "feature", "name": "conv_rate" }, "target": { "type": "featureView", "name": "driver_hourly_stats_fresh" } },
      { "source": { "type": "feature", "name": "conv_rate" }, "target": { "type": "featureView", "name": "driver_hourly_stats" } }
    ],
    "pagination": { "totalCount": 2, "totalPages": 1 }
  }
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
- **Response Example**:
  ```json
  {
    "project": "multiproject",
    "objects": {
      "entities": [ ... ],
      "dataSources": [ ... ],
      "featureViews": [ ... ],
      "featureServices": [ ... ],
      "features": [
        { "name": "conv_rate", "featureView": "driver_hourly_stats_fresh", "type": "Float32" },
        { "name": "acc_rate", "featureView": "driver_hourly_stats_fresh", "type": "Float32" },
        { "name": "avg_daily_trips", "featureView": "driver_hourly_stats_fresh", "type": "Int64" },
        { "name": "conv_rate", "featureView": "driver_hourly_stats", "type": "Float32" },
        { "name": "acc_rate", "featureView": "driver_hourly_stats", "type": "Float32" },
        { "name": "conv_rate_plus_val1", "featureView": "transformed_conv_rate_fresh", "type": "Float64" },
        { "name": "conv_rate_plus_val2", "featureView": "transformed_conv_rate_fresh", "type": "Float64" },
        { "name": "conv_rate_plus_val1", "featureView": "transformed_conv_rate", "type": "Float64" },
        { "name": "conv_rate_plus_val2", "featureView": "transformed_conv_rate", "type": "Float64" }
      ]
    },
    "relationships": [ ... ],
    "indirectRelationships": [ ... ],
    "pagination": {
      "features": { "totalCount": 10, "totalPages": 1 },
      ...
    }
  }
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

### Error Handling

The REST API provides consistent error responses with HTTP status codes included in the JSON response body. All error responses follow this format:

```json
{
  "status_code": 404,
  "detail": "Entity 'user_id' does not exist in project 'demo_project'",
  "error_type": "FeastObjectNotFoundException"
}
```

#### Error Response Fields

- **`status_code`**: The HTTP status code (e.g., 404, 422, 500)
- **`detail`**: Human-readable error message describing the issue
- **`error_type`**: The specific type of error that occurred

#### HTTP Status Code Mapping

| HTTP Status | Error Type | Description | Common Causes |
|-------------|------------|-------------|---------------|
| **400** | `HTTPException` | Bad Request | Invalid request format or parameters |
| **401** | `HTTPException` | Unauthorized | Missing or invalid authentication token |
| **403** | `FeastPermissionError` | Forbidden | Insufficient permissions to access the resource |
| **404** | `FeastObjectNotFoundException` | Not Found | Requested entity, feature view, data source, etc. does not exist |
| **422** | `ValidationError` / `RequestValidationError` / `ValueError` / `PushSourceNotFoundException` | Unprocessable Entity | Validation errors, missing required parameters, or invalid input |
| **500** | `InternalServerError` | Internal Server Error | Unexpected server-side errors |


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
- `feature` - Features (including features from on-demand feature views)
- `featureService` - Feature services
- `permission` - Permissions
- `savedDataset` - Saved datasets

**Common Relationship Patterns:**
- Feature Views → Data Sources (feature views depend on data sources)
- Feature Views → Entities (feature views use entities as join keys)
- Feature Services → Feature Views (feature services consume feature views)
- Features → Feature Views (features belong to feature views, including on-demand feature views)
- Features → Feature Services (features are consumed by feature services)
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

### Metrics

#### Get Resource Counts
- **Endpoint**: `GET /api/v1/metrics/resource_counts`
- **Description**: Retrieve counts of registry objects (entities, data sources, feature views, etc.) for a project or across all projects.
- **Parameters**:
  - `project` (optional): Project name to filter resource counts (if not provided, returns counts for all projects)
- **Examples**:
  ```bash
  # Get counts for specific project
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/metrics/resource_counts?project=my_project"
  
  # Get counts for all projects
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/metrics/resource_counts"
  ```
- **Response Example** (single project):
  ```json
  {
    "project": "my_project",
    "counts": {
      "entities": 5,
      "dataSources": 3,
      "savedDatasets": 2,
      "features": 12,
      "featureViews": 4,
      "featureServices": 2
    }
  }
  ```
- **Response Example** (all projects):
  ```json
  {
    "total": {
      "entities": 15,
      "dataSources": 8,
      "savedDatasets": 5,
      "features": 35,
      "featureViews": 12,
      "featureServices": 6
    },
    "perProject": {
      "project_a": {
        "entities": 5,
        "dataSources": 3,
        "savedDatasets": 2,
        "features": 12,
        "featureViews": 4,
        "featureServices": 2
      },
      "project_b": {
        "entities": 10,
        "dataSources": 5,
        "savedDatasets": 3,
        "features": 23,
        "featureViews": 8,
        "featureServices": 4
      }
    }
  }
  ```

#### Get Recently Visited Objects
- **Endpoint**: `GET /api/v1/metrics/recently_visited`
- **Description**: Retrieve the most recently visited registry objects for the authenticated user in a project.
- **Parameters**:
  - `project` (optional): Project name to filter recent visits (defaults to current project)
  - `object` (optional): Object type to filter recent visits (e.g., entities, features, feature_services)
  - `page` (optional): Page number for pagination (starts from 1)
  - `limit` (optional): Number of items per page (maximum 100)
  - `sort_by` (optional): Field to sort by (e.g., timestamp, path, object)
  - `sort_order` (optional): Sort order: "asc" or "desc" (default: "asc")
- **Examples**:
  ```bash
  # Get all recent visits for a project
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/metrics/recently_visited?project=my_project"
  
  # Get recent visits with pagination
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/metrics/recently_visited?project=my_project&page=1&limit=10"
  
  # Get recent visits filtered by object type
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/metrics/recently_visited?project=my_project&object=entities"
  
  # Get recent visits sorted by timestamp descending
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/metrics/recently_visited?project=my_project&sort_by=timestamp&sort_order=desc"
  ```
- **Response Example** (without pagination):
  ```json
  {
    "visits": [
      {
        "path": "/api/v1/entities/driver",
        "timestamp": "2024-07-18T12:34:56.789Z",
        "project": "my_project",
        "user": "alice",
        "object": "entities",
        "object_name": "driver",
        "method": "GET"
      },
      {
        "path": "/api/v1/feature_services/user_service",
        "timestamp": "2024-07-18T12:30:45.123Z",
        "project": "my_project",
        "user": "alice",
        "object": "feature_services",
        "object_name": "user_service",
        "method": "GET"
      }
    ],
    "pagination": {
      "totalCount": 2
    }
  }
  ```
- **Response Example** (with pagination):
  ```json
  {
    "visits": [
      {
        "path": "/api/v1/entities/driver",
        "timestamp": "2024-07-18T12:34:56.789Z",
        "project": "my_project",
        "user": "alice",
        "object": "entities",
        "object_name": "driver",
        "method": "GET"
      }
    ],
    "pagination": {
      "page": 1,
      "limit": 10,
      "totalCount": 25,
      "totalPages": 3,
      "hasNext": true,
      "hasPrevious": false
    }
  }
  ```

**Note**: Recent visits are automatically logged when users access registry objects via the REST API. The logging behavior can be configured through the `feature_server.recent_visit_logging` section in `feature_store.yaml` (see configuration section below).


### Search API

#### Search Resources
- **Endpoint**: `GET /api/v1/search`
- **Description**: Search across all Feast resources including entities, feature views, features, feature services, data sources, and saved datasets. Supports cross-project search, fuzzy matching, relevance scoring, and advanced filtering.
- **Parameters**:
  - `query` (required): Search query string. Searches in resource names, descriptions, and tags. Empty string returns all resources.
  - `projects` (optional): List of project names to search in. If not specified, searches all projects
  - `allow_cache` (optional, default: `true`): Whether to allow cached data
  - `tags` (optional): Filter results by tags in key:value format (e.g., `tags=environment:production&tags=team:ml`)
  - `page` (optional, default: `1`): Page number for pagination (starts from 1)
  - `limit` (optional, default: `50`, max: `100`): Number of items per page
  - `sort_by` (optional, default: `match_score`): Field to sort by (`match_score`, `name`, or `type`)
  - `sort_order` (optional, default: `desc`): Sort order ("asc" or "desc")
- **Search Algorithm**:
  - **Exact name match**: Highest priority (score: 100)
  - **Description match**: High priority (score: 80) 
  - **Feature name match**: Medium-high priority (score: 50)
  - **Tag match**: Medium priority (score: 60)
  - **Fuzzy name match**: Lower priority (score: 40, similarity threshold: 50%)
- **Examples**:
  ```bash
  # Basic search across all projects
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/search?query=user"
  
  # Search in specific projects
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/search?query=driver&projects=ride_sharing&projects=analytics"
  
  # Search with tag filtering
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/search?query=features&tags=environment:production&tags=team:ml"
  
  # Search with pagination and sorting
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/search?query=conv_rate&page=1&limit=10&sort_by=name&sort_order=asc"
  
  # Empty query to list all resources with filtering
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/search?query=&projects=my_project&page=1&limit=20"
  ```
- **Response Example**:
  ```json
  {
    "query": "user",
    "projects_searched": ["project1", "project2"],
    "results": [
      {
        "type": "entity",
        "name": "user_id",
        "description": "Primary identifier for users",
        "project": "project1",
        "match_score": 100,
        "matched_tags": {}
      },
      {
        "type": "featureView",
        "name": "user_features",
        "description": "User demographic and behavioral features",
        "project": "project1",
        "match_score": 100,
        "matched_tags": {"team": "user_analytics"}
      },
      {
        "type": "feature",
        "name": "user_age",
        "description": "Age of the user in years",
        "project": "project1",
        "featureView": "user_features",
        "match_score": 80,
        "matched_tags": {}
      },
      {
        "type": "dataSource",
        "name": "user_analytics",
        "description": "Analytics data for user behavior tracking",
        "project": "project2",
        "match_score": 80,
        "matched_tags": {"source": "user_data"}
      }
    ],
    "pagination": {
      "page": 1,
      "limit": 50,
      "totalCount": 4,
      "totalPages": 1,
      "hasNext": false,
      "hasPrevious": false
    },
    "errors": []
  }
  ```
- **Project Handling**:
  - **No projects specified**: Searches all available projects
  - **Single project**: Searches only that project (includes warning if project doesn't exist)
  - **Multiple projects**: Searches only existing projects, includes warnings about non-existent ones
  - **Empty projects list**: Treated as search all projects
- **Error Responses**:
  ```json
  // Invalid sort_by parameter (HTTP 400)
  {
    "detail": "Invalid sort_by parameter: 'invalid_field'. Valid options are: ['match_score', 'name', 'type']"
  }
  
  // Invalid sort_order parameter (HTTP 400)
  {
    "detail": "Invalid sort_order parameter: 'invalid_order'. Valid options are: ['asc', 'desc']"
  }
  
  // Invalid pagination limit above maximum (HTTP 400)
  {
    "detail": "Invalid limit parameter: '150'. Must be less than or equal to 100"
  }
  
  // Missing required query parameter (HTTP 422)
  {
    "detail": [
      {
        "type": "missing",
        "loc": ["query_params", "query"],
        "msg": "Field required"
      }
    ]
  }
  
  // Successful response with warnings
  {
    "query": "user",
    "projects_searched": ["existing_project"],
    "results": [],
    "pagination": {
      "page": 1,
      "limit": 50,
      "totalCount": 0,
      "totalPages": 0
    },
    "errors": ["Following projects do not exist: nonexistent_project"]
  }

  // Successful response but empty results
  {
    "query": "user",
    "projects_searched": ["existing_project"],
    "results": [],
    "pagination": {
      "page": 1,
      "limit": 50,
      "totalCount": 0,
      "totalPages": 0
    },
    "errors": []
  }
  ```
---
#### Get Popular Tags
- **Endpoint**: `GET /api/v1/metrics/popular_tags`
- **Description**: Discover Feature Views by popular tags. Returns the most popular tags (tags assigned to maximum number of feature views) with their associated feature views. If no project is specified, returns popular tags across all projects.
- **Parameters**:
  - `project` (optional): Project name for popular tags (returns all projects if not specified)
  - `limit` (optional, default: 4): Number of popular tags to return
  - `allow_cache` (optional, default: true): Whether to allow cached responses
- **Examples**:
  ```bash
  # Basic usage (all projects)
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/metrics/popular_tags"
  
  # Specific project
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/metrics/popular_tags?project=my_project"
  
  # Custom limit
  curl -H "Authorization: Bearer <token>" \
    "http://localhost:6572/api/v1/metrics/popular_tags?project=my_project&limit=3"
  ```
- **Response Model**: `PopularTagsResponse`
- **Response Example**:
  ```json
  {
    "popular_tags": [
      {
        "tag_key": "environment",
        "tag_value": "production",
        "feature_views": [
          {
            "name": "user_features",
            "project": "my_project"
          },
          {
            "name": "order_features",
            "project": "my_project"
          }
        ],
        "total_feature_views": 2
      },
      {
        "tag_key": "team",
        "tag_value": "ml_team",
        "feature_views": [
          {
            "name": "user_features",
            "project": "my_project"
          }
        ],
        "total_feature_views": 1
      }
    ],
    "metadata": {
      "totalFeatureViews": 3,
      "totalTags": 2,
      "limit": 4
    }
  }
  ```

**Response Models:**
- `FeatureViewInfo`: Contains feature view name and project
- `PopularTagInfo`: Contains tag information and associated feature views
- `PopularTagsMetadata`: Contains metadata about the response
- `PopularTagsResponse`: Main response model containing popular tags and metadata

## Registry Server Configuration: Recent Visit Logging

The registry server supports configuration of recent visit logging via the `feature_server` section in `feature_store.yaml`.

**Example:**
```yaml
feature_server:
  type: local
  recent_visit_logging:
    limit: 100  # Number of recent visits to store per user
    log_patterns:
      - ".*/entities/(?!all$)[^/]+$"
      - ".*/data_sources/(?!all$)[^/]+$"
      - ".*/feature_views/(?!all$)[^/]+$"
      - ".*/features/(?!all$)[^/]+$"
      - ".*/feature_services/(?!all$)[^/]+$"
      - ".*/saved_datasets/(?!all$)[^/]+$"
      - ".*/custom_api/.*"
```

**Configuration Options:**
- **recent_visit_logging.limit**: Maximum number of recent visits to store per user (default: 100).
- **recent_visit_logging.log_patterns**: List of regex patterns for API paths to log as recent visits.

**Default Log Patterns:**
- `.*/entities/(?!all$)[^/]+$` - Individual entity endpoints
- `.*/data_sources/(?!all$)[^/]+$` - Individual data source endpoints  
- `.*/feature_views/(?!all$)[^/]+$` - Individual feature view endpoints
- `.*/features/(?!all$)[^/]+$` - Individual feature endpoints
- `.*/feature_services/(?!all$)[^/]+$` - Individual feature service endpoints
- `.*/saved_datasets/(?!all$)[^/]+$` - Individual saved dataset endpoints

**Behavior:**
- Only requests matching one of the `log_patterns` will be tracked
- Only the most recent `limit` visits per user are stored
- Metrics endpoints (`/metrics/*`) are automatically excluded from logging to prevent circular references
- Visit data is stored per user and per project in the registry metadata

