# Metadata info of feature_store.yaml

## Description

The metadata info of Feast `feature_store.yaml` is:

| Key | Required | Value Type |Definition |
| ------------------------|---------------------|------------------------|------------------------| 
| project   | Y | string | name of the project |
| provider  | N | string |  |
| registry  | Y | NA | definition of registry |
| registry.path | Y | string | URI of storing registry data |
| registry.cache_ttl_seconds | N | integer | |
| registry.registry_type | N | string | type of registry server |
| registry.sqlalchemy_config_kwargs | N | NA | kwargs configuration for sql registry server|
| registry.account  | N | string | cloud account type |
| registry.user   | N | string | cloud login account |
| registry.password | N | string | cloud login password |
| registry.role    | N | string | cloud login account role |
| registry.warehouse | N | string | snowflake warehouse name |
| registry.database | N | string | snowflake db name |
| registry.schema   | N | string | snowflake schema name |
| online_store  | Y | | |
| offline_store  | Y | NA | | |
| offline_store.type | Y | string | storage type | 
| entity_key_serialization_version | N | string | |
| feature_server | N | NA | config of feature server |
| feature_server.transformation_service_endpoint | N | string | python transformation server endpoint for Go feature server |

