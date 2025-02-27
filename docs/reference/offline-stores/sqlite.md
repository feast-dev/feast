# SQLite Offline Store

The SQLite offline store provides support for using SQLite as a backend for historical feature values. This is useful for local development and testing, as well as for small-scale production deployments.

## Example Configuration

```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: sqlite
    path: data/offline_store.db  # Optional: defaults to ":memory:" for in-memory database
    connection_timeout: 900      # Optional: connection timeout in seconds
online_store:
    type: sqlite
    path: data/online_store.db
```
## Configuration Options
The following configuration options are available for the SQLite offline store:
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| type | string | "sqlite" | Specifies the offline store type as "sqlite" |
| path | string | ":memory:" | Path to the SQLite database file. Use ":memory:" for an in-memory database |
| connection_timeout | int | None | Optional connection timeout in seconds |
## Data Type Mapping
The SQLite offline store maps SQLite data types to Arrow types as follows:
| SQLite Type | Arrow Type |
|------------|------------|
| INTEGER | Int64 |
| REAL | Float64 |
| TEXT | String |
| BLOB | Binary |
| NUMERIC | Float64 |
| BOOLEAN | Boolean |
| DATETIME | Timestamp[us] |
| DATE | Date32 |
| TIME | Time64[us] |
| VARCHAR | String |
| CHAR | String |
