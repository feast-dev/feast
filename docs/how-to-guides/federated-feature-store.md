# Federated Feature Store Setup

A federated feature store architecture allows multiple teams to contribute to a shared Feast registry while maintaining clear ownership boundaries. This pattern is particularly useful for organizations with multiple data science teams or projects that need to share features while preserving autonomy.

## Overview

In a federated setup, you typically have:

- **Platform Repository (Central)**: A single repository managed by the platform team that acts as the source of truth for core infrastructure objects like entities, data sources, and batch/stream feature views.
- **Team Repositories (Distributed)**: Team-owned repositories for training pipelines and inference services. Teams define their own FeatureServices and On-Demand Feature Views (ODFVs) and apply them safely without affecting objects they don't own.

## Key Concept: Understanding `partial=True` vs `partial=False`

The `partial` parameter in `FeatureStore.apply()` controls how Feast handles object deletions:

- **`partial=True` (default)**: Only adds or updates the specified objects. Does NOT delete any existing objects in the registry. This is safe for teams to use when they only want to manage a subset of objects.
- **`partial=False`**: Performs a full synchronization - adds, updates, AND deletes objects. Objects not in the provided list (but tracked by Feast) will be removed from the registry. This should only be used by the platform team with full control.

```python
# Team repository - safe partial apply
# Only adds/updates the specified FeatureService
# Does NOT delete any other objects
store.apply([my_feature_service], partial=True)

# Platform repository - full sync with deletion capability
# Syncs all objects and can remove objects via objects_to_delete
store.apply(all_objects, objects_to_delete=objects_to_remove, partial=False)
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     Platform Repository                         │
│                    (Central, platform team)                     │
│                                                                 │
│  ├── feature_store.yaml  (shared registry config)              │
│  ├── entities.py         (Entity definitions)                  │
│  ├── data_sources.py     (DataSource definitions)              │
│  └── feature_views.py    (FeatureView, StreamFeatureView)      │
│                                                                 │
│  Applies with partial=False (full control)                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Shared Registry (e.g., S3, GCS, SQL)
                              │
              ┌───────────────┼───────────────┐
              │               │               │
    ┌─────────▼────────┐ ┌───▼────────┐ ┌────▼──────────┐
    │  Team A Repo     │ │  Team B    │ │  Team C       │
    │  (Distributed)   │ │  Repo      │ │  Repo         │
    │                  │ │            │ │               │
    │  ├── fs.yaml     │ │  ├── ...   │ │  ├── ...      │
    │  ├── odfvs.py    │ │  └── ...   │ │  └── ...      │
    │  └── services.py │ │            │ │               │
    │                  │ │            │ │               │
    │  partial=True    │ │ partial=   │ │ partial=True  │
    │  (safe add)      │ │ True       │ │               │
    └──────────────────┘ └────────────┘ └───────────────┘
```

## Setting Up the Platform Repository

The platform repository maintains full control over core Feast objects.

### 1. Platform `feature_store.yaml`

```yaml
project: my_feast_project
registry: s3://my-bucket/feast-registry/registry.db
provider: aws
online_store:
    type: dynamodb
    region: us-west-2
offline_store:
    type: snowflake
    account: my_account
    database: FEAST_DB
    warehouse: FEAST_WH
```

### 2. Platform Repository Structure

```
platform_repo/
├── feature_store.yaml
├── entities.py
├── data_sources.py
├── feature_views.py
└── apply.py
```

### 3. Platform Object Definitions

**entities.py**
```python
from feast import Entity

# Core entities managed by platform team
user = Entity(
    name="user_id",
    description="User entity",
    join_keys=["user_id"]
)

driver = Entity(
    name="driver_id", 
    description="Driver entity",
    join_keys=["driver_id"]
)
```

**data_sources.py**
```python
from feast import SnowflakeSource

# Core data sources managed by platform team
user_features_source = SnowflakeSource(
    database="FEAST_DB",
    schema="FEATURES",
    table="USER_FEATURES",
    timestamp_field="event_timestamp",
)

driver_stats_source = SnowflakeSource(
    database="FEAST_DB",
    schema="FEATURES", 
    table="DRIVER_STATS",
    timestamp_field="event_timestamp",
)
```

**feature_views.py**
```python
from datetime import timedelta
from feast import FeatureView, Field
from feast.types import Float32, Int64
from entities import user, driver
from data_sources import user_features_source, driver_stats_source

# Core feature views managed by platform team
user_features = FeatureView(
    name="user_features",
    entities=[user],
    ttl=timedelta(days=1),
    schema=[
        Field(name="age", dtype=Int64),
        Field(name="account_balance", dtype=Float32),
    ],
    source=user_features_source,
)

driver_stats = FeatureView(
    name="driver_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
    ],
    source=driver_stats_source,
)
```

### 4. Platform Apply Script

**apply.py**
```python
from feast import FeatureStore
from entities import user, driver
from data_sources import user_features_source, driver_stats_source
from feature_views import user_features, driver_stats

# Initialize the feature store
store = FeatureStore(repo_path=".")

# Collect all objects managed by platform
all_objects = [
    user,
    driver,
    user_features_source,
    driver_stats_source,
    user_features,
    driver_stats,
]

# Apply with partial=False to maintain full control
# This allows the platform team to manage deletions
store.apply(all_objects, partial=False)

print("Platform objects applied successfully!")
```

## Setting Up Team Repositories

Team repositories add their own FeatureServices and ODFVs without interfering with platform-managed objects.

### 1. Team `feature_store.yaml`

Teams use the **same registry configuration** as the platform:

```yaml
project: my_feast_project  # Same project as platform
registry: s3://my-bucket/feast-registry/registry.db  # Same registry
provider: aws
online_store:
    type: dynamodb
    region: us-west-2
offline_store:
    type: snowflake
    account: my_account
    database: FEAST_DB
    warehouse: FEAST_WH
```

### 2. Team Repository Structure

```
team_a_repo/
├── feature_store.yaml
├── on_demand_views.py
├── feature_services.py
└── apply.py
```

### 3. Team Object Definitions

**on_demand_views.py**
```python
from feast import FeatureStore, Field, OnDemandFeatureView, RequestSource
from feast.types import Float32

# Team A's custom on-demand feature view
# Uses platform-managed feature views as inputs
@OnDemandFeatureView(
    sources=[
        "user_features:age",  # References platform feature view
        RequestSource(schema=[Field(name="current_year", dtype=Float32)]),
    ],
    schema=[Field(name="years_until_retirement", dtype=Float32)],
)
def user_age_odfv(inputs):
    """Calculate years until retirement."""
    return {"years_until_retirement": 65 - inputs["age"]}
```

**feature_services.py**
```python
from feast import FeatureService, FeatureStore

# Initialize feature store to get references
store = FeatureStore(repo_path=".")

# Team A's feature service for their ML model
user_model_features = FeatureService(
    name="user_model_v1",
    features=[
        "user_features",  # Platform feature view
        "user_age_odfv",  # Team's ODFV
    ],
)
```

### 4. Team Apply Script

**apply.py**
```python
from feast import FeatureStore
from on_demand_views import user_age_odfv
from feature_services import user_model_features

# Initialize the feature store
store = FeatureStore(repo_path=".")

# Collect team's objects
team_objects = [
    user_age_odfv,
    user_model_features,
]

# Apply with partial=True (safe for teams)
# Only adds/updates team's objects, does NOT delete anything
store.apply(team_objects, partial=True)

print("Team objects applied successfully!")
```

## Ownership Boundaries

| Object Type | Platform Repository | Team Repositories |
|-------------|---------------------|-------------------|
| Entities | ✅ Yes | ❌ No |
| Data Sources | ✅ Yes | ❌ No |
| Feature Views | ✅ Yes | ❌ No |
| Stream Feature Views | ✅ Yes | ❌ No |
| Feature Services | Optional | ✅ Yes |
| On-Demand Feature Views | Optional | ✅ Yes |

## Strategies for Avoiding Registry Drift

### 1. Naming Conventions

Establish clear naming conventions to prevent collisions:

```python
# Platform objects: simple names
user_features = FeatureView(name="user_features", ...)

# Team objects: prefixed with team name
team_a_user_service = FeatureService(name="team_a_user_model", ...)
team_b_driver_service = FeatureService(name="team_b_driver_model", ...)
```

### 2. CI/CD Integration

Use CI/CD pipelines to automate applies and catch errors early:

```yaml
# .github/workflows/apply-platform.yml
name: Apply Platform Features
on:
  push:
    branches: [main]
    paths:
      - 'platform_repo/**'

jobs:
  apply:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Apply platform features
        run: |
          cd platform_repo
          pip install feast[aws,snowflake]
          python apply.py
```

```yaml
# .github/workflows/apply-team-a.yml
name: Apply Team A Features
on:
  push:
    branches: [main]
    paths:
      - 'team_a_repo/**'

jobs:
  apply:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Apply team A features
        run: |
          cd team_a_repo
          pip install feast[aws,snowflake]
          python apply.py
```

### 3. Registry Validation

Implement validation to check for ownership violations:

```python
def validate_team_objects(team_name, objects):
    """Validate that team only creates allowed object types."""
    allowed_types = (FeatureService, OnDemandFeatureView)
    
    for obj in objects:
        if not isinstance(obj, allowed_types):
            raise ValueError(
                f"Team {team_name} cannot create {type(obj).__name__}. "
                f"Only FeatureServices and ODFVs are allowed."
            )
        
        # Check naming convention
        if not obj.name.startswith(f"{team_name}_"):
            raise ValueError(
                f"Team {team_name} objects must be prefixed with '{team_name}_'. "
                f"Got: {obj.name}"
            )

# In team apply script
validate_team_objects("team_a", team_objects)
store.apply(team_objects, partial=True)
```

### 4. Read-Only Access for Teams

Configure infrastructure permissions so teams have:
- **Read access** to the registry (to get platform objects)
- **Write access** only for their specific object types
- **No delete permissions** on registry objects

## Multi-Tenant Configuration with Schema Isolation

For stronger isolation, use separate database schemas per team:

### Platform Configuration

```yaml
# platform_repo/feature_store.yaml
project: my_feast_project
registry: s3://my-bucket/feast-registry/registry.db
provider: aws
offline_store:
    type: snowflake
    account: my_account
    database: FEAST_DB
    schema: PLATFORM  # Platform schema
    warehouse: FEAST_WH
```

### Team Configuration with Custom Schema

```yaml
# team_a_repo/feature_store.yaml
project: my_feast_project
registry: s3://my-bucket/feast-registry/registry.db
provider: aws
offline_store:
    type: snowflake
    account: my_account
    database: FEAST_DB
    schema: TEAM_A  # Team-specific schema
    warehouse: FEAST_WH
```

This approach allows teams to materialize features to their own schemas while sharing the central registry.

## Complete Working Example

Here's a complete example showing both platform and team repositories in action:

### Platform Repository

```python
# platform_repo/apply.py
from feast import Entity, FeatureStore, FeatureView, Field, SnowflakeSource
from feast.types import Float32, Int64
from datetime import timedelta

# Initialize feature store
store = FeatureStore(repo_path=".")

# Define entities
customer = Entity(name="customer_id", join_keys=["customer_id"])

# Define data source
customer_source = SnowflakeSource(
    database="FEAST_DB",
    schema="RAW",
    table="CUSTOMER_FEATURES",
    timestamp_field="event_timestamp",
)

# Define feature view
customer_features = FeatureView(
    name="customer_features",
    entities=[customer],
    ttl=timedelta(days=7),
    schema=[
        Field(name="total_purchases", dtype=Int64),
        Field(name="avg_order_value", dtype=Float32),
    ],
    source=customer_source,
)

# Apply all platform objects with full control
platform_objects = [customer, customer_source, customer_features]
store.apply(platform_objects, partial=False)
print("✅ Platform objects applied")
```

### Team Repository

```python
# team_marketing_repo/apply.py
from feast import FeatureStore, FeatureService, Field, OnDemandFeatureView, RequestSource
from feast.types import Float32, Int64

# Initialize feature store (same registry as platform)
store = FeatureStore(repo_path=".")

# Define on-demand feature view
@OnDemandFeatureView(
    sources=[
        "customer_features:total_purchases",
        "customer_features:avg_order_value",
        RequestSource(schema=[Field(name="current_cart_value", dtype=Float32)]),
    ],
    schema=[
        Field(name="purchase_propensity_score", dtype=Float32),
    ],
)
def marketing_propensity(inputs):
    """Calculate purchase propensity based on history and current cart."""
    purchases = inputs["total_purchases"]
    avg_value = inputs["avg_order_value"]
    cart_value = inputs["current_cart_value"]
    
    # Simple propensity calculation
    propensity = (purchases * 0.3 + (cart_value / max(avg_value, 1)) * 0.7)
    return {"purchase_propensity_score": min(propensity, 100.0)}

# Define feature service for team's ML model
marketing_campaign_service = FeatureService(
    name="team_marketing_campaign_model",
    features=[
        "customer_features",  # Platform feature view
        "marketing_propensity",  # Team's ODFV
    ],
)

# Apply team objects safely with partial=True
team_objects = [marketing_propensity, marketing_campaign_service]
store.apply(team_objects, partial=True)
print("✅ Team marketing objects applied")
```

### Using the Features

```python
# In team's training or inference code
from feast import FeatureStore

store = FeatureStore(repo_path=".")

# Get online features using team's feature service
features = store.get_online_features(
    features="team_marketing_campaign_model",
    entity_rows=[
        {"customer_id": "C123", "current_cart_value": 150.0}
    ],
).to_dict()

print(features)
# Output includes both platform features and team's ODFV:
# {
#   "customer_id": ["C123"],
#   "total_purchases": [42],
#   "avg_order_value": [125.50],
#   "purchase_propensity_score": [42.3]
# }
```

## Best Practices

1. **Platform Team Responsibilities**:
   - Maintain core entities and data sources
   - Define and manage batch/stream feature views
   - Use `partial=False` to maintain full control
   - Document available features for teams to use

2. **Team Responsibilities**:
   - Always use `partial=True` when applying objects
   - Follow naming conventions (prefix with team name)
   - Only create FeatureServices and ODFVs
   - Reference platform feature views by name, don't redefine them

3. **Registry Management**:
   - Use a centralized remote registry (S3, GCS, SQL)
   - Implement access controls at the infrastructure level
   - Set up monitoring for registry changes
   - Regular backups of registry state

4. **Testing Strategy**:
   - Use staging registries for testing changes
   - Validate team applies don't modify platform objects
   - Test feature retrieval end-to-end before production deployment

5. **Documentation**:
   - Maintain a catalog of available platform features
   - Document team ownership of FeatureServices and ODFVs
   - Keep architecture diagrams up to date

## Troubleshooting

### Problem: Team accidentally deleted platform objects

**Cause**: Team used `partial=False` instead of `partial=True`.

**Solution**: 
- Restore from registry backup
- Add validation in team CI/CD to prevent `partial=False`
- Set registry permissions to prevent teams from deleting objects

### Problem: Feature service references unknown feature view

**Cause**: Feature view not yet applied by platform team.

**Solution**:
- Ensure platform applies are run before team applies
- Use CI/CD dependencies to enforce ordering
- Add validation to check that referenced feature views exist

### Problem: Registry conflicts between teams

**Cause**: Teams using same object names.

**Solution**:
- Enforce naming conventions with prefixes
- Add validation to check for name collisions
- Consider using separate projects per team (advanced)

## Related Resources

- [Structuring Feature Repos](structuring-repos.md) - Multi-environment setup patterns
- [Remote Registry](../reference/registries/remote.md) - Remote registry configuration
- [Registry Server](../reference/feature-servers/registry-server.md) - Registry server setup
- [Feature Store API Reference](http://rtd.feast.dev/en/stable/index.html#feast.FeatureStore.apply) - `apply()` method documentation

## Summary

A federated feature store architecture enables scalable collaboration across multiple teams:

- The **platform team** maintains core objects (entities, sources, views) with `partial=False`
- **Individual teams** safely add their FeatureServices and ODFVs with `partial=True`
- Clear ownership boundaries prevent accidental deletions and conflicts
- Proper CI/CD, naming conventions, and validation ensure smooth operation

This pattern allows organizations to scale Feast usage across many teams while maintaining a consistent, well-governed feature store.
