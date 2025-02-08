# Django Offline Store

The Django offline store provides integration between Feast and Django models, allowing you to use your Django models as feature sources.

## Configuration

To use the Django offline store, configure your feature store YAML as follows:

```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
offline_store:
    type: django
    connection_string: optional_database_url  # Optional, uses Django's default database if not provided
```

## Usage Examples

### Using Django Models as Feature Sources

```python
from feast import Entity, FeatureView, Field, ValueType
from feast.infra.offline_stores.contrib.django_offline_store.django_source import DjangoSource
from myapp.models import UserStats

# Create an Entity
user = Entity(
    name="user_id",
    value_type=ValueType.INT64,
    description="User identifier",
)

# Create a Feature View using a Django model
user_stats_source = DjangoSource(
    model=UserStats,
    timestamp_field="event_timestamp",
)

user_stats_view = FeatureView(
    name="user_stats",
    entities=[user],
    ttl=timedelta(days=1),
    schema=[
        Field(name="total_orders", dtype=ValueType.INT64),
        Field(name="average_order_value", dtype=ValueType.FLOAT),
    ],
    source=user_stats_source,
)
```

### Retrieving Features

```python
from feast import FeatureStore
import pandas as pd

# Create a FeatureStore instance
store = FeatureStore(repo_path="path/to/feature_repo")

# Create an entity DataFrame
entity_df = pd.DataFrame(
    {
        "user_id": [1, 2, 3],
        "event_timestamp": [
            datetime.now(),
            datetime.now(),
            datetime.now(),
        ]
    }
)

# Get historical features
features = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "user_stats:total_orders",
        "user_stats:average_order_value",
    ],
).to_df()
```

## Integration Details

The Django offline store allows you to:
1. Use Django models as feature sources
2. Leverage Django's ORM for feature retrieval
3. Maintain Django's schema migrations
4. Use Django's authentication system (optional)

### Feature Source

The `DjangoSource` class allows you to use any Django model as a feature source. It automatically:
- Maps model fields to feature columns
- Handles timestamp fields for point-in-time joins
- Supports field mapping for column name transformations

### Offline Store

The `DjangoOfflineStore` class provides:
- Historical feature retrieval using Django's ORM
- Support for point-in-time joins
- Integration with Django's database configuration

## Limitations

1. SQL queries as entity DataFrames are not supported
2. Only supports databases supported by Django's ORM
3. Requires Django to be properly configured in your environment

## Implementation Details

The Django offline store implementation consists of:

1. `DjangoSource`: Extends `DataSource` to support Django models
2. `DjangoOfflineStore`: Implements `OfflineStore` for feature retrieval
3. `DjangoOfflineStoreConfig`: Configuration for the offline store
4. `SavedDatasetDjangoStorage`: Storage for saved datasets

The implementation follows Django's best practices and integrates seamlessly with Feast's feature retrieval system.
