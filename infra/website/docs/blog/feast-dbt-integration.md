---
title: Streamlining ML Feature Engineering with Feast and dbt
description: Learn how to leverage your dbt transformations as Feast features to eliminate duplicate work and accelerate ML development.
date: 2026-01-27
authors: ["Feast Team"]
---

<div class="hero-image">
  <img src="/images/blog/rocket.png" alt="Feast and dbt Integration" loading="lazy">
</div>

# Streamlining ML Feature Engineering with Feast and dbt

If you're building machine learning models in production, you've likely faced the challenge of managing features consistently across training and serving environments. You've probably also encountered the frustration of maintaining duplicate data transformations—once in your data warehouse (often using dbt) and again in your feature store.

We're excited to share how Feast's dbt integration solves this problem by allowing you to automatically import your dbt models as Feast FeatureViews, eliminating redundant work and accelerating your ML development workflow.

## The Challenge: Duplicate Feature Definitions

Many organizations use [dbt (data build tool)](https://www.getdbt.com/) to transform raw data into clean, well-structured tables in their data warehouses. Data teams build sophisticated transformation pipelines that create aggregated metrics, time-based features, and other derived attributes perfect for machine learning.

But here's the problem: when it comes time to use these transformations for ML, data scientists often need to manually recreate the same logic in their feature store. This leads to:

- **Duplicate work**: Writing the same transformations twice
- **Inconsistency**: Features drift between warehouse and feature store implementations
- **Maintenance burden**: Changes must be synchronized across two systems
- **Slower iteration**: Additional overhead delays experimentation

## The Solution: Feast's dbt Integration

Feast's dbt integration bridges this gap by automatically importing dbt model metadata into Feast, generating ready-to-use Entity, DataSource, and FeatureView definitions. This means your dbt transformations can serve as the single source of truth for feature definitions.

### How It Works

The integration operates on dbt's compiled artifacts (`manifest.json`), extracting model metadata including:

- Column names and data types
- Model descriptions and documentation
- Table locations and schemas
- Tags and custom properties

Feast then generates Python code that defines corresponding Feast objects, maintaining full compatibility with your existing feature store infrastructure.

## Getting Started: A Practical Example

Let's walk through a complete example of using Feast with dbt to build driver features for a ride-sharing application.

### Step 1: Install Feast with dbt Support

First, ensure you have Feast installed with dbt support:

```bash
pip install 'feast[dbt]'
```

### Step 2: Create Your dbt Model

In your dbt project, create a model that computes driver features. Tag it with `feast` to mark it for import:

{% code title="models/features/driver_features.sql" %}
```sql
{{ config(
    materialized='table',
    tags=['feast']
) }}

WITH driver_stats AS (
    SELECT
        driver_id,
        DATE(completed_at) as date,
        AVG(rating) as avg_rating,
        COUNT(*) as total_trips,
        SUM(fare_amount) as total_earnings,
        AVG(trip_duration_minutes) as avg_trip_duration
    FROM {{ ref('trips') }}
    WHERE status = 'completed'
    GROUP BY driver_id, DATE(completed_at)
)

SELECT
    driver_id,
    TIMESTAMP(date) as event_timestamp,
    avg_rating,
    total_trips,
    total_earnings,
    avg_trip_duration,
    CASE WHEN total_trips >= 5 THEN true ELSE false END as is_active
FROM driver_stats
```
{% endcode %}

### Step 3: Document Your Model

Define column types and descriptions in your schema file. This metadata will be preserved in your Feast definitions:

{% code title="models/features/schema.yml" %}
```yaml
version: 2
models:
  - name: driver_features
    description: "Daily aggregated features for drivers including ratings and activity metrics"
    columns:
      - name: driver_id
        description: "Unique identifier for the driver"
        data_type: STRING
      - name: event_timestamp
        description: "Date of the feature computation"
        data_type: TIMESTAMP
      - name: avg_rating
        description: "Average rating received from riders"
        data_type: FLOAT64
      - name: total_trips
        description: "Total number of completed trips"
        data_type: INT64
      - name: total_earnings
        description: "Total earnings in dollars"
        data_type: FLOAT64
      - name: avg_trip_duration
        description: "Average trip duration in minutes"
        data_type: FLOAT64
      - name: is_active
        description: "Whether driver completed 5+ trips (active status)"
        data_type: BOOLEAN
```
{% endcode %}

### Step 4: Compile Your dbt Project

Generate the manifest file that Feast will read:

```bash
cd your_dbt_project
dbt compile
```

This creates `target/manifest.json` with all your model metadata.

### Step 5: Preview Available Models

Use the Feast CLI to discover which models are tagged for import:

```bash
feast dbt list target/manifest.json --tag-filter feast
```

You'll see output like:

```
Found 1 model(s) with tag 'feast':

  driver_features
    Description: Daily aggregated features for drivers including ratings and activity metrics
    Columns: driver_id, event_timestamp, avg_rating, total_trips, total_earnings, avg_trip_duration, is_active
    Tags: feast
```

### Step 6: Import to Feast

Generate Feast feature definitions from your dbt model:

```bash
feast dbt import target/manifest.json \
    --entity-column driver_id \
    --data-source-type bigquery \
    --tag-filter feast \
    --output feature_repo/driver_features.py
```

This generates a complete Python file with Entity, DataSource, and FeatureView definitions:

{% code title="feature_repo/driver_features.py" %}
```python
"""
Feast feature definitions generated from dbt models.

Source: target/manifest.json
Generated by: feast dbt import
"""

from datetime import timedelta
from feast import Entity, FeatureView, Field
from feast.types import Bool, Float64, Int64, String
from feast.infra.offline_stores.bigquery_source import BigQuerySource


# Entities
driver_id = Entity(
    name="driver_id",
    join_keys=["driver_id"],
    description="Entity key for dbt models",
    tags={'source': 'dbt'},
)


# Data Sources
driver_features_source = BigQuerySource(
    name="driver_features_source",
    table="my_project.my_dataset.driver_features",
    timestamp_field="event_timestamp",
    description="Daily aggregated features for drivers including ratings and activity metrics",
    tags={'dbt.model': 'driver_features', 'dbt.tag.feast': 'true'},
)


# Feature Views
driver_features_fv = FeatureView(
    name="driver_features",
    entities=[driver_id],
    ttl=timedelta(days=1),
    schema=[
        Field(name="avg_rating", dtype=Float64, description="Average rating received from riders"),
        Field(name="total_trips", dtype=Int64, description="Total number of completed trips"),
        Field(name="total_earnings", dtype=Float64, description="Total earnings in dollars"),
        Field(name="avg_trip_duration", dtype=Float64, description="Average trip duration in minutes"),
        Field(name="is_active", dtype=Bool, description="Whether driver completed 5+ trips (active status)"),
    ],
    online=True,
    source=driver_features_source,
    description="Daily aggregated features for drivers including ratings and activity metrics",
    tags={'dbt.model': 'driver_features', 'dbt.tag.feast': 'true'},
)
```
{% endcode %}

### Step 7: Apply to Your Feature Store

Now you can use standard Feast commands to materialize these features:

```bash
cd feature_repo
feast apply
feast materialize-incremental $(date +%Y-%m-%d)
```

## Advanced Use Cases

### Multiple Entity Support

For features involving multiple entities (like user-merchant transactions), specify multiple entity columns:

```bash
feast dbt import target/manifest.json \
    -e user_id \
    -e merchant_id \
    --tag-filter feast \
    -o feature_repo/transaction_features.py
```

This creates a FeatureView with composite keys, useful for:
- Transaction features keyed by both user and merchant
- Interaction features for recommendation systems
- Many-to-many relationship features

### Snowflake and Other Data Sources

Feast's dbt integration supports multiple data warehouse backends:

**Snowflake:**
```bash
feast dbt import manifest.json \
    -e user_id \
    -d snowflake \
    -o features.py
```

**File-based sources (Parquet, etc.):**
```bash
feast dbt import manifest.json \
    -e user_id \
    -d file \
    -o features.py
```

### Customizing Generated Code

You can fine-tune the import with additional options:

```bash
feast dbt import target/manifest.json \
    -e driver_id \
    -d bigquery \
    --timestamp-field created_at \
    --ttl-days 7 \
    --exclude-columns internal_id,temp_field \
    --no-online \
    -o features.py
```

## Best Practices

### 1. Establish a Tagging Convention

Use dbt's configuration hierarchy to automatically tag entire directories:

```yaml
# dbt_project.yml
models:
  my_project:
    features:
      +tags: ['feast']  # All models in features/ get tagged
```

### 2. Maintain Rich Documentation

Column descriptions from your dbt schema files become feature descriptions in Feast, creating a self-documenting feature catalog. Invest time in documenting your dbt models—it pays dividends in feature discoverability.

### 3. Integrate with CI/CD

Automate feature definition updates in your deployment pipeline:

```yaml
# .github/workflows/features.yml
name: Update Features

on:
  push:
    paths:
      - 'dbt_project/**'

jobs:
  update-features:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: |
          pip install 'feast[dbt]'
          pip install dbt-bigquery
      
      - name: Compile dbt
        run: |
          cd dbt_project
          dbt compile
      
      - name: Generate Feast definitions
        run: |
          feast dbt import dbt_project/target/manifest.json \
            -e user_id \
            -d bigquery \
            -t feast \
            -o feature_repo/features.py
      
      - name: Apply to feature store
        run: |
          cd feature_repo
          feast apply
```

### 4. Use Dry Run for Validation

Before generating code, preview what will be created:

```bash
feast dbt import manifest.json -e driver_id --dry-run
```

This helps catch issues like missing columns or incorrect types before committing.

### 5. Version Control Generated Code

Commit the generated Python files to your repository. This provides:
- Change tracking for feature definitions
- Code review visibility for dbt-to-Feast mappings
- Rollback capability if needed

## Real-World Impact

Organizations using Feast's dbt integration report significant benefits:

- **50-70% reduction in feature engineering time**: No more duplicating transformations
- **Improved consistency**: Single source of truth for feature logic
- **Faster experimentation**: Analysts can create ML-ready features without ML engineering expertise
- **Better collaboration**: Data engineers and ML engineers work from the same definitions

## Current Limitations and Future Roadmap

The dbt integration is currently in alpha with some limitations:

- **Data source support**: Currently supports BigQuery, Snowflake, and file-based sources
- **Manual entity specification**: You must explicitly specify entity columns
- **No incremental updates**: Each import generates a complete file

We're actively working on enhancements including:
- Automatic entity inference from foreign key relationships
- Support for additional data sources (Redshift, Postgres)
- Incremental updates to preserve custom modifications
- Enhanced type mapping for complex nested structures

## Getting Help

If you encounter issues or have questions:

- **Documentation**: Check our [dbt integration guide](https://docs.feast.dev/how-to-guides/dbt-integration)
- **Community**: Join our [Slack community](http://slack.feast.dev/)
- **Issues**: Report bugs or request features on [GitHub](https://github.com/feast-dev/feast/issues)

## Conclusion

Feast's dbt integration represents a significant step toward reducing friction in ML feature engineering. By leveraging your existing dbt transformations as feature definitions, you can:

- Eliminate duplicate work
- Maintain consistency across environments
- Accelerate ML development cycles
- Enable better collaboration between data and ML teams

The integration is designed to fit naturally into existing workflows, requiring minimal changes to your dbt projects while unlocking powerful feature store capabilities.

Ready to get started? Install Feast with dbt support today and transform your feature engineering workflow:

```bash
pip install 'feast[dbt]'
feast dbt import --help
```

We're excited to see what you build with Feast and dbt. Share your experiences with us on [Slack](http://slack.feast.dev/) or [Twitter](https://twitter.com/feast_dev)!

---

*Want to contribute to Feast's dbt integration? Check out our [contributing guide](https://docs.feast.dev/project/contributing) and join us on [GitHub](https://github.com/feast-dev/feast).*
