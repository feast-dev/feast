---
title: Streamlining AI Feature Engineering with Feast and dbt
description: Learn how to leverage your dbt transformations as Feast features to eliminate duplicate work and accelerate AI development.
date: 2026-01-27
authors: ["Francisco Javier Arceo", "Yassin Nouh"]
---

<div class="hero-image">
  <img src="/images/blog/rocket.png" alt="Feast and dbt Integration" loading="lazy">
</div>

# Streamlining AI Feature Engineering with Feast and dbt

If you're a dbt user, you know the power of well-crafted data models. You've invested time building clean, tested, and documented transformations that your team relies on. Your dbt models represent the single source of truth for analytics, reporting, and increasingly—AI features.

But here's the challenge: when your AI team wants to use these models for production predictions, they often need to rebuild the same transformations in their feature store. Your beautiful dbt models, with all their logic and documentation, end up getting reimplemented elsewhere. This feels like wasted effort, and it is.

What if you could take your existing dbt models and put them directly into production for AI without rewriting anything? That's exactly what Feast's dbt integration enables.

## Your dbt Models Are Already AI-Ready

You've already done the hard work with dbt:

- **Transformed raw data** into clean, aggregated tables
- **Documented your models** with column descriptions and metadata
- **Tested your logic** to ensure data quality
- **Organized your transformations** into a maintainable codebase

These models are perfect for AI features. The aggregations you've built for your daily reports? Those are features. The customer attributes you've enriched? Features. The time-based calculations you've perfected? You guessed it—features.

The problem isn't your models—they're great. The problem is getting them into a system that can serve them for real-time AI predictions with low latency and point-in-time correctness.

## How Feast Brings Your dbt Models to Production AI

Feast's dbt integration is designed with one principle in mind: **your dbt models should be the single source of truth**. Instead of asking you to rewrite your transformations, Feast reads your dbt project and automatically generates everything needed to serve those models for AI predictions.

Here's how it works:

1. **Tag your dbt models** that you want to use as features (just add `tags: ['feast']` to your config)
2. **Run `feast dbt import`** to automatically generate Feast definitions from your dbt metadata
3. **Deploy to production** using Feast's feature serving infrastructure

Feast reads your `manifest.json` (the compiled output from `dbt compile`) and extracts:

- Column names, types, and descriptions from your schema files
- Table locations from your dbt models
- All the metadata you've already documented

Then it generates Python code defining Feast entities, data sources, and feature views—all matching your dbt models exactly. Your documentation becomes feature documentation. Your data types become feature types. Your models become production-ready features.

The best part? **You don't change your dbt workflow at all.** Keep building models the way you always have. The integration simply creates a bridge from your dbt project to production AI serving.

## See It In Action: From dbt Model to Production Features

Let's walk through a real example. Imagine you're a data engineer at a ride-sharing company, and you've already built dbt models to track driver performance. Your analytics team loves these models, and now your AI team wants to use them to predict which drivers are likely to accept rides.

Perfect use case. Let's take your existing dbt models to production AI in just a few steps.

### Step 1: Install Feast with dbt Support

First, ensure you have Feast installed with dbt support:

```bash
pip install 'feast[dbt]'
```

### Step 2: Tag Your Existing dbt Model

You already have a dbt model that computes driver metrics. All you need to do is add one tag to mark it for Feast:

{% code title="models/features/driver_features.sql" %}
```sql
{{ config(
    materialized='table',
    tags=['feast']  -- ← Just add this tag!
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

That's it. One tag. Your model doesn't change—it keeps working exactly as before for your analytics workloads.

### Step 3: Use Your Existing Documentation

You're probably already documenting your dbt models (and if you're not, you should be!). Feast uses this exact same documentation—no duplication needed:

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

Your column descriptions and data types become the feature documentation in Feast automatically. Write it once, use it everywhere.

### Step 4: Compile Your dbt Project (As Usual)

This is your normal dbt workflow—nothing special here:

```bash
cd your_dbt_project
dbt compile
```

This creates `target/manifest.json` with all your model metadata—the same artifact you're already generating.

### Step 5: See What Feast Found

Use the Feast CLI to discover your tagged models:

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

### Step 6: Import Your dbt Model to Feast

Now for the magic—automatically generate production-ready feature definitions from your dbt model:

```bash
feast dbt import target/manifest.json \
    --entity-column driver_id \
    --data-source-type bigquery \
    --tag-filter feast \
    --output feature_repo/driver_features.py
```

In seconds, Feast generates a complete Python file with everything needed for production AI serving—all from your existing dbt model:

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

## What Just Happened?

You just went from dbt model to production AI features without rewriting a single line of transformation logic. Your dbt model—with all its carefully crafted SQL, documentation, and testing—is now:

- **Serving features in milliseconds** for real-time predictions
- **Maintaining point-in-time correctness** to prevent data leakage during training
- **Syncing with your data warehouse** automatically as your dbt models update
- **Self-documenting** using the descriptions you already wrote

And here's the best part: when you update your dbt model (maybe you add a new column or refine your logic), just re-run `feast dbt import` and `feast apply`. Your production features stay in sync with your dbt source of truth.

## Advanced Use Cases for dbt Users

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

## Why dbt Users Love This

Data teams using Feast with dbt are seeing real impact:

- **"We stopped rewriting features twice"**: Data engineers build once in dbt, AI teams use directly
- **50-70% faster AI deployment**: From dbt model to production features in minutes, not weeks
- **Single source of truth**: When dbt models update, AI features stay in sync
- **Analytics expertise becomes AI expertise**: Your dbt knowledge directly translates to AI feature engineering
- **Better collaboration**: No more need to rewrite SQL in Python

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

## Conclusion: Your dbt Models Deserve Production AI

You've invested time and care into your dbt models. They're clean, documented, tested, and trusted by your organization. They shouldn't have to be rewritten to power AI—they should work as-is.

Feast's dbt integration makes that possible. Your dbt models become production AI features with:

- ✅ No rewriting or duplication
- ✅ No changes to your dbt workflow
- ✅ All your documentation preserved
- ✅ Real-time serving for predictions
- ✅ Point-in-time correctness for training

If you're a dbt user who's been asked to "make those models work for AI," this is your answer.

Ready to see your dbt models in production? Install Feast and try it out:

```bash
pip install 'feast[dbt]'
cd your_dbt_project
dbt compile
feast dbt import target/manifest.json -e your_entity_column -d bigquery
```

Your models are already great. Now make them do more.

Join us on [Slack](http://slack.feast.dev/) to share your dbt + Feast success stories, or check out the [full documentation](https://docs.feast.dev/how-to-guides/dbt-integration) to dive deeper.

---

*Want to contribute to Feast's dbt integration? Check out our [contributing guide](https://docs.feast.dev/project/contributing) and join us on [GitHub](https://github.com/feast-dev/feast).*
