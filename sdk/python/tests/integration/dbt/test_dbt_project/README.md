# Test dbt Project for Feast Integration Tests

This directory contains a minimal dbt project used for testing the Feast dbt integration functionality.

## Structure

```
test_dbt_project/
├── dbt_project.yml          # dbt project configuration
├── profiles.yml             # dbt connection profiles (using DuckDB for testing)
├── models/                  # dbt SQL models
│   ├── driver_features.sql
│   ├── customer_features.sql
│   ├── product_features.sql
│   └── schema.yml           # Model and column metadata
└── target/
    └── manifest.json        # Pre-generated dbt manifest
```

## Models

The test project includes 3 models with different configurations:

### 1. driver_features
- **Entity**: driver_id (INT64)
- **Tags**: feast, ml, driver
- **Features**: conv_rate, acc_rate, avg_daily_trips
- **Use case**: Tests INT32, INT64, and FLOAT64 types

### 2. customer_features
- **Entity**: customer_id (STRING)
- **Tags**: feast, ml, customer
- **Features**: total_orders, total_spent, avg_order_value
- **Use case**: Tests STRING entity and FLOAT64 features

### 3. product_features
- **Entity**: product_id (STRING)
- **Tags**: feast, recommendations (note: no 'ml' tag)
- **Features**: view_count, purchase_count, rating_avg
- **Use case**: Tests tag filtering and FLOAT32 type

## Pre-generated Manifest

The `target/manifest.json` file is **pre-generated** and committed to the repository. This allows tests to run without requiring:
- dbt CLI installation
- Database connections
- dbt compilation step

This approach keeps the tests fast and portable.

## Testing Different Data Sources

The integration tests use this manifest to test all three Feast data source types:
- **BigQuery**: `feast_test_db.public.{model_name}`
- **Snowflake**: Database: `feast_test_db`, Schema: `public`, Table: `{model_name}`
- **File**: Path: `/data/{model_name}.parquet`

## Running Tests

The integration tests are located at:
```
sdk/python/tests/integration/dbt/test_dbt_integration.py
```

Run them with:
```bash
pytest sdk/python/tests/integration/dbt/test_dbt_integration.py -v
```

## Updating the Manifest

If you need to update the manifest (e.g., to add new models or change metadata):

1. Make changes to the model SQL files or schema.yml
2. Manually edit `target/manifest.json` to reflect the changes
3. Ensure the manifest is valid JSON and follows dbt manifest schema v9

**Note**: We don't actually compile with dbt CLI to keep tests simple and fast.
