---
name: feast-user-guide
description: Guide for working with Feast (Feature Store) — defining features, configuring feature_store.yaml, retrieving features online/offline, using the CLI, and building RAG retrieval pipelines. Use when the user asks about creating entities, feature views, on-demand feature views, stream feature views, feature services, data sources, feature_store.yaml configuration, feast apply/materialize commands, online or historical feature retrieval, or vector-based document retrieval with Feast.
license: Apache-2.0
compatibility: Works with Claude Code, OpenAI Codex, and any Agent Skills compatible tool.
metadata:
  author: feast-dev
  version: "1.0"
---

# Feast User Guide

## Quick Start

A Feast project requires:
1. A `feature_store.yaml` config file
2. Python files defining entities, data sources, feature views, and feature services
3. Running `feast apply` to register definitions

```bash
feast init my_project
cd my_project
feast apply
```

## Core Concepts

### Entity
An entity is a collection of semantically related features (e.g., a customer, a driver). Entities have join keys used to look up features.

```python
from feast import Entity
from feast.value_type import ValueType

driver = Entity(
    name="driver_id",
    description="Driver identifier",
    value_type=ValueType.INT64,
)
```

### Data Sources
Data sources describe where raw feature data lives.

```python
from feast import FileSource, BigQuerySource, KafkaSource, PushSource, RequestSource
from feast.data_format import ParquetFormat

# Batch source (file)
driver_stats_source = FileSource(
    name="driver_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Request source (for on-demand features)
input_request = RequestSource(
    name="vals_to_add",
    schema=[Field(name="val_to_add", dtype=Float64)],
)
```

### FeatureView
Maps features from a data source to entities with a schema, TTL, and online/offline settings.

```python
from feast import FeatureView, Field
from feast.types import Float32, Int64, String
from datetime import timedelta

driver_hourly_stats = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=365),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_stats_source,
)
```

### OnDemandFeatureView
Computes features at request time from other feature views and/or request data.

```python
from feast import on_demand_feature_view
import pandas as pd

@on_demand_feature_view(
    sources=[driver_hourly_stats, input_request],
    schema=[Field(name="conv_rate_plus_val", dtype=Float64)],
    mode="pandas",
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val"] = inputs["conv_rate"] + inputs["val_to_add"]
    return df
```

### FeatureService
Groups features from multiple views for retrieval.

```python
from feast import FeatureService

driver_fs = FeatureService(
    name="driver_ranking",
    features=[driver_hourly_stats, transformed_conv_rate],
)
```

## Feature Retrieval

### Online (low-latency)
```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")

features = store.get_online_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
    ],
    entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}],
).to_dict()
```

### Historical (training data with point-in-time joins)
```python
entity_df = pd.DataFrame({
    "driver_id": [1001, 1002],
    "event_timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"],
).to_df()
```

Or use a FeatureService:
```python
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=driver_fs,
).to_df()
```

## Materialization

Load features from offline store into online store:

```bash
# Full materialization over a time range
feast materialize 2023-01-01T00:00:00 2023-12-31T23:59:59

# Incremental (from last materialized timestamp)
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```

Python API:
```python
from datetime import datetime
store.materialize(start_date=datetime(2023, 1, 1), end_date=datetime(2023, 12, 31))
store.materialize_incremental(end_date=datetime.utcnow())
```

## CLI Commands

| Command | Purpose |
|---------|---------|
| `feast init [DIR]` | Create new feature repository |
| `feast apply` | Register/update feature definitions |
| `feast plan` | Preview changes without applying |
| `feast materialize START END` | Materialize features to online store |
| `feast materialize-incremental END` | Incremental materialization |
| `feast entities list` | List registered entities |
| `feast feature-views list` | List feature views |
| `feast feature-services list` | List feature services |
| `feast on-demand-feature-views list` | List on-demand feature views |
| `feast teardown` | Remove infrastructure resources |
| `feast version` | Show SDK version |

Options: `--chdir` / `-c` (run in different directory), `--feature-store-yaml` / `-f` (override config path).

## Vector Search / RAG

Define a feature view with vector fields for similarity search:

```python
from feast.types import Array, Float32

wiki_passages = FeatureView(
    name="wiki_passages",
    entities=[passage_entity],
    schema=[
        Field(name="passage_text", dtype=String),
        Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=384,
            vector_search_metric="COSINE",
        ),
    ],
    source=passages_source,
    online=True,
)
```

Retrieve similar documents:
```python
results = store.retrieve_online_documents(
    feature="wiki_passages:embedding",
    query=query_embedding,
    top_k=5,
)
```

## feature_store.yaml Minimal Config

```yaml
project: my_project
registry: data/registry.db
provider: local
online_store:
  type: sqlite
  path: data/online_store.db
```

## Common Imports

```python
from feast import (
    Entity, FeatureView, OnDemandFeatureView, FeatureService,
    Field, FileSource, RequestSource, FeatureStore,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, String, Bool, Array
from feast.value_type import ValueType
from datetime import timedelta
```

## Detailed References

- **Feature definitions** (all types, parameters, patterns): See [references/feature-definitions.md](references/feature-definitions.md)
- **Configuration** (feature_store.yaml, all store types, auth): See [references/configuration.md](references/configuration.md)
- **Retrieval & RAG** (online/offline retrieval, vector search, RAG retriever): See [references/retrieval-and-rag.md](references/retrieval-and-rag.md)
