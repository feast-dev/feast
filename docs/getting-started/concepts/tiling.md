# Tiling with Intermediate Representations in Feast

## Overview

**Tiling** is an optimization technique for **streaming time-windowed aggregations** that enables massively efficient feature computation by pre-aggregating data into smaller time intervals (tiles) and storing **Intermediate Representations (IRs)** for correct merging.

**Primary Use Case: Streaming**

Tiling provides **speedup** for streaming scenarios where features are updated frequently (every few minutes) from sources like Kafka, Kinesis, or PushSource.

**Key Benefits (Streaming):**
- **Faster**: Reuse 90%+ of tiles between updates instead of recomputing from scratch
- **Correct results**: IRs ensure mathematically accurate merging for all aggregation types
- **Memory efficient**: Only process new events, reuse previous tiles in memory
- **Real-time capable**: Handle high-throughput streaming with low latency
- **Incremental updates**: Compute 1 new tile instead of rescanning entire window

---

## The Problem: Why Intermediate Representations?

Traditional approaches to time-windowed aggregations either:
1. **Recompute from raw data** every time → Slow, expensive
2. **Store final aggregated values** per tile → Fast but often **incorrect** when merging

### The Merging Problem

You **cannot correctly merge** many common aggregations:

```
WRONG: avg(tile1, tile2) ≠ (avg_tile1 + avg_tile2) / 2

Example:
  tile1: [10, 20, 30] → avg = 20
  tile2: [100]        → avg = 100
  
  Correct merged avg: (10+20+30+100) / 4 = 40
  Wrong merged avg:   (20 + 100) / 2     = 60
```

**The same problem exists for:**
- Standard deviation (`std`)
- Variance (`var`)
- Median and percentiles
- Any "holistic" aggregation that requires knowledge of all values

---

## The Solution: Intermediate Representations (IRs)

Instead of storing **final aggregated values**, store **intermediate data** that preserves the mathematical properties needed for correct merging.

### Example: Average

**Traditional (Incorrect)**:
```
Tile 1: avg = 20
Tile 2: avg = 20
Merged avg = (20 + 20) / 2 = 20 - WRONG
```

**With IRs (Correct)**:
```
Tile 1: sum = 60, count = 3
Tile 2: sum = 100, count = 1
Merged: sum = 160, count = 4
Merged avg = 160 / 4 = 40 - CORRECT
```

---

## Aggregation Categories

### Algebraic Aggregations

These can be merged by applying the same aggregation function to tiles:

| Aggregation | Stored Value | Merge Strategy | Storage |
|-------------|--------------|----------------|---------|
| `sum` | sum | `sum(tile_sums)` | 1 column |
| `count` | count | `sum(tile_counts)` | 1 column |
| `max` | max | `max(tile_maxes)` | 1 column |
| `min` | min | `min(tile_mins)` | 1 column |

**No IRs needed** - the final value is the IR!

---

### Holistic Aggregations

These require storing multiple intermediate values:

#### Average (`avg`, `mean`)

**Stored IRs**: `sum`, `count`  
**Final computation**: `avg = sum / count`  
**Merge strategy**: Sum the sums and counts, then divide

**Storage**: 3 columns (final + 2 IRs)

---

#### Standard Deviation (`std`, `stddev`)

**Stored IRs**: `count`, `sum`, `sum_of_squares`  
**Final computation**: 
```python
variance = (sum_sq - sum²/count) / (count - δ)
std = sqrt(variance)
# δ = 1 for sample, 0 for population
```

**Merge strategy**: Sum all three IRs, then apply formula

**Storage**: 4 columns (final + 3 IRs)

---

#### Variance (`var`, `variance`)

**Stored IRs**: `count`, `sum`, `sum_of_squares`  
**Final computation**: Same as std but without `sqrt()`

**Storage**: 4 columns (final + 3 IRs)

---

## How Tiling Works

Tiling is optimized for **streaming scenarios** with frequent updates (e.g., every few minutes).

### 1. Continuous Tile Updates

```
Stream Events → Partition by Hop Intervals → Compute IRs → Store Windowed Aggregations
  |                  |                            |              |
  |                  |                            |              └─> Online Store (Redis, etc.)
  |                  |                            └─> avg_sum, avg_count, std_sum_sq, etc.
  |                  └─> 5-min hops: [00:00-00:05], [00:05-00:10], ...
  └─> customer_id=1: [txn1, txn2, txn3, ...]

Every 5 minutes:
- New events arrive
- Only 1 new tile computed (5 min of data)
- 11 previous tiles reused (in memory during streaming session)
- Final aggregation = merge 12 tiles (1 new + 11 reused)
```

**Why It's Fast:**
- **Without tiling:** Scan entire 1-hour window (1000+ events) every 5 minutes
- **With tiling:** Only process 5 minutes of new events, reuse previous tiles
- **Speedup:** Faster for streaming updates!

---

### 2. Streaming Update Efficiency

| Update | Without Tiling | With Tiling | Tile Reuse |
|--------|---------------|-------------|------------|
| T=00:00 | Compute 1hr | Compute 12 tiles | 0% reuse (initial) |
| T=00:05 | Compute 1hr (1000+ events) | Compute 1 tile + reuse 11 | 92% reuse |
| T=00:10 | Compute 1hr (1000+ events) | Compute 1 tile + reuse 11 | 92% reuse |
| T=00:15 | Compute 1hr (1000+ events) | Compute 1 tile + reuse 11 | 92% reuse |

**Key Benefit:** Tiles stay in memory during the streaming session, enabling massive reuse.

---

## Tiling Algorithm

### Sawtooth Window Tiling

1. **Partition events** into hop-sized intervals (e.g., 5 minutes)
2. **Compute cumulative tail aggregations** for each hop from the start of the materialization window
3. **Subtract tiles** to form windowed aggregations (current_tile - previous_tile)
4. **Store IRs** for correct merging of holistic aggregations
5. **At materialization**, store windowed aggregations in online store

**Benefits**:
- Efficient query-time performance (pre-computed windows)
- Minimal storage overhead (only hop-sized tiles)
- Mathematically correct for all aggregation types

---

## Configuration

### Recommended: StreamFeatureView (Streaming Scenarios)

Tiling provides **maximum benefit for streaming scenarios** with frequent updates:

```python
from feast import StreamFeatureView, Aggregation
from feast.data_source import PushSource, KafkaSource
from datetime import timedelta

# Example with Kafka streaming source
customer_features = StreamFeatureView(
    name="customer_transaction_features",
    entities=[customer],
    source=KafkaSource(
        name="transactions_stream",
        kafka_bootstrap_servers="localhost:9092",
        topic="transactions",
        timestamp_field="event_timestamp",
        batch_source=file_source,  # For historical data
    ),
    aggregations=[
        Aggregation(column="amount", function="sum", time_window=timedelta(hours=1)),
        Aggregation(column="amount", function="avg", time_window=timedelta(hours=1)),
        Aggregation(column="amount", function="std", time_window=timedelta(hours=1)),
    ],
    timestamp_field="event_timestamp",
    online=True,
    
    # Tiling configuration
    enable_tiling=True,  # speedup for streaming!
    tiling_hop_size=timedelta(minutes=5),  # Update frequency
)
```

**When to Enable:**
- Streaming data sources (Kafka, Kinesis, PushSource)
- Frequent updates (every few minutes)
- Real-time feature serving
- High-throughput event processing

---

### Key Parameters

- `aggregations`: List of time-windowed aggregations to compute
- `timestamp_field`: Column name for timestamps (required when aggregations are specified)
- `enable_tiling`: Enable tiling optimization (default: `False`)
  - Set to `True` for **streaming scenarios**
- `tiling_hop_size`: Time interval between tiles (default: 5 minutes)
  - Smaller = more granular tiles, potentially higher memory during processing window
  - Larger = less granular tiles, potentially lower memory during processing window

### Compute Engine Requirements

- **Spark Compute Engine**: Fully supported for streaming and batch
- **Ray Compute Engine**: Fully supported for streaming and batch
- **Local Compute Engine**: Does NOT support time-windowed aggregations

---

## Architecture

Tiling in Feast uses a **simple, pure pandas architecture** that works with any compute engine:

### How It Works

```
┌─────────────────┐
│ Engine DataFrame│ (Spark/Ray/etc)
└────────┬────────┘
         │ .toPandas() / .to_pandas()
         ▼
┌─────────────────┐
│ Pandas DataFrame│
└────────┬────────┘
         │ orchestrator.apply_sawtooth_window_tiling()
         ▼
┌─────────────────┐
│  Cumulative     │ (pandas with _tile_start, _tile_end, IRs)
│     Tiles       │
└────────┬────────┘
         │ tile_subtraction.convert_cumulative_to_windowed()
         ▼
┌─────────────────┐
│   Windowed      │ (pandas with final aggregations)
│ Aggregations    │
└────────┬────────┘
         │ spark.createDataFrame() / ray.from_pandas()
         ▼
┌─────────────────┐
│ Engine DataFrame│
└─────────────────┘
```


## Summary

Tiling with Intermediate Representations provides a powerful optimization for **streaming time-windowed aggregations** in Feast.
