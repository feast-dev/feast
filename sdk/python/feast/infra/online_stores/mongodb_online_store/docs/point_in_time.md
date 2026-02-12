# Point-in-Time Joins in the MongoDB Offline Store

This document describes the point-in-time join semantics supported by the MongoDB Offline Store implementation for Feast.

The join logic itself is not implemented inside the Offline Store. Instead, the store’s schema, indexing strategy, and query guarantees are explicitly designed to support Feast’s point-in-time correctness model.

## 1. Conceptual Overview

A point-in-time join answers the question:

Given an event that occurred at time T for entity E, what were the feature values that existed at or before T?

This prevents data leakage by ensuring that training data never observes future feature values.

Within Feast:
* Point-in-time semantics are defined by Feast core
* Store implementations provide efficient access primitives
* Joins are orchestrated above the store layer

The MongoDB Offline Store enables these joins without embedding join logic itself.

## 2. Key Concepts

### Training Events

A training event represents an observation used for model training.

Each training event must include:
* One or more entity keys
* An event timestamp

Example (taxi ride):
```
{
  "_id": ObjectId("..."),
  "driver_id": 123,
  "pickup_ts": ISODate("2026-01-20T12:00:30Z"),
  "fare": 18.50,
  "tip": 3.25,
  "label": 1
}
```
Training events:
* Are not owned by Feast
* Are not mutated by the Offline Store
* May live in any domain-specific collection

### Feature History (Offline Store)

Feature data is stored in an append-only MongoDB collection.

Each document represents:
* One entity
* One feature view
* One event timestamp

```
{
  "_id": ObjectId("..."),
  "entity_key": {
    "driver_id": 123
  },
  "feature_view": "driver_stats",
  "event_timestamp": ISODate("2026-01-20T12:00:00Z"),
  "features": {
    "rating": 4.91,
    "trips_last_7d": 132
  }
}
```
Multiple rows may exist for the same entity and feature view, distinguished only by timestamp.

## 3. Required Schema Guarantees

The MongoDB Offline Store must guarantee the following properties:
* Append-only writes 
    Feature rows are never updated or deleted.
* Monotonically increasing timestamps per entity 
    Newer feature values always have later timestamps.
* One row per (entity, feature_view, event_timestamp)
    This ensures deterministic joins.

⸻

## 4. Required Indexes

To support efficient point-in-time queries, the following index is required on the feature history collection:

```
db.feature_history.create_index({
  "entity_key.driver_id": 1,
  "feature_view": 1,
  "event_timestamp": -1
})
```
This index enables efficient retrieval of the most recent feature row at or before a given timestamp.

## 5. Point-in-Time Join Semantics

For each training event E and each feature view FV:
	1.	Match feature rows with the same entity key
	2.	Filter rows where
feature.event_timestamp ≤ training.event_timestamp
	3.	Select the row with the maximum event_timestamp

Each feature view is joined independently to avoid cross-feature coupling.

⸻

## 6. Example: Taxi Ride Training Join (MongoDB Aggregation)

### Training Events Collection
```
{
  "_id": ObjectId("..."),
  "driver_id": 123,
  "pickup_ts": ISODate("2026-01-20T12:00:30Z"),
  "fare": 18.50,
  "label": 1
}
```
### Feature History Collection
```
[{
  "entity_key": { "driver_id": 123 },
  "feature_view": "driver_stats",
  "event_timestamp": ISODate("2026-01-20T12:00:00Z"),
  "features": {
    "rating": 4.91,
    "trips_last_7d": 132
  }
},
{
  "entity_key": { "driver_id": 123 },
  "feature_view": "pricing",
  "event_timestamp": ISODate("2026-01-20T12:01:00Z"),
  "features": {
    "surge_multiplier": 1.2
  }
}]
```

### Aggregation Pipeline (Single Pass, No Application Loops)

The following aggregation performs point-in-time joins for multiple feature views in a single pipeline.

```python
pipeline = [
    {
        "$lookup": {
            "from": "feature_history",
            "let": {
                "driver_id": "$driver_id",
                "event_ts": "$pickup_ts"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                { "$eq": ["$entity_key.driver_id", "$$driver_id"] },
                                { "$lte": ["$event_timestamp", "$$event_ts"] }
                            ]
                        }
                    }
                },
                { "$sort": { "event_timestamp": -1 } },
                {
                    "$group": {
                        "_id": "$feature_view",
                        "features": { "$first": "$features" },
                        "event_timestamp": { "$first": "$event_timestamp" }
                    }
                }
            ],
            "as": "joined_features"
        }
    },
    {
        "$addFields": {
            "features": {
                "$arrayToObject": {
                    "$map": {
                        "input": "$joined_features",
                        "as": "fv",
                        "in": {
                            "k": "$$fv._id",
                            "v": "$$fv.features"
                        }
                    }
                }
            }
        }
    },
    {
        "$project": {
            "joined_features": 0
        }
    }
]
```
### Execution
```python
results = list(db.training_events.aggregate(pipeline))
```
### Resulting Document
```
{
  "driver_id": 123,
  "pickup_ts": ISODate("2026-01-20T12:00:30Z"),
  "fare": 18.50,
  "label": 1,
  "features": {
    "driver_stats": {
      "rating": 4.91,
      "trips_last_7d": 132
    },
    "pricing": {
      "surge_multiplier": 1.2
    }
  }
}
```
Feast may flatten this structure into feature columns such as:
```
driver_stats__rating
driver_stats__trips_last_7d
pricing__surge_multiplier
```

## 7. Responsibilities and Non-Goals

### MongoDB Offline Store Responsibilities
* Store append-only feature history
* Support efficient temporal lookups
* Guarantee ordering and timestamp semantics

### Explicit Non-Goals

The MongoDB Offline Store does *not*:
* Join training events with features
* Merge feature views together
* Enforce point-in-time correctness rules

These responsibilities belong to Feast core.


## 8. Summary

The MongoDB Offline Store enables point-in-time joins by providing:
* A complete historical record of feature values
* Stable schemas and required indexes
* Efficient access patterns for time-aware queries

The join logic is documented here as a semantic contract, ensuring correctness while keeping the store implementation simple, testable, and aligned with Feast’s architecture.