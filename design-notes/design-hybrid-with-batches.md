Native MongoDB Offline Store (Hybrid Design)

Design Document

Overview

This document describes the design of the Native MongoDB Offline Store for Feast using a hybrid execution model. The system combines MongoDB’s strengths in indexed data retrieval with Python’s strengths in relational and temporal joins.

The implementation uses a single-collection schema in MongoDB to store feature data across all FeatureViews and performs point-in-time (PIT) joins using a “fetch + pandas join” strategy. This replaces an earlier fully in-database $lookup approach that proved unscalable for large workloads.

The result is a design that is performant, scalable, and aligned with Feast’s semantics.

⸻

Data Model

All FeatureViews share a single MongoDB collection (feature_history). Each document represents an observation of a FeatureView for a given entity at a specific timestamp.

Each document contains:
	•	A serialized entity identifier (entity_id)
	•	A FeatureView identifier (feature_view)
	•	A subdocument of feature values (features)
	•	An event timestamp (event_timestamp)
	•	An ingestion timestamp (created_at)

This schema supports:
	•	Sparse feature storage (not all features present in every document)
	•	Flexible schema evolution over time
	•	Efficient indexing across FeatureViews

A compound index is maintained on:
	•	(entity_id, feature_view, event_timestamp DESC)

This index supports efficient filtering by entity, FeatureView, and time range.

⸻

Execution Model

High-Level Strategy

The system implements historical feature retrieval in three stages:
	1.	Preprocessing (Python)
	•	Normalize timestamps to UTC
	•	Serialize entity keys into entity_id
	•	Partition the input entity_df into manageable chunks
	2.	Data Fetching (MongoDB)
	•	Query MongoDB using $in on entity IDs
	•	Filter by FeatureView and time bounds
	•	Retrieve matching feature documents in batches
	3.	Point-in-Time Join (Python)
	•	Convert MongoDB results into pandas DataFrames
	•	Perform per-FeatureView joins using merge_asof
	•	Apply TTL constraints and feature selection

This design avoids per-row database joins and instead performs a small number of efficient indexed scans.

⸻

Chunking and Batching

To ensure scalability, the system separates concerns between:
	•	Chunk size (entity_df)
Controls memory usage in Python
Default: ~5,000 rows
	•	Batch size (MongoDB queries)
Controls query size and index efficiency
Default: ~1,000 entity IDs per query

Each chunk of entity_df is processed independently:
	•	Entity IDs are extracted and deduplicated
	•	Feature data is fetched in batches
	•	Results are joined and accumulated

This ensures:
	•	Bounded memory usage
	•	Predictable query performance
	•	Compatibility with large workloads

⸻

Point-in-Time Join Semantics

For each FeatureView:
	•	Feature data is sorted by (entity_id, event_timestamp)
	•	The entity dataframe is similarly sorted
	•	A backward merge_asof is performed

This ensures:
	•	Only feature values with timestamps ≤ entity timestamp are used
	•	The most recent valid feature value is selected

TTL constraints are applied after the join:
	•	If the matched feature timestamp is older than the allowed TTL window, the value is set to NULL

⸻

Key Improvements in Current Design

1. Projection (Reduced Data Transfer)

The system now explicitly limits fields retrieved from MongoDB to only those required:
	•	entity_id
	•	feature_view
	•	event_timestamp
	•	Requested feature fields within features

This reduces:
	•	Network overhead
	•	BSON decoding cost
	•	Memory usage in pandas

This is especially important for wide FeatureViews or large documents.

⸻

2. Bounded Time Filtering

Queries now include both:
	•	An upper bound (<= max_ts)
	•	A lower bound (>= min_ts)

This significantly reduces the amount of historical data scanned when:
	•	The entity dataframe spans a narrow time window
	•	The feature store contains deep history

This optimization improves:
	•	Query latency
	•	Index selectivity
	•	Memory footprint of retrieved data

Future enhancements may incorporate TTL-aware lower bounds.

⸻

3. Correct Sorting for Temporal Joins

The system ensures proper sorting before merge_asof:
	•	Both dataframes are sorted by (entity_id, timestamp)

This is critical for correctness when:
	•	Multiple entities are processed in a single batch
	•	Data is interleaved across entities

Without this, joins may silently produce incorrect results.

⸻

Tradeoffs

Advantages
	•	Scalability: Avoids O(n × m) behavior of correlated joins
	•	Flexibility: Supports sparse and evolving schemas
	•	Performance: Leverages MongoDB indexes efficiently
	•	Simplicity: Uses well-understood pandas join semantics

Limitations
	•	Memory-bound joins: Requires chunking for large workloads
	•	Multiple passes: Each FeatureView requires a separate join
	•	No server-side joins: MongoDB is used only for filtering, not relational logic

⸻

Comparison to Alternative Designs

Full MongoDB Join ($lookup)

Rejected due to:
	•	Poor scaling with large entity sets
	•	Repeated execution of correlated subqueries
	•	High latency (orders of magnitude slower)

⸻

Ibis-Based Design
	•	Uses one collection per FeatureView
	•	Loads data into memory and performs joins in Python

Comparison:
	•	Similar performance after hybrid redesign
	•	Simpler query model
	•	Less flexible schema

The Native design trades simplicity for:
	•	Unified storage
	•	Better alignment with document-based ingestion
	•	More flexible feature evolution

⸻

Operational Considerations

Index Management

Indexes are created lazily at runtime:
	•	Ensures correctness without manual setup
	•	Avoids placing responsibility on users

Future improvements may include:
	•	Optional strict index validation
	•	Configuration-driven index management

⸻

MongoDB Client Usage

Each chunk currently uses a separate MongoDB client instance.

This is acceptable for moderate workloads but may be optimized in the future by:
	•	Reusing a shared client per retrieval job
	•	Leveraging connection pooling more explicitly

⸻

Future Work

Several enhancements are possible:
	1.	Streaming Joins
	•	Avoid materializing all feature data in memory
	•	Process data incrementally
	2.	Adaptive Chunking
	•	Dynamically adjust chunk size based on memory pressure
	3.	TTL Pushdown
	•	Incorporate TTL constraints into MongoDB queries
	4.	Parallel Execution
	•	Process chunks concurrently for large workloads

⸻

Conclusion

The hybrid MongoDB + pandas design represents a significant improvement over the initial fully in-database approach. It aligns system responsibilities with the strengths of each component:
	•	MongoDB handles indexed filtering and retrieval
	•	Python handles temporal join logic

With the addition of projection, bounded time filtering, and correct sorting, the system is now both performant and correct for large-scale historical feature retrieval.

This design provides a strong foundation for further optimization and production use.

