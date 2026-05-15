---
title: Historical Features Without Entity IDs
description: Feast now supports entity-less historical feature retrieval by datetime range—making it easier to train models when you don't have or need entity IDs.
date: 2026-02-19
authors: ["Jitendra Yejare", "Aniket Paluskar"]
---

# Historical Features Without Entity IDs

For years, Historical Feature Retrieval in Feast required an **entity dataframe**; you had to supply the exact entity keys (e.g. `driver_id`, `user_id`) and timestamps you wanted to join features for. That works well when you have a fixed set of entities—for example, a list of users you want to score or a training set already keyed by IDs. But in many AI and ML projects, you **don’t have** entity IDs upfront, or the problem **doesn’t naturally have** entities at all. In those cases, being forced to create and pass an entity dataframe was a real friction.

We’re excited to share that Feast now supports **entity-less historical feature retrieval** based on a **datetime range**. You can pull all historical feature data for a time window without specifying any entity dataframe—addressing the long-standing [GitHub issue #1611](https://github.com/feast-dev/feast/issues/1611) and simplifying training and tuning workflows where entity IDs are optional or irrelevant.

# The Problem: Entity IDs Aren’t Always There

Classic use of a feature store looks like this:

```python
entity_df = pd.DataFrame({
    "driver_id": [1001, 1002, 1003],
    "event_timestamp": [datetime(2025, 1, 1), datetime(2025, 1, 2), datetime(2025, 1, 3)]
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"],
).to_df()
```

You already have a set of entities and timestamps; Feast joins features onto them. But in many real-world setups:

- **Time-series and sequence models** – You care about a time range and all data in it, not a pre-defined list of entity IDs. Building an entity dataframe means first querying “who existed in this period?” and then passing those IDs in, which is extra plumbing and can be expensive.
- **Global or population-level models** – You’re modeling aggregates, trends, or system-wide behavior. There may be no natural “entity” to key on, or you want “all entities” in a window.
- **Exploratory analysis and research** – You want “all features in the last 7 days” to experiment with models or features. Requiring entity IDs forces you to materialize a full entity list before you can even call the feature store.
- **Cold start and new users** – When training models that will later serve new or rarely-seen entities (e.g. recommendation cold start, fraud detection for new accounts), you often don’t have a fixed, known entity set at training time. You want to train on “all entities that had activity in this window” so the model generalizes from the full population.
- **Batch training on full history** – You want to train on all available history in a date range. Generating and passing a huge entity dataframe is cumbersome and sometimes not even possible if the entity set is large or dynamic.

In all these cases, **passing entity IDs is either not possible, not required, or unnecessarily complex**. Making the entity dataframe optional and supporting retrieval by datetime range makes the feature store much easier to use in production and in research.

# What’s New: Optional Entity DataFrame and Date Range

Feast now supports entity-less historical feature retrieval by datetime range for several offline stores; you can pull historical feature data for a time window without specifying any entity dataframe. You specify a time window (and optionally rely on TTL for defaults), and the offline store returns all feature data in that range.

- **Entity dataframe is optional** – You can omit `entity_df` and use `start_date` and/or `end_date` instead.
- **Point-in-time correctness** – Retrieval still uses point-in-time semantics (e.g. LATERAL joins in the offline stores) so you get correct historical values.
- **Smart defaults** – If you don’t pass `start_date`, the range can be derived from the feature view TTL; if you don’t pass `end_date`, it defaults to “now”.
- **Backward compatible** – The existing entity-based API is unchanged. When you have an entity dataframe (e.g. for ODFV or targeted batch scoring), you keep using it with entity dataframe as before.

Entity-less retrieval is supported across multiple offline stores: **Postgres** (where it was first introduced), **Dask**, **Spark**, and **Ray**—with Spark and Ray being especially important for large-scale and distributed training workloads. More offline stores will be supported in the future based on user demand and priority.

# How to Use It

You can use any of these patterns depending on how much you want to specify.

**1. Explicit date range (data between start and end):**

```python
training_df = store.get_historical_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
    start_date=datetime(2025, 7, 1, 1, 0, 0),
    end_date=datetime(2025, 7, 2, 3, 30, 0),
).to_df()
```

**2. Only end date (Start date is end date minus TTL):**

```python
training_df = store.get_historical_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
    end_date=datetime(2025, 7, 2, 3, 30, 0),
).to_df()
```

**3. Only start date (data from start date to now):**

```python
training_df = store.get_historical_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
    start_date=datetime(2025, 7, 1, 1, 0, 0),
).to_df()
```

**4. No dates (data from TTL window to now):**

```python
training_df = store.get_historical_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
).to_df()
```

**5. Entity-based retrieval still works (e.g. for ODFV or when you need data for specific entities):**

```python
entity_df = pd.DataFrame.from_dict({
    "driver_id": [1005],
    "event_timestamp": [datetime(2025, 6, 29, 23, 0, 0)],
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "transformed_conv_rate:conv_rate_plus_val1",
    ],
).to_df()
```

Feast does not support mixing entity-based and range-based retrieval in one call; either pass `entity_df` or pass `start_date`/`end_date`, not both.

# Try It Out

To experiment with entity-less retrieval:

1. Use a feature store backed by an offline store that supports it: **Postgres**, **Dask**, **Spark**, or **Ray** (see [Feast docs](https://docs.feast.dev/) for setup). Spark and Ray are a great fit for distributed and large-scale training.
2. Call `get_historical_features()` with only `features` and, as needed, `start_date` and `end_date` (or rely on TTL and default end time).
3. For full details, tests, and behavior, see [PR #5527](https://github.com/feast-dev/feast/pull/5527) and the updated [FAQ on historical retrieval without an entity dataframe](https://docs.feast.dev/getting-started/faq#how-do-i-run-get_historical_features-without-providing-an-entity-dataframe).

# Why This Makes Production Easier

- **Simpler training pipelines** – No need to pre-query “all entity IDs in range” or maintain a separate entity table just to call the feature store. You specify a time window and get features.
- **Fewer moving parts** – Less code, fewer joins, and fewer failure modes when you don’t need entity-based slicing.
- **Better fit for time-range-centric workflows** – Time-series, global models, and exploratory jobs can all use the same API without artificial entity construction.
- **Same point-in-time guarantees** – Entity-less retrieval still respects feature view TTL and temporal correctness, so your training data remains valid.

We’re excited to see how the community uses entity-less historical retrieval. If you have feedback or want to help bring this to more offline stores, join the discussion on [GitHub issue #1611](https://github.com/feast-dev/feast/issues/1611) or [Feast Slack](https://slack.feast.dev).
