# ADR-0004: Entity Join Key Mapping

## Status

Accepted

## Context

Multiple different entity keys in the source data may need to map onto the same entity from the feature data table during a join. For example, `spammer_id` and `reporter_id` may both need the `years_on_platform` feature from a table keyed by `user_id`.

Without entity join key mapping:

- Users had to rename columns in their entity dataframe to match the feature view's join key before retrieval.
- It was impossible to join a feature view twice on two different columns in the entity data (e.g., getting user features for both `spammer_id` and `reporter_id` in the same query).

### Example

Entity source data:

| spammer_id | reporter_id | timestamp  |
|------------|-------------|------------|
| 2          | 8           | 1629909366 |
| 1          | 2           | 1629909323 |

Desired joined data should include `spammer_feature_a` and `reporter_feature_a`, both sourced from the same `user` feature view but joined on different keys.

## Decision

Implement join key overrides using a `with_join_key_map()` method on feature views, combined with `with_name()` for disambiguation. This was **Option 8b** from the RFC.

### API

```python
abuse_feature_service = FeatureService(
    name="my_abuse_model_v1",
    features=[
        user_features
            .with_name("reporter_features")
            .with_join_key_map({"user_id": "reporter_id"}),
        user_features
            .with_name("spammer_features")
            .with_join_key_map({"user_id": "spammer_id"}),
    ],
)
```

### Key Decisions

- **Query-time mapping** rather than registration-time. This provides flexibility since the same feature view can be used with different mappings in different contexts.
- **Join key level mapping** rather than entity-level mapping. While entity-level mapping (Option 10) better preserves abstraction boundaries, join key mapping is more flexible and doesn't require registering additional entities.
- **`with_name()` required** when using the same feature view multiple times to avoid output column name collisions. If omitted, a name collision error is raised.
- **Mapping overwrites wholly**: specifying a mapping replaces the default join behavior entirely. If you want the original join key included, it must be explicitly listed.

### Implementation

- **Offline (historical) retrieval**: After feature subtable cleaning and dedup, entity columns are renamed based on the mapping before the join.
- **Online retrieval**: Shadow entity keys are translated to the original join key for the online store lookup, then results are remapped to the shadow entity names.
- The `join_key_map` is stored on `FeatureViewProjection` and flows through both online and offline retrieval paths.

## Consequences

### Positive

- Users can join the same feature view on different entity columns in a single query.
- No need to register additional entities or manually rename columns before retrieval.
- Works consistently across both online and offline retrieval.
- Feature view definitions remain clean and reusable.

### Negative

- Adds complexity to the retrieval path with column renaming logic.
- Users must remember to use `with_name()` to avoid collisions when joining the same feature view multiple times.

## References

- Original RFC: [Feast RFC-023: Shadow Entities Mapping](https://docs.google.com/document/d/1TsCwKf3nVXTAfL0f8i26jnCgHA3bRd4dKQ8QdM87vIA/edit)
- GitHub Issue: [#1762](https://github.com/feast-dev/feast/issues/1762)
- Implementation: `sdk/python/feast/feature_view.py` (`with_join_key_map` method), `sdk/python/feast/feature_view_projection.py` (`join_key_map` field)
