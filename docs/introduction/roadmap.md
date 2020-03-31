# Roadmap

## Feast 0.5

[Discussion](https://github.com/gojek/feast/issues/527)

### New functionality

1. Streaming statistics and validation \(M1 from [Feature Validation RFC](https://docs.google.com/document/d/1TPmd7r4mniL9Y-V_glZaWNo5LMXLshEAUpYsohojZ-8/edit)\)
2. Batch statistics and validation \(M2 from [Feature Validation RFC](https://docs.google.com/document/d/1TPmd7r4mniL9Y-V_glZaWNo5LMXLshEAUpYsohojZ-8/edit)\)
3. Support for Redis Clusters \([\#502](https://github.com/gojek/feast/issues/502)\)
4. User authentication & authorization \([\#504](https://github.com/gojek/feast/issues/504)\)
5. Add feature or feature set descriptions \([\#463](https://github.com/gojek/feast/issues/463)\)
6. Redis Cluster Support \([\#478](https://github.com/gojek/feast/issues/478)\)
7. Job management API  \([\#302](https://github.com/gojek/feast/issues/302)\)

### Technical debt, refactoring, or housekeeping

1. Clean up and document all configuration options \([\#525](https://github.com/gojek/feast/issues/525)\)
2. Externalize storage interfaces \([\#402](https://github.com/gojek/feast/issues/402)\)
3. Reduce memory usage in Redis \([\#515](https://github.com/gojek/feast/issues/515)\)
4. Support for handling out of order ingestion \([\#273](https://github.com/gojek/feast/issues/273)\)
5. Remove feature versions and enable automatic data migration \([\#386](https://github.com/gojek/feast/issues/386)\) \([\#462](https://github.com/gojek/feast/issues/462)\)
6. Tracking of batch ingestion by with dataset\_id/job\_id \([\#461](https://github.com/gojek/feast/issues/461)\)
7. Write Beam metrics after ingestion to store \(not prior\) \([\#489](https://github.com/gojek/feast/issues/489)\)

## Feast 0.6

### New functionality

1. Extended discovery API/SDK \(needs to be scoped
   1. Resource listing
   2. Schemas, statistics, metrics
   3. Entities as a higher-level concept \([\#405](https://github.com/gojek/feast/issues/405)\)
   4. Add support for discovery based on annotations/labels/tags for easier filtering and discovery
2. Add support for default values \(needs to be scoped\)
3. Add support for audit logs \(needs to be scoped\)
4. Support for an open source warehouse store or connector  \(needs to be scoped\)

### Technical debt, refactoring, or housekeeping

1. Move all non-registry functionality out of Feast Core and make it optional \(needs to be scoped\)
   1. Allow Feast serving to use its own local feature sets \(files\)
   2. Move job management to Feast serving
   3. Move stream management \(topic generation\) out of Feast core
2. Remove feature set versions from Feast \(not just retrieval API\) \(needs to be scoped\)
   1. Allow for auto-migration of data in Feast
   2. Implement interface for adding a managed data store
3. Multi-store support for serving \(batch and online\) \(needs to be scoped\)

