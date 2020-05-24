# Roadmap

## Feast 0.5 \(Technical Release\)

[Discussion](https://github.com/gojek/feast/issues/527)

### New functionality

1. Streaming statistics and validation \(M1 from [Feature Validation RFC](https://docs.google.com/document/d/1TPmd7r4mniL9Y-V_glZaWNo5LMXLshEAUpYsohojZ-8/edit)\)
2. Support for Redis Clusters \([\#478](https://github.com/gojek/feast/issues/478), [\#502](https://github.com/gojek/feast/issues/502)\)
3. Add feature and feature set labels, i.e. key/value registry metadata \([\#463](https://github.com/gojek/feast/issues/463)\)
4. Job management API  \([\#302](https://github.com/gojek/feast/issues/302)\)

### Technical debt, refactoring, or housekeeping

1. Clean up and document all configuration options \([\#525](https://github.com/gojek/feast/issues/525)\)
2. Externalize storage interfaces \([\#402](https://github.com/gojek/feast/issues/402)\)
3. Reduce memory usage in Redis \([\#515](https://github.com/gojek/feast/issues/515)\)
4. Support for handling out of order ingestion \([\#273](https://github.com/gojek/feast/issues/273)\)
5. Remove feature versions and enable automatic data migration \([\#386](https://github.com/gojek/feast/issues/386)\) \([\#462](https://github.com/gojek/feast/issues/462)\)
6. Tracking of batch ingestion by with dataset\_id/job\_id \([\#461](https://github.com/gojek/feast/issues/461)\)
7. Write Beam metrics after ingestion to store \(not prior\) \([\#489](https://github.com/gojek/feast/issues/489)\)

## Feast 0.6 \(Feature Release\)

### New functionality

1. User authentication & authorization \([\#504](https://github.com/gojek/feast/issues/504)\)
2. Batch statistics and validation \(M2 from [Feature Validation RFC](https://docs.google.com/document/d/1TPmd7r4mniL9Y-V_glZaWNo5LMXLshEAUpYsohojZ-8/edit)\)
3. Online feature/entity status metadata \([\#658](https://github.com/gojek/feast/pull/658)\)

