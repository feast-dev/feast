# Roadmap

## Feast 0.7 \(Feature Release\)

[Discussion](https://github.com/feast-dev/feast/issues/834)

[GitHub Milestone](https://github.com/feast-dev/feast/milestone/4)

### New functionality

1. Entities as a first-class concept [\#405](https://github.com/feast-dev/feast/issues/405)
2. Datasets as a first-class concept \(TBD\)
3. Feast UI \(MVP\)
4. Native SDK types instead of proto types

### Technical debt, refactoring, or housekeeping

1. Improved integration testing framework
2. Resolve non-determinism in end-to-end tests

### Proposals

1. Training-serving skew detection proposal

## Feast 0.6 \(Feature Release\)

[Discussion](https://github.com/feast-dev/feast/issues/767)

[GitHub Milestone](https://github.com/feast-dev/feast/milestone/3)

### New functionality

1. Batch statistics and validation [\#612](https://github.com/feast-dev/feast/pull/612)
2. Authentication and authorization [\#554](https://github.com/feast-dev/feast/pull/554)
3. Online feature and entity status metadata [\#658](https://github.com/feast-dev/feast/pull/658)
4. Improved searching and filtering of features and entities 
5. Python support for labels [\#663](https://github.com/feast-dev/feast/issues/663)

### Technical debt, refactoring, or housekeeping

1. Improved job life cycle management [\#761](https://github.com/feast-dev/feast/issues/761)
2. Compute and write metrics for rows prior to store writes [\#763](https://github.com/feast-dev/feast/pull/763) 

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

