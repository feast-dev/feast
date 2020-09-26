# Roadmap

## Feast 0.8

[Discussion](https://github.com/feast-dev/feast/issues/1018)

[Feast 0.8 RFC](https://docs.google.com/document/d/1snRxVb8ipWZjCiLlfkR4Oc28p7Fkv_UXjvxBFWjRBj4/edit#heading=h.yvkhw2cuvx5)

### **New Functionality**

1. Add support for AWS \(data sources and deployment\)
2. Add support for local deployment
3. Add support for Spark based ingestion
4. Add support for Spark based historical retrieval

### **Technical debt, refactoring, or housekeeping**

1. Move job management functionality to SDK
2. Remove Apache Beam based ingestion
3. Allow direct ingestion from batch sources that does not pass through stream
4. Remove Feast Historical Serving abstraction to allow direct access from Feast SDK to data sources for retrieval

## Feast 0.7

[Discussion](https://github.com/feast-dev/feast/issues/834)

[GitHub Milestone](https://github.com/feast-dev/feast/milestone/4)

### **New Functionality**

1. Label based Ingestion Job selector for Job Controller [\#903](https://github.com/feast-dev/feast/pull/903)
2. Authentication Support for Java & Go SDKs [\#971](https://github.com/feast-dev/feast/pull/971)
3. Automatically Restart Ingestion Jobs on Upgrade [\#949](https://github.com/feast-dev/feast/pull/949)
4. Structured Audit Logging [\#891](https://github.com/feast-dev/feast/pull/891)
5. Request Response Logging support via Fluentd [\#961](https://github.com/feast-dev/feast/pull/961)
6. Feast Core Rest Endpoints [\#878](https://github.com/feast-dev/feast/pull/878)

### **Technical debt, refactoring, or housekeeping**

1. Improved integration testing framework [\#886](https://github.com/feast-dev/feast/pull/886)
2. Rectify all flaky batch tests [\#953](https://github.com/feast-dev/feast/pull/953), [\#982](https://github.com/feast-dev/feast/pull/982)
3. Decouple job management from Feast Core [\#951](https://github.com/feast-dev/feast/pull/951)

## Feast 0.6

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

## Feast 0.5

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

