# Design Decisions

## Job Management

* We currently support [optional consolidation](https://github.com/feast-dev/feast/pull/825/files#diff-11e36b0d5cfc6742aa51adf2812b664eR85) of jobs by source. This allows one job to populate many stores if all the store share a source. The upside is we need to provision less jobs. The downside is we need to managing the ingestion of data into stores independently becomes a lot more complicated. For the time being we will allow a flag to switch between both consolidation strategies, but we may remove consolidation in the future if it introduces too much complexity in the overall design.



