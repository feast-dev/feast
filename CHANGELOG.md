# Changelog

## [v0.11.0](https://github.com/feast-dev/feast/tree/v0.11.0) (2021-06-24)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.8...v0.11.0)

**Implemented enhancements:**

- Allow BigQuery project to be configured [\#1656](https://github.com/feast-dev/feast/pull/1656) ([MattDelac](https://github.com/MattDelac))
- Add to_bigquery function to BigQueryRetrievalJob [\#1634](https://github.com/feast-dev/feast/pull/1634) ([vtao2](https://github.com/vtao2))
- Add AWS authentication using github actions [\#1629](https://github.com/feast-dev/feast/pull/1629) ([tsotnet](https://github.com/tsotnet))
- Introduce an OnlineStore interface [\#1628](https://github.com/feast-dev/feast/pull/1628) ([achals](https://github.com/achals))
- Add to_df to convert get_online_feature response to pandas dataframe [\#1623](https://github.com/feast-dev/feast/pull/1623) ([tedhtchang](https://github.com/tedhtchang))
- Add datastore namespace option in configs [\#1581](https://github.com/feast-dev/feast/pull/1581) ([tsotnet](https://github.com/tsotnet))
- Add offline_store config [\#1552](https://github.com/feast-dev/feast/pull/1552) ([tsotnet](https://github.com/tsotnet))
- Entity value_type inference for Feature Repo registration [\#1538](https://github.com/feast-dev/feast/pull/1538) ([mavysavydav](https://github.com/mavysavydav))
- Inferencing of Features in FeatureView and timestamp column of DataSource [\#1523](https://github.com/feast-dev/feast/pull/1523) ([mavysavydav](https://github.com/mavysavydav))
- Add Unix Timestamp value type [\#1520](https://github.com/feast-dev/feast/pull/1520) ([MattDelac](https://github.com/MattDelac))
- Add support for Redis and Redis Cluster [\#1511](https://github.com/feast-dev/feast/pull/1511) ([qooba](https://github.com/qooba))
- Add path option to cli [\#1509](https://github.com/feast-dev/feast/pull/1509) ([tedhtchang](https://github.com/tedhtchang))

**Fixed bugs:**

- Schema Inferencing should happen at apply time [\#1646](https://github.com/feast-dev/feast/pull/1646) ([mavysavydav](https://github.com/mavysavydav))
- Don't use .result\(\) in BigQueryOfflineStore, since it still leads to OOM [\#1642](https://github.com/feast-dev/feast/pull/1642) ([tsotnet](https://github.com/tsotnet))
- Don't load entire bigquery query results in memory [\#1638](https://github.com/feast-dev/feast/pull/1638) ([tsotnet](https://github.com/tsotnet))
- Remove file loader & its test [\#1632](https://github.com/feast-dev/feast/pull/1632) ([tsotnet](https://github.com/tsotnet))
- Provide descriptive error on invalid table reference [\#1627](https://github.com/feast-dev/feast/pull/1627) ([codyjlin](https://github.com/codyjlin))
- Fix ttl duration when ttl is None [\#1624](https://github.com/feast-dev/feast/pull/1624) ([MattDelac](https://github.com/MattDelac))
- Fix race condition in historical e2e tests [\#1620](https://github.com/feast-dev/feast/pull/1620) ([woop](https://github.com/woop))
- Add validations when materializing from file sources [\#1615](https://github.com/feast-dev/feast/pull/1615) ([achals](https://github.com/achals))
- Add entity column validations when getting historical features from bigquery [\#1614](https://github.com/feast-dev/feast/pull/1614) ([achals](https://github.com/achals))
- Allow telemetry configuration to fail gracefully [\#1612](https://github.com/feast-dev/feast/pull/1612) ([achals](https://github.com/achals))
- Update type conversion from pandas to timestamp to support various the timestamp types [\#1603](https://github.com/feast-dev/feast/pull/1603) ([achals](https://github.com/achals))
- Add current directory in sys path for CLI commands that might depend on custom providers [\#1594](https://github.com/feast-dev/feast/pull/1594) ([MattDelac](https://github.com/MattDelac))
- Fix contention issue [\#1582](https://github.com/feast-dev/feast/pull/1582) ([woop](https://github.com/woop))
- Ensure that only None types fail predicate [\#1580](https://github.com/feast-dev/feast/pull/1580) ([woop](https://github.com/woop))
- Don't create bigquery dataset if it already exists [\#1569](https://github.com/feast-dev/feast/pull/1569) ([tsotnet](https://github.com/tsotnet))
- Don't lose materialization interval tracking when re-applying feature views [\#1559](https://github.com/feast-dev/feast/pull/1559) ([jklegar](https://github.com/jklegar))
- Validate project and repo names for apply and init commands [\#1558](https://github.com/feast-dev/feast/pull/1558) ([tedhtchang](https://github.com/tedhtchang))
- Bump supported Python version to 3.7 [\#1504](https://github.com/feast-dev/feast/pull/1504) ([tsotnet](https://github.com/tsotnet))

**Merged pull requests:**

- Rename telemetry to usage [\#1660](https://github.com/feast-dev/feast/pull/1660) ([tsotnet](https://github.com/tsotnet))
- Refactor OfflineStoreConfig classes into their owning modules [\#1657](https://github.com/feast-dev/feast/pull/1657) ([achals](https://github.com/achals))
- Run python unit tests in parallel [\#1652](https://github.com/feast-dev/feast/pull/1652) ([achals](https://github.com/achals))
- Refactor OnlineStoreConfig classes into owning modules [\#1649](https://github.com/feast-dev/feast/pull/1649) ([achals](https://github.com/achals))
- Fix table\_refs in BigQuerySource definitions [\#1644](https://github.com/feast-dev/feast/pull/1644) ([tsotnet](https://github.com/tsotnet))
- Make test historical retrieval longer [\#1630](https://github.com/feast-dev/feast/pull/1630) ([MattDelac](https://github.com/MattDelac))
- Fix failing historical retrieval assertion [\#1622](https://github.com/feast-dev/feast/pull/1622) ([woop](https://github.com/woop))
- Add a specific error for missing columns during materialization [\#1619](https://github.com/feast-dev/feast/pull/1619) ([achals](https://github.com/achals))
- Use drop\_duplicates\(\) instead of groupby \(about 1.5~2x faster\) [\#1617](https://github.com/feast-dev/feast/pull/1617) ([rightx2](https://github.com/rightx2))
- Optimize historical retrieval with BigQuery offline store [\#1602](https://github.com/feast-dev/feast/pull/1602) ([MattDelac](https://github.com/MattDelac))
- Use CONCAT\(\) instead of ROW\_NUMBER\(\) [\#1601](https://github.com/feast-dev/feast/pull/1601) ([MattDelac](https://github.com/MattDelac))
- Minor doc fix in the code snippet: Fix to reference the right instance for the retrieved job instance object [\#1599](https://github.com/feast-dev/feast/pull/1599) ([dmatrix](https://github.com/dmatrix))
- Repo and project names should not start with an underscore [\#1597](https://github.com/feast-dev/feast/pull/1597) ([tedhtchang](https://github.com/tedhtchang))
- Append nanoseconds to dataset name in test\_historical\_retrival to prevent tests stomping over each other [\#1593](https://github.com/feast-dev/feast/pull/1593) ([achals](https://github.com/achals))
- Make start and end timestamps tz aware in the CLI [\#1590](https://github.com/feast-dev/feast/pull/1590) ([achals](https://github.com/achals))
- Bump fastavro version [\#1585](https://github.com/feast-dev/feast/pull/1585) ([kevinhu](https://github.com/kevinhu))
- Change OfflineStore class description [\#1571](https://github.com/feast-dev/feast/pull/1571) ([tedhtchang](https://github.com/tedhtchang))
- Fix Sphinx documentation building [\#1563](https://github.com/feast-dev/feast/pull/1563) ([woop](https://github.com/woop))
- Add test coverage and remove MacOS integration tests [\#1562](https://github.com/feast-dev/feast/pull/1562) ([woop](https://github.com/woop))
- Improve GCP exception handling [\#1561](https://github.com/feast-dev/feast/pull/1561) ([woop](https://github.com/woop))
- Update default cli no option help message [\#1550](https://github.com/feast-dev/feast/pull/1550) ([tedhtchang](https://github.com/tedhtchang))
- Add opt-out exception logging telemetry [\#1535](https://github.com/feast-dev/feast/pull/1535) ([jklegar](https://github.com/jklegar))
- Add instruction for install Feast on IKS and OpenShift using Kustomize [\#1534](https://github.com/feast-dev/feast/pull/1534) ([tedhtchang](https://github.com/tedhtchang))
- BigQuery type to Feast type conversion chart update [\#1530](https://github.com/feast-dev/feast/pull/1530) ([mavysavydav](https://github.com/mavysavydav))
- remove unnecessay path join in setup.py [\#1529](https://github.com/feast-dev/feast/pull/1529) ([shihabuddinbuet](https://github.com/shihabuddinbuet))
- Add roadmap to documentation [\#1528](https://github.com/feast-dev/feast/pull/1528) ([woop](https://github.com/woop))
- Add test matrix for different Python versions [\#1526](https://github.com/feast-dev/feast/pull/1526) ([woop](https://github.com/woop))
- Update broken urls in the github pr template file [\#1521](https://github.com/feast-dev/feast/pull/1521) ([tedhtchang](https://github.com/tedhtchang))
- Add a fixed timestamp to quickstart data [\#1513](https://github.com/feast-dev/feast/pull/1513) ([jklegar](https://github.com/jklegar))
- Make gcp imports optional [\#1512](https://github.com/feast-dev/feast/pull/1512) ([jklegar](https://github.com/jklegar))
- Fix documentation inconsistency [\#1510](https://github.com/feast-dev/feast/pull/1510) ([jongillham](https://github.com/jongillham))
- Upgrade grpcio version in python SDK [\#1508](https://github.com/feast-dev/feast/pull/1508) ([szalai1](https://github.com/szalai1))
- pre-commit command typo fix in CONTRIBUTING.md [\#1506](https://github.com/feast-dev/feast/pull/1506) ([mavysavydav](https://github.com/mavysavydav))
- Add optional telemetry to other CLI commands [\#1505](https://github.com/feast-dev/feast/pull/1505) ([jklegar](https://github.com/jklegar))


## [v0.10.8](https://github.com/feast-dev/feast/tree/v0.10.8) (2021-06-17)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.7...v0.10.8)

**Implemented enhancements:**

- Add `to_bigquery()` function to BigQueryRetrievalJob [\#1634](https://github.com/feast-dev/feast/pull/1634) ([vtao2](https://github.com/vtao2))

**Fixed bugs:**

- Don't use .result\(\) in BigQueryOfflineStore, since it still leads to OOM [\#1642](https://github.com/feast-dev/feast/pull/1642) ([tsotnet](https://github.com/tsotnet))
- Don't load entire bigquery query results in memory [\#1638](https://github.com/feast-dev/feast/pull/1638) ([tsotnet](https://github.com/tsotnet))
- Add entity column validations when getting historical features from bigquery [\#1614](https://github.com/feast-dev/feast/pull/1614) ([achals](https://github.com/achals))

**Merged pull requests:**

- Make test historical retrieval longer [\#1630](https://github.com/feast-dev/feast/pull/1630) ([MattDelac](https://github.com/MattDelac))
- Fix failing historical retrieval assertion [\#1622](https://github.com/feast-dev/feast/pull/1622) ([woop](https://github.com/woop))
- Optimize historical retrieval with BigQuery offline store [\#1602](https://github.com/feast-dev/feast/pull/1602) ([MattDelac](https://github.com/MattDelac))

## [v0.10.7](https://github.com/feast-dev/feast/tree/v0.10.7) (2021-06-07)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.6...v0.10.7)

**Fixed bugs:**

- Fix race condition in historical e2e tests [\#1620](https://github.com/feast-dev/feast/pull/1620) ([woop](https://github.com/woop))

**Merged pull requests:**

- Use drop\_duplicates\(\) instead of groupby \(about 1.5~2x faster\) [\#1617](https://github.com/feast-dev/feast/pull/1617) ([rightx2](https://github.com/rightx2))
- Use CONCAT\(\) instead of ROW\_NUMBER\(\) [\#1601](https://github.com/feast-dev/feast/pull/1601) ([MattDelac](https://github.com/MattDelac))  
- Minor doc fix in the code snippet: Fix to reference the right instance for the retrieved job instance object [\#1599](https://github.com/feast-dev/feast/pull/1599) ([dmatrix](https://github.com/dmatrix))
- Append nanoseconds to dataset name in test\_historical\_retrival to prevent tests stomping over each other [\#1593](https://github.com/feast-dev/feast/pull/1593) ([achals](https://github.com/achals))
- Make start and end timestamps tz aware in the CLI [\#1590](https://github.com/feast-dev/feast/pull/1590) ([achals](https://github.com/achals))

## [v0.10.6](https://github.com/feast-dev/feast/tree/v0.10.6) (2021-05-27)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.5...v0.10.6)

**Implemented enhancements:**

- Add datastore namespace option in configs [\#1581](https://github.com/feast-dev/feast/pull/1581) ([tsotnet](https://github.com/tsotnet))

**Fixed bugs:**

- Fix contention issue [\#1582](https://github.com/feast-dev/feast/pull/1582) ([woop](https://github.com/woop))
- Ensure that only None types fail predicate [\#1580](https://github.com/feast-dev/feast/pull/1580) ([woop](https://github.com/woop))
- Don't create bigquery dataset if it already exists [\#1569](https://github.com/feast-dev/feast/pull/1569) ([tsotnet](https://github.com/tsotnet))

**Merged pull requests:**

- Change OfflineStore class description [\#1571](https://github.com/feast-dev/feast/pull/1571) ([tedhtchang](https://github.com/tedhtchang))


## [v0.10.5](https://github.com/feast-dev/feast/tree/v0.10.5) (2021-05-19)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.4...v0.10.5)

**Implemented enhancements:**

- Add offline\_store config [\#1552](https://github.com/feast-dev/feast/pull/1552) ([tsotnet](https://github.com/tsotnet))

**Fixed bugs:**

- Validate project and repo names for apply and init commands [\#1558](https://github.com/feast-dev/feast/pull/1558) ([tedhtchang](https://github.com/tedhtchang))

**Merged pull requests:**

- Fix Sphinx documentation building [\#1563](https://github.com/feast-dev/feast/pull/1563) ([woop](https://github.com/woop))
- Add test coverage and remove MacOS integration tests [\#1562](https://github.com/feast-dev/feast/pull/1562) ([woop](https://github.com/woop))
- Improve GCP exception handling [\#1561](https://github.com/feast-dev/feast/pull/1561) ([woop](https://github.com/woop))
- Update default cli no option help message [\#1550](https://github.com/feast-dev/feast/pull/1550) ([tedhtchang](https://github.com/tedhtchang))
- Add opt-out exception logging telemetry [\#1535](https://github.com/feast-dev/feast/pull/1535) ([jklegar](https://github.com/jklegar))
- Add instruction for install Feast on IKS and OpenShift using Kustomize [\#1534](https://github.com/feast-dev/feast/pull/1534) ([tedhtchang](https://github.com/tedhtchang))

## [v0.10.4](https://github.com/feast-dev/feast/tree/v0.10.4) (2021-05-12)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.3...v0.10.4)

**Implemented enhancements:**

- Inferencing of Features in FeatureView and timestamp column of DataSource [\#1523](https://github.com/feast-dev/feast/pull/1523) ([mavysavydav](https://github.com/mavysavydav))
- Add Unix Timestamp value type [\#1520](https://github.com/feast-dev/feast/pull/1520) ([MattDelac](https://github.com/MattDelac))
- Fix materialize for None [\#1481](https://github.com/feast-dev/feast/pull/1481) ([qooba](https://github.com/qooba))

**Merged pull requests:**

- BigQuery type to Feast type conversion chart update [\#1530](https://github.com/feast-dev/feast/pull/1530) ([mavysavydav](https://github.com/mavysavydav))
- remove unnecessay path join in setup.py [\#1529](https://github.com/feast-dev/feast/pull/1529) ([shihabuddinbuet](https://github.com/shihabuddinbuet))
- Add roadmap to documentation [\#1528](https://github.com/feast-dev/feast/pull/1528) ([woop](https://github.com/woop))
- Add test matrix for different Python versions [\#1526](https://github.com/feast-dev/feast/pull/1526) ([woop](https://github.com/woop))
- Update broken urls in the github pr template file [\#1521](https://github.com/feast-dev/feast/pull/1521) ([tedhtchang](https://github.com/tedhtchang))
- Upgrade grpcio version in python SDK [\#1508](https://github.com/feast-dev/feast/pull/1508) ([szalai1](https://github.com/szalai1))
- Better logging for materialize command [\#1499](https://github.com/feast-dev/feast/pull/1499) ([jklegar](https://github.com/jklegar))


## [v0.10.3](https://github.com/feast-dev/feast/tree/v0.10.3) (2021-04-21)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.2...v0.10.3)

**Implemented enhancements:**

- Add support for third party providers [\#1501](https://github.com/feast-dev/feast/pull/1501) ([tsotnet](https://github.com/tsotnet))
- Infer entity dataframe event timestamp column [\#1495](https://github.com/feast-dev/feast/pull/1495) ([jklegar](https://github.com/jklegar))
- Allow Feast apply to import files recursively \(and add .feastignore\) [\#1482](https://github.com/feast-dev/feast/pull/1482) ([tsotnet](https://github.com/tsotnet))

**Fixed bugs:**

- Bump supported Python version to 3.7 [\#1504](https://github.com/feast-dev/feast/pull/1504) ([tsotnet](https://github.com/tsotnet))
- Fix bug in allowing empty repositories to be applied to a GCS registry [\#1488](https://github.com/feast-dev/feast/pull/1488) ([woop](https://github.com/woop))

**Merged pull requests:**

- Add a fixed timestamp to quickstart data [\#1513](https://github.com/feast-dev/feast/pull/1513) ([jklegar](https://github.com/jklegar))
- Make gcp imports optional [\#1512](https://github.com/feast-dev/feast/pull/1512) ([jklegar](https://github.com/jklegar))
- Fix documentation inconsistency [\#1510](https://github.com/feast-dev/feast/pull/1510) ([jongillham](https://github.com/jongillham))
- pre-commit command typo fix in CONTRIBUTING.md [\#1506](https://github.com/feast-dev/feast/pull/1506) ([mavysavydav](https://github.com/mavysavydav))
- Add optional telemetry to other CLI commands [\#1505](https://github.com/feast-dev/feast/pull/1505) ([jklegar](https://github.com/jklegar))
- Pass entities information to Provider [\#1498](https://github.com/feast-dev/feast/pull/1498) ([MattDelac](https://github.com/MattDelac))
- Update broken urls in contributing.md [\#1489](https://github.com/feast-dev/feast/pull/1489) ([tedhtchang](https://github.com/tedhtchang))
- Python docs formatting fixes [\#1473](https://github.com/feast-dev/feast/pull/1473) ([jklegar](https://github.com/jklegar))

## [v0.10.2](https://github.com/feast-dev/feast/tree/v0.10.2) (2021-04-21)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.1...v0.10.2)

**Fixed bugs:**

- Fix bug in allowing empty repositories to be applied to a GCS registry [\#1488](https://github.com/feast-dev/feast/pull/1488) ([woop](https://github.com/woop))

## [v0.10.1](https://github.com/feast-dev/feast/tree/v0.10.1) (2021-04-21)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.0...v0.10.1)

**Fixed bugs:**

- Fix time zone issue with get\_historical\_features [\#1475](https://github.com/feast-dev/feast/pull/1475) ([tsotnet](https://github.com/tsotnet))

**Merged pull requests:**

- Improve exception handling, logging, and validation [\#1477](https://github.com/feast-dev/feast/pull/1477) ([woop](https://github.com/woop))
- Remove duped pic [\#1476](https://github.com/feast-dev/feast/pull/1476) ([YikSanChan](https://github.com/YikSanChan))
- Fix created timestamp related errors for BigQuery source [\#1474](https://github.com/feast-dev/feast/pull/1474) ([jklegar](https://github.com/jklegar))
- Remove unnecessary MAVEN\_CONFIG [\#1472](https://github.com/feast-dev/feast/pull/1472) ([danielsiwiec](https://github.com/danielsiwiec))
- Fix CLI entities command & add feature-views command [\#1471](https://github.com/feast-dev/feast/pull/1471) ([tsotnet](https://github.com/tsotnet))


## [v0.10.0](https://github.com/feast-dev/feast/tree/0.10.0) (2021-04-15)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.9.5...v0.10.0)

** Implemented enhancements:**

- Add template generation to Feast CLI for Google Cloud Platform [\#1460](https://github.com/feast-dev/feast/pull/1460) ([woop](https://github.com/woop))
- Add support for retrieving data from sources that don't match providers [\#1454](https://github.com/feast-dev/feast/pull/1454) ([woop](https://github.com/woop))
- Add materialize-incremental CLI command [\#1442](https://github.com/feast-dev/feast/pull/1442) ([tsotnet](https://github.com/tsotnet))
- Add registry refreshing and caching [\#1431](https://github.com/feast-dev/feast/pull/1431) ([woop](https://github.com/woop))
- Add missing FeatureStore methods [\#1423](https://github.com/feast-dev/feast/pull/1423) ([jklegar](https://github.com/jklegar))
- Allow importing of new FeatureStore classes [\#1422](https://github.com/feast-dev/feast/pull/1422) ([woop](https://github.com/woop))
- Add Feast init command [\#1414](https://github.com/feast-dev/feast/pull/1414) ([oavdeev](https://github.com/oavdeev))
- Add support for parquet ingestion [\#1410](https://github.com/feast-dev/feast/pull/1410) ([oavdeev](https://github.com/oavdeev))
- Add materialize\_incremental method [\#1407](https://github.com/feast-dev/feast/pull/1407) ([jklegar](https://github.com/jklegar))
- Add support for pull query from BigQuery [\#1403](https://github.com/feast-dev/feast/pull/1403) ([jklegar](https://github.com/jklegar))
- Add support for partial apply to create infra [\#1402](https://github.com/feast-dev/feast/pull/1402) ([oavdeev](https://github.com/oavdeev))
- Add online read API to FeatureStore class [\#1399](https://github.com/feast-dev/feast/pull/1399) ([oavdeev](https://github.com/oavdeev))
- Add historical retrieval for BigQuery and Parquet [\#1389](https://github.com/feast-dev/feast/pull/1389) ([woop](https://github.com/woop))
- Add feature views [\#1386](https://github.com/feast-dev/feast/pull/1386) ([oavdeev](https://github.com/oavdeev))
- Implement materialize method [\#1379](https://github.com/feast-dev/feast/pull/1379) ([jklegar](https://github.com/jklegar))
- Read and write path for Datastore and SQLite [\#1376](https://github.com/feast-dev/feast/pull/1376) ([oavdeev](https://github.com/oavdeev))
- Download BigQuery table to pyarrow table for python-based ingestion flow [\#1366](https://github.com/feast-dev/feast/pull/1366) ([jklegar](https://github.com/jklegar))
- FeatureStore, FeatureView, Config, and BigQuerySource classes for updated SDK [\#1364](https://github.com/feast-dev/feast/pull/1364) ([jklegar](https://github.com/jklegar))
- Add support for new deploy CLI [\#1362](https://github.com/feast-dev/feast/pull/1362) ([oavdeev](https://github.com/oavdeev))

** Fixed bugs:**

- Fix time zone access with native python datetimes [\#1469](https://github.com/feast-dev/feast/pull/1469) ([tsotnet](https://github.com/tsotnet))
- Small fixes for created\_timestamp [\#1468](https://github.com/feast-dev/feast/pull/1468) ([jklegar](https://github.com/jklegar))
- Fix offline store \(tz-naive & field\_mapping issues\) [\#1466](https://github.com/feast-dev/feast/pull/1466) ([tsotnet](https://github.com/tsotnet))
- Fix get\_online\_features return schema [\#1455](https://github.com/feast-dev/feast/pull/1455) ([jklegar](https://github.com/jklegar))
- Fix noisy path warning [\#1452](https://github.com/feast-dev/feast/pull/1452) ([woop](https://github.com/woop))
- Fix flaky test\_feature\_store fixture [\#1447](https://github.com/feast-dev/feast/pull/1447) ([jklegar](https://github.com/jklegar))
- Use timestamp check for token refresh [\#1444](https://github.com/feast-dev/feast/pull/1444) ([terryyylim](https://github.com/terryyylim))
- Fix bug in event timestamp removal in local mode [\#1441](https://github.com/feast-dev/feast/pull/1441) ([jklegar](https://github.com/jklegar))
- Fix timezone issue in materialize & materialize\_incremental [\#1439](https://github.com/feast-dev/feast/pull/1439) ([tsotnet](https://github.com/tsotnet))
- Fix materialization\_intervals initialization in FeatureView [\#1438](https://github.com/feast-dev/feast/pull/1438) ([tsotnet](https://github.com/tsotnet))
- Fix broken Terraform installation files [\#1420](https://github.com/feast-dev/feast/pull/1420) ([josegpg](https://github.com/josegpg))
- Fix retry handling for GCP datastore [\#1416](https://github.com/feast-dev/feast/pull/1416) ([oavdeev](https://github.com/oavdeev))
- Make CLI apply in local mode idempotent [\#1401](https://github.com/feast-dev/feast/pull/1401) ([oavdeev](https://github.com/oavdeev))
- Fix a bug in client archive\_project method and fix lint in grpc auth [\#1396](https://github.com/feast-dev/feast/pull/1396) ([randxie](https://github.com/randxie))

**Merged pull requests:**

- Change GCP template names to match local template [\#1470](https://github.com/feast-dev/feast/pull/1470) ([jklegar](https://github.com/jklegar))
- Add logging to materialize [\#1467](https://github.com/feast-dev/feast/pull/1467) ([woop](https://github.com/woop))
- Validate timestamp column present in entity dataframe [\#1464](https://github.com/feast-dev/feast/pull/1464) ([jklegar](https://github.com/jklegar))
- Fix & clean up Feast CLI commands [\#1463](https://github.com/feast-dev/feast/pull/1463) ([tsotnet](https://github.com/tsotnet))
- Flatten configuration structure for online store [\#1459](https://github.com/feast-dev/feast/pull/1459) ([woop](https://github.com/woop))
- Optimize write rate in Gcp Firestore [\#1458](https://github.com/feast-dev/feast/pull/1458) ([tsotnet](https://github.com/tsotnet))
- Allow apply to take a single Entity or FeatureView [\#1457](https://github.com/feast-dev/feast/pull/1457) ([jklegar](https://github.com/jklegar))
- Validate datetimes in materialize in correct order [\#1456](https://github.com/feast-dev/feast/pull/1456) ([jklegar](https://github.com/jklegar))
- Add test to ensure saving and loading from registry is safe [\#1453](https://github.com/feast-dev/feast/pull/1453) ([woop](https://github.com/woop))
- Port telemetry to FeatureStore API [\#1446](https://github.com/feast-dev/feast/pull/1446) ([jklegar](https://github.com/jklegar))
- Add materialize-incremental cli test [\#1445](https://github.com/feast-dev/feast/pull/1445) ([tsotnet](https://github.com/tsotnet))
- Support join keys in historical feature retrieval [\#1440](https://github.com/feast-dev/feast/pull/1440) ([jklegar](https://github.com/jklegar))
- Refactor OfflineStore into Provider [\#1437](https://github.com/feast-dev/feast/pull/1437) ([woop](https://github.com/woop))
- Fix multi-entity online retrieval [\#1435](https://github.com/feast-dev/feast/pull/1435) ([woop](https://github.com/woop))
- Fix feature name consistency between online & historical apis [\#1434](https://github.com/feast-dev/feast/pull/1434) ([tsotnet](https://github.com/tsotnet))
- Rename Metadata Store to Registry [\#1433](https://github.com/feast-dev/feast/pull/1433) ([woop](https://github.com/woop))
- Add support for Pydantic as configuration loader [\#1432](https://github.com/feast-dev/feast/pull/1432) ([woop](https://github.com/woop))
- Add entity join key and fix entity references [\#1429](https://github.com/feast-dev/feast/pull/1429) ([jklegar](https://github.com/jklegar))
- Slightly more sensible test names [\#1428](https://github.com/feast-dev/feast/pull/1428) ([oavdeev](https://github.com/oavdeev))
- Make entity description optional and fix empty table\_ref [\#1425](https://github.com/feast-dev/feast/pull/1425) ([jklegar](https://github.com/jklegar))
- Add Development Guide for Main Feast Repo Feast Components [\#1424](https://github.com/feast-dev/feast/pull/1424) ([mrzzy](https://github.com/mrzzy))
- Fix protobuf building for Python SDK [\#1418](https://github.com/feast-dev/feast/pull/1418) ([woop](https://github.com/woop))
- Add project name generator [\#1417](https://github.com/feast-dev/feast/pull/1417) ([woop](https://github.com/woop))
- \[Python SDK\]\[Auth\] Refresh token id w/o gcloud cli [\#1413](https://github.com/feast-dev/feast/pull/1413) ([pyalex](https://github.com/pyalex))
- Firestore ingestion perf improvements + benchmark script [\#1411](https://github.com/feast-dev/feast/pull/1411) ([oavdeev](https://github.com/oavdeev))
- Fixed old urls in documentation [\#1406](https://github.com/feast-dev/feast/pull/1406) ([tedhtchang](https://github.com/tedhtchang))
- Upgrade Gcloud setup dependency [\#1405](https://github.com/feast-dev/feast/pull/1405) ([woop](https://github.com/woop))
- Fix documentation building for Feast SDK [\#1400](https://github.com/feast-dev/feast/pull/1400) ([woop](https://github.com/woop))
- Bump jinja2 from 2.11.2 to 2.11.3 in /sdk/python [\#1398](https://github.com/feast-dev/feast/pull/1398) ([dependabot[bot]](https://github.com/apps/dependabot))
- Improve spark-on-k8s-operator documentation [\#1397](https://github.com/feast-dev/feast/pull/1397) ([jklegar](https://github.com/jklegar))
- Update Python SDK dependencies [\#1394](https://github.com/feast-dev/feast/pull/1394) ([woop](https://github.com/woop))
- Move Python proto package into submodule [\#1393](https://github.com/feast-dev/feast/pull/1393) ([woop](https://github.com/woop))
- Add nicer validation for repo config [\#1392](https://github.com/feast-dev/feast/pull/1392) ([oavdeev](https://github.com/oavdeev))
- Remove Python CI dependencies [\#1390](https://github.com/feast-dev/feast/pull/1390) ([woop](https://github.com/woop))
- Move Project field to Table/View spec [\#1388](https://github.com/feast-dev/feast/pull/1388) ([woop](https://github.com/woop))
- Remove Mirror CI [\#1387](https://github.com/feast-dev/feast/pull/1387) ([woop](https://github.com/woop))
- Add feedback link to install docs page [\#1375](https://github.com/feast-dev/feast/pull/1375) ([jparthasarthy](https://github.com/jparthasarthy))
- Support multiple features per key in firestore format spec [\#1374](https://github.com/feast-dev/feast/pull/1374) ([oavdeev](https://github.com/oavdeev))
- Fix hashing algorithm in the firestore spec [\#1373](https://github.com/feast-dev/feast/pull/1373) ([oavdeev](https://github.com/oavdeev))
- Support make protos on Mac [\#1371](https://github.com/feast-dev/feast/pull/1371) ([tedhtchang](https://github.com/tedhtchang))
- Add support for privileged tests [\#1369](https://github.com/feast-dev/feast/pull/1369) ([woop](https://github.com/woop))
- Remove base tests folder [\#1368](https://github.com/feast-dev/feast/pull/1368) ([woop](https://github.com/woop))
- Add Firestore online format specification [\#1367](https://github.com/feast-dev/feast/pull/1367) ([oavdeev](https://github.com/oavdeev))
- Improve documentation for k8s-spark resource template [\#1363](https://github.com/feast-dev/feast/pull/1363) ([theofpa](https://github.com/theofpa))

## [v0.9.1](https://github.com/feast-dev/feast/tree/v0.9.1) (2021-01-29)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.9.0...v0.9.1)

**Implemented enhancements:**

- Add telemetry to Python SDK [\#1289](https://github.com/feast-dev/feast/pull/1289) ([jklegar](https://github.com/jklegar))

**Fixed bugs:**

- Fix kafka download url [\#1298](https://github.com/feast-dev/feast/pull/1298) ([jklegar](https://github.com/jklegar))
- disable telemetry in docker-compose test and job\_service [\#1297](https://github.com/feast-dev/feast/pull/1297) ([jklegar](https://github.com/jklegar))


## [v0.9.0](https://github.com/feast-dev/feast/tree/v0.9.0) (2021-01-28)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.8.4...v0.9.0)

**Implemented enhancements:**

- Enable user to provide spark job template as input for jobservice deployment [\#1285](https://github.com/feast-dev/feast/pull/1285) ([khorshuheng](https://github.com/khorshuheng))
- Add feature table name filter to jobs list api [\#1282](https://github.com/feast-dev/feast/pull/1282) ([terryyylim](https://github.com/terryyylim))
- Report observed value for aggregated checks in pre-ingestion feature validation [\#1278](https://github.com/feast-dev/feast/pull/1278) ([pyalex](https://github.com/pyalex))
- Add docs page for Azure setup [\#1276](https://github.com/feast-dev/feast/pull/1276) ([jklegar](https://github.com/jklegar))
- Azure example terraform [\#1274](https://github.com/feast-dev/feast/pull/1274) ([jklegar](https://github.com/jklegar))


**Fixed bugs:**

- make EMR jar uploader work the same as k8s one [\#1284](https://github.com/feast-dev/feast/pull/1284) ([oavdeev](https://github.com/oavdeev))
- Don't error when azure vars not set [\#1277](https://github.com/feast-dev/feast/pull/1277) ([jklegar](https://github.com/jklegar))
- Prevent ingestion job config parser from unwanted fieldMapping transformation [\#1261](https://github.com/feast-dev/feast/pull/1261) ([pyalex](https://github.com/pyalex))
- Features are not being ingested due to max age overflow [\#1209](https://github.com/feast-dev/feast/pull/1209) ([pyalex](https://github.com/pyalex))
- Feature Table is not being update when only max\_age was changed [\#1208](https://github.com/feast-dev/feast/pull/1208) ([pyalex](https://github.com/pyalex))
- Truncate staging timestamps in entities dataset to ms [\#1207](https://github.com/feast-dev/feast/pull/1207) ([pyalex](https://github.com/pyalex))
- Bump terraform rds module version [\#1204](https://github.com/feast-dev/feast/pull/1204) ([oavdeev](https://github.com/oavdeev))


**Merged pull requests:**

- Use date partitioning column in FileSource [\#1293](https://github.com/feast-dev/feast/pull/1293) ([pyalex](https://github.com/pyalex))
- Add EMR CI/CD entrypoint script [\#1290](https://github.com/feast-dev/feast/pull/1290) ([oavdeev](https://github.com/oavdeev))
- Online serving optimizations [\#1286](https://github.com/feast-dev/feast/pull/1286) ([pyalex](https://github.com/pyalex))
- Make third party grpc packages recognizable as python module [\#1283](https://github.com/feast-dev/feast/pull/1283) ([khorshuheng](https://github.com/khorshuheng))
- Report observed values in feature validation as Gauge [\#1280](https://github.com/feast-dev/feast/pull/1280) ([pyalex](https://github.com/pyalex))
- Keep same amount of partitions after repartitioning in IngestionJob [\#1279](https://github.com/feast-dev/feast/pull/1279) ([pyalex](https://github.com/pyalex))
- Add request feature counter metric [\#1272](https://github.com/feast-dev/feast/pull/1272) ([terryyylim](https://github.com/terryyylim))
- Use SEND\_INTERRUPT to cancel EMR jobs [\#1271](https://github.com/feast-dev/feast/pull/1271) ([oavdeev](https://github.com/oavdeev))
- Fix historical test flakiness [\#1270](https://github.com/feast-dev/feast/pull/1270) ([jklegar](https://github.com/jklegar))
- Allow https url for spark ingestion jar [\#1266](https://github.com/feast-dev/feast/pull/1266) ([jklegar](https://github.com/jklegar))
- Add project name to feature validation metric [\#1264](https://github.com/feast-dev/feast/pull/1264) ([pyalex](https://github.com/pyalex))
- Use dataproc console url instead of gcs for log uri [\#1263](https://github.com/feast-dev/feast/pull/1263) ([khorshuheng](https://github.com/khorshuheng))
- Make nodes priority \(for redis cluster\) configurable in Serving [\#1260](https://github.com/feast-dev/feast/pull/1260) ([pyalex](https://github.com/pyalex))
- Enhance job api to return associated feature table and start time [\#1259](https://github.com/feast-dev/feast/pull/1259) ([khorshuheng](https://github.com/khorshuheng))
- Reporting metrics from validation UDF [\#1256](https://github.com/feast-dev/feast/pull/1256) ([pyalex](https://github.com/pyalex))
- Allow use the same timestamp column for both created & even timestamp in Historical Retrieval [\#1255](https://github.com/feast-dev/feast/pull/1255) ([pyalex](https://github.com/pyalex))
- Apply grpc tracing interceptor on Feast SDK [\#1243](https://github.com/feast-dev/feast/pull/1243) ([khorshuheng](https://github.com/khorshuheng))
- Apply grpc tracing interceptor on online serving [\#1242](https://github.com/feast-dev/feast/pull/1242) ([khorshuheng](https://github.com/khorshuheng))
- Python UDF in Ingestion being used for feature validation [\#1234](https://github.com/feast-dev/feast/pull/1234) ([pyalex](https://github.com/pyalex))
- Add spark k8s operator launcher [\#1225](https://github.com/feast-dev/feast/pull/1225) ([oavdeev](https://github.com/oavdeev))
- Add deadletter/read-from-source metrics to batch and stream ingestion [\#1223](https://github.com/feast-dev/feast/pull/1223) ([terryyylim](https://github.com/terryyylim))
- Implement AbstractStagingClient for azure blob storage [\#1218](https://github.com/feast-dev/feast/pull/1218) ([jklegar](https://github.com/jklegar))
- Configurable materialization destination for view in BigQuerySource [\#1201](https://github.com/feast-dev/feast/pull/1201) ([pyalex](https://github.com/pyalex))
- Update Feast Core list features method [\#1176](https://github.com/feast-dev/feast/pull/1176) ([terryyylim](https://github.com/terryyylim))
- S3 endpoint configuration \#1169 [\#1172](https://github.com/feast-dev/feast/pull/1172) ([mike0sv](https://github.com/mike0sv))
- Increase kafka consumer waiting time in e2e tests [\#1268](https://github.com/feast-dev/feast/pull/1268) ([pyalex](https://github.com/pyalex))
- E2E tests support for jobservice's control loop [\#1267](https://github.com/feast-dev/feast/pull/1267) ([pyalex](https://github.com/pyalex))
- Optimize memory footprint for Spark Ingestion Job [\#1265](https://github.com/feast-dev/feast/pull/1265) ([pyalex](https://github.com/pyalex))
- Fix historical test for azure [\#1262](https://github.com/feast-dev/feast/pull/1262) ([jklegar](https://github.com/jklegar))
- Change azure https to wasbs and add azure creds to spark [\#1258](https://github.com/feast-dev/feast/pull/1258) ([jklegar](https://github.com/jklegar))
- Docs, fixes and scripts to run e2e tests in minikube [\#1254](https://github.com/feast-dev/feast/pull/1254) ([oavdeev](https://github.com/oavdeev))
- Fix azure blob storage access in e2e tests [\#1253](https://github.com/feast-dev/feast/pull/1253) ([jklegar](https://github.com/jklegar))
- Update python version requirements to 3.7 for Dataproc launcher [\#1251](https://github.com/feast-dev/feast/pull/1251) ([pyalex](https://github.com/pyalex))
- Fix build-ingestion-py-dependencies script [\#1250](https://github.com/feast-dev/feast/pull/1250) ([pyalex](https://github.com/pyalex))
- Add datadog\(statsd\) client to python package for IngestionJob [\#1249](https://github.com/feast-dev/feast/pull/1249) ([pyalex](https://github.com/pyalex))
- Add prow job for azure e2e test [\#1244](https://github.com/feast-dev/feast/pull/1244) ([jklegar](https://github.com/jklegar))
- Azure e2e test [\#1241](https://github.com/feast-dev/feast/pull/1241) ([jklegar](https://github.com/jklegar))
- Add Feast Serving histogram metrics [\#1240](https://github.com/feast-dev/feast/pull/1240) ([terryyylim](https://github.com/terryyylim))
- CI should work on python 3.6 [\#1237](https://github.com/feast-dev/feast/pull/1237) ([pyalex](https://github.com/pyalex))
- Integration test for k8s spark operator support [\#1236](https://github.com/feast-dev/feast/pull/1236) ([oavdeev](https://github.com/oavdeev))
- Add prow config for spark k8s operator integration testing [\#1235](https://github.com/feast-dev/feast/pull/1235) ([oavdeev-tt](https://github.com/oavdeev-tt))
- Upgrading spark to 3.0.1 [\#1227](https://github.com/feast-dev/feast/pull/1227) ([pyalex](https://github.com/pyalex))
- Support TFRecord as one of the output formats for historical feature retrieval [\#1222](https://github.com/feast-dev/feast/pull/1222) ([khorshuheng](https://github.com/khorshuheng))
- Remove stage\_dataframe from the launcher interface [\#1220](https://github.com/feast-dev/feast/pull/1220) ([oavdeev](https://github.com/oavdeev))
- Refactor staging client uploader and use it in EMR launcher [\#1219](https://github.com/feast-dev/feast/pull/1219) ([oavdeev](https://github.com/oavdeev))
- Remove unused EMR code [\#1217](https://github.com/feast-dev/feast/pull/1217) ([oavdeev](https://github.com/oavdeev))
- Remove job id from ingested row counter metric [\#1216](https://github.com/feast-dev/feast/pull/1216) ([terryyylim](https://github.com/terryyylim))
- Quickstart link fixed [\#1213](https://github.com/feast-dev/feast/pull/1213) ([szczeles](https://github.com/szczeles))
- Delete v1 concepts [\#1194](https://github.com/feast-dev/feast/pull/1194) ([terryyylim](https://github.com/terryyylim))
- Dont write defaults to config [\#1188](https://github.com/feast-dev/feast/pull/1188) ([mike0sv](https://github.com/mike0sv))
- Refactor tests which utilizes feature sets [\#1186](https://github.com/feast-dev/feast/pull/1186) ([terryyylim](https://github.com/terryyylim))
- Refactor configurable options and add sphinx docs [\#1174](https://github.com/feast-dev/feast/pull/1174) ([terryyylim](https://github.com/terryyylim))
- Remove unnecessary Google Auth dependency [\#1170](https://github.com/feast-dev/feast/pull/1170) ([woop](https://github.com/woop))


## [v0.8.2](https://github.com/feast-dev/feast/tree/v0.8.2) (2020-12-01)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.8.1...v0.8.2)

**Implemented enhancements:**

- Configurable materialization destination for view in BigQuerySource [\#1201](https://github.com/feast-dev/feast/pull/1201) ([pyalex](https://github.com/pyalex))

**Fixed bugs:**

- Fix tag order for release workflow [\#1205](https://github.com/feast-dev/feast/pull/1205) ([terryyylim](https://github.com/terryyylim))
- Fix Feature Table not updated on new feature addition [\#1197](https://github.com/feast-dev/feast/pull/1197) ([khorshuheng](https://github.com/khorshuheng))

**Merged pull requests:**

- Suppress kafka logs in Ingestion Job [\#1206](https://github.com/feast-dev/feast/pull/1206) ([pyalex](https://github.com/pyalex))
- Add project name to metrics labels in Ingestion Job [\#1202](https://github.com/feast-dev/feast/pull/1202) ([pyalex](https://github.com/pyalex))


## [v0.8.1](https://github.com/feast-dev/feast/tree/v0.8.1) (2020-11-24)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.8.0...v0.8.1)

**Implemented enhancements:**

- Expires Redis Keys based on Feature Table Max Age [\#1161](https://github.com/feast-dev/feast/pull/1161) ([khorshuheng](https://github.com/khorshuheng))
- Jobservice control loop \(based on \#1140\) [\#1156](https://github.com/feast-dev/feast/pull/1156) ([oavdeev](https://github.com/oavdeev))

**Fixed bugs:**

- Lazy metrics initialization \(to correct pick up in executor\) [\#1195](https://github.com/feast-dev/feast/pull/1195) ([pyalex](https://github.com/pyalex))
- Add missing third\_party folder [\#1185](https://github.com/feast-dev/feast/pull/1185) ([terryyylim](https://github.com/terryyylim))
- Fix missing name variable instantiation [\#1166](https://github.com/feast-dev/feast/pull/1166) ([terryyylim](https://github.com/terryyylim))

**Merged pull requests:**

- Bump ssh-agent version [\#1175](https://github.com/feast-dev/feast/pull/1175) ([terryyylim](https://github.com/terryyylim))
- Refactor configurable options and add sphinx docs [\#1174](https://github.com/feast-dev/feast/pull/1174) ([terryyylim](https://github.com/terryyylim))
- Stabilize flaky e2e tests [\#1173](https://github.com/feast-dev/feast/pull/1173) ([pyalex](https://github.com/pyalex))
- Fix connection resets in CI for Maven [\#1164](https://github.com/feast-dev/feast/pull/1164) ([woop](https://github.com/woop))
- Add dataproc executor resource config [\#1160](https://github.com/feast-dev/feast/pull/1160) ([terryyylim](https://github.com/terryyylim))
- Fix github workflow deprecating env variable [\#1158](https://github.com/feast-dev/feast/pull/1158) ([terryyylim](https://github.com/terryyylim))
- Ensure consistency of github workflow [\#1157](https://github.com/feast-dev/feast/pull/1157) ([terryyylim](https://github.com/terryyylim))


## [v0.8.0](https://github.com/feast-dev/feast/tree/v0.8.0) (2020-11-10)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.7.1...v0.8.0)

**Implemented enhancements:**

- Implement JobService API calls & connect it to SDK [\#1129](https://github.com/feast-dev/feast/pull/1129) ([tsotnet](https://github.com/tsotnet))
- Allow user to specify custom secrets to be mounted on Feast Serving and Feast Core pods [\#1127](https://github.com/feast-dev/feast/pull/1127) ([khorshuheng](https://github.com/khorshuheng))
- Allow spark expressions in field mapping during Ingestion [\#1122](https://github.com/feast-dev/feast/pull/1122) ([pyalex](https://github.com/pyalex))
- Update rest endpoints [\#1121](https://github.com/feast-dev/feast/pull/1121) ([terryyylim](https://github.com/terryyylim))
- Add feature table deletion [\#1114](https://github.com/feast-dev/feast/pull/1114) ([terryyylim](https://github.com/terryyylim))
- Add historical retrieval via job service [\#1107](https://github.com/feast-dev/feast/pull/1107) ([oavdeev](https://github.com/oavdeev))
- Implement list job and get job methods for Dataproc launcher [\#1106](https://github.com/feast-dev/feast/pull/1106) ([khorshuheng](https://github.com/khorshuheng))
- Allow entities and features to be updated [\#1105](https://github.com/feast-dev/feast/pull/1105) ([terryyylim](https://github.com/terryyylim))
- Add get\_by\_id and list\_jobs interface to the launcher interface and implement it for EMR [\#1095](https://github.com/feast-dev/feast/pull/1095) ([oavdeev](https://github.com/oavdeev))
- Support redis ssl in feast-serving [\#1092](https://github.com/feast-dev/feast/pull/1092) ([oavdeev](https://github.com/oavdeev))
- Add helm charts for feast jobservice [\#1081](https://github.com/feast-dev/feast/pull/1081) ([tsotnet](https://github.com/tsotnet))
- Terraform cleanup: tags, formatting, better defaults [\#1080](https://github.com/feast-dev/feast/pull/1080) ([oavdeev](https://github.com/oavdeev))
- Update docker-compose for Feast 0.8 [\#1078](https://github.com/feast-dev/feast/pull/1078) ([khorshuheng](https://github.com/khorshuheng))
- Configure jupyter env on AWS via terraform [\#1077](https://github.com/feast-dev/feast/pull/1077) ([oavdeev](https://github.com/oavdeev))
- Implement EMR job cancelling [\#1075](https://github.com/feast-dev/feast/pull/1075) ([oavdeev](https://github.com/oavdeev))
- Streaming Ingestion Job supports AVRO format as input [\#1072](https://github.com/feast-dev/feast/pull/1072) ([pyalex](https://github.com/pyalex))
- Accept Pandas dataframe as input for historical feature retrieval [\#1071](https://github.com/feast-dev/feast/pull/1071) ([khorshuheng](https://github.com/khorshuheng))
- Add EMR streaming job launcher [\#1065](https://github.com/feast-dev/feast/pull/1065) ([oavdeev](https://github.com/oavdeev))
- EMR launcher [\#1061](https://github.com/feast-dev/feast/pull/1061) ([oavdeev](https://github.com/oavdeev))
- Add AWS managed kafka config to the example terraform [\#1058](https://github.com/feast-dev/feast/pull/1058) ([oavdeev](https://github.com/oavdeev))
- Feast SDK integration for historical feature retrieval using Spark [\#1054](https://github.com/feast-dev/feast/pull/1054) ([khorshuheng](https://github.com/khorshuheng))
- Update GetOnlineFeatures method in sdks [\#1052](https://github.com/feast-dev/feast/pull/1052) ([terryyylim](https://github.com/terryyylim))
- "Start Offline-to-online ingestion" method in Python SDK [\#1051](https://github.com/feast-dev/feast/pull/1051) ([pyalex](https://github.com/pyalex))
- Adding support for custom grpc dial options in Go SDK [\#1043](https://github.com/feast-dev/feast/pull/1043) ([ankurs](https://github.com/ankurs))
- CLI command to start/stop/list streaming ingestion job on emr [\#1040](https://github.com/feast-dev/feast/pull/1040) ([oavdeev](https://github.com/oavdeev))
- Update serving service to handle new redis encoding [\#1038](https://github.com/feast-dev/feast/pull/1038) ([terryyylim](https://github.com/terryyylim))
- terraform config for aws [\#1033](https://github.com/feast-dev/feast/pull/1033) ([oavdeev](https://github.com/oavdeev))
- Streaming Ingestion Pipeline with Spark [\#1027](https://github.com/feast-dev/feast/pull/1027) ([pyalex](https://github.com/pyalex))
- Run offline-to-online ingestion job on EMR [\#1026](https://github.com/feast-dev/feast/pull/1026) ([oavdeev](https://github.com/oavdeev))
- Add redis SSL support to the offline-to-online ingestion job [\#1025](https://github.com/feast-dev/feast/pull/1025) ([oavdeev](https://github.com/oavdeev))
- Dataproc and Standalone Cluster Spark Job launcher [\#1022](https://github.com/feast-dev/feast/pull/1022) ([khorshuheng](https://github.com/khorshuheng))
- Pyspark job for feature batch retrieval [\#1021](https://github.com/feast-dev/feast/pull/1021) ([khorshuheng](https://github.com/khorshuheng))
- Batch Ingestion Job rewritten on Spark [\#1020](https://github.com/feast-dev/feast/pull/1020) ([pyalex](https://github.com/pyalex))
- Add Feature Tables API to Core & Python SDK [\#1019](https://github.com/feast-dev/feast/pull/1019) ([mrzzy](https://github.com/mrzzy))
- Introduce Entity as higher-level concept [\#1014](https://github.com/feast-dev/feast/pull/1014) ([terryyylim](https://github.com/terryyylim))

**Fixed bugs:**

- Fix stencil client serialization issue [\#1147](https://github.com/feast-dev/feast/pull/1147) ([pyalex](https://github.com/pyalex))
- Deadletter path is being incorrectly joined [\#1144](https://github.com/feast-dev/feast/pull/1144) ([pyalex](https://github.com/pyalex))
- In Historical Retrieval \(SDK\) use project from client context [\#1138](https://github.com/feast-dev/feast/pull/1138) ([pyalex](https://github.com/pyalex))
- Pass project from context to get entities [\#1137](https://github.com/feast-dev/feast/pull/1137) ([pyalex](https://github.com/pyalex))
- JobService is in crashloop after installing helm chart [\#1135](https://github.com/feast-dev/feast/pull/1135) ([pyalex](https://github.com/pyalex))
- Fix env var names for jupyter terraform config [\#1085](https://github.com/feast-dev/feast/pull/1085) ([oavdeev](https://github.com/oavdeev))
- Fix java class name validation [\#1084](https://github.com/feast-dev/feast/pull/1084) ([oavdeev](https://github.com/oavdeev))
- Multiple tiny AWS related fixes [\#1083](https://github.com/feast-dev/feast/pull/1083) ([oavdeev](https://github.com/oavdeev))

**Merged pull requests:**

- Make created\_timestamp property optional in KafkaSource [\#1146](https://github.com/feast-dev/feast/pull/1146) ([pyalex](https://github.com/pyalex))
- In Streaming E2E Test filter kafka consumers by group id prefix [\#1145](https://github.com/feast-dev/feast/pull/1145) ([pyalex](https://github.com/pyalex))
- Limit concurrency on e2e test runs to 1 [\#1142](https://github.com/feast-dev/feast/pull/1142) ([oavdeev](https://github.com/oavdeev))
- Update prow trigger for AWS [\#1139](https://github.com/feast-dev/feast/pull/1139) ([oavdeev](https://github.com/oavdeev))
- e2e test fixes to make them work on AWS [\#1132](https://github.com/feast-dev/feast/pull/1132) ([oavdeev](https://github.com/oavdeev))
- Add feature table name & job id to deadletter destination [\#1143](https://github.com/feast-dev/feast/pull/1143) ([pyalex](https://github.com/pyalex))
- Drop hardcoded FEAST\_CORE\_URL env from JobService helm chart [\#1136](https://github.com/feast-dev/feast/pull/1136) ([pyalex](https://github.com/pyalex))
- Add Prow to AWS codebuild trigger [\#1133](https://github.com/feast-dev/feast/pull/1133) ([oavdeev](https://github.com/oavdeev))
- Optional IngestionJob parameters passed by Spark Launcher [\#1130](https://github.com/feast-dev/feast/pull/1130) ([pyalex](https://github.com/pyalex))
- Wait for job to be ready before cancelling [\#1126](https://github.com/feast-dev/feast/pull/1126) ([khorshuheng](https://github.com/khorshuheng))
- Ability to run e2e tests in non-default project [\#1125](https://github.com/feast-dev/feast/pull/1125) ([pyalex](https://github.com/pyalex))
- Ensure job is completed when ingesting to BQ [\#1123](https://github.com/feast-dev/feast/pull/1123) ([terryyylim](https://github.com/terryyylim))
- Add end-to-end Prow Job launcher for AWS tests [\#1118](https://github.com/feast-dev/feast/pull/1118) ([woop](https://github.com/woop))
- Add confluent kafka installation to minimal notebook [\#1116](https://github.com/feast-dev/feast/pull/1116) ([woop](https://github.com/woop))
- Scaffolding for integration tests [\#1113](https://github.com/feast-dev/feast/pull/1113) ([khorshuheng](https://github.com/khorshuheng))
- Add serving integration test for updated feature type [\#1112](https://github.com/feast-dev/feast/pull/1112) ([terryyylim](https://github.com/terryyylim))
- In Historical Retrieval from BQ join between source & entities is performed inside BQ [\#1110](https://github.com/feast-dev/feast/pull/1110) ([pyalex](https://github.com/pyalex))
- Make created timestamp column optional [\#1108](https://github.com/feast-dev/feast/pull/1108) ([khorshuheng](https://github.com/khorshuheng))
- Add validation when feature type is changed [\#1102](https://github.com/feast-dev/feast/pull/1102) ([terryyylim](https://github.com/terryyylim))
- E2E flow in prow is working [\#1101](https://github.com/feast-dev/feast/pull/1101) ([pyalex](https://github.com/pyalex))
- Return e2e back to prow runner [\#1100](https://github.com/feast-dev/feast/pull/1100) ([pyalex](https://github.com/pyalex))
- Add gh workflow for dockerhub [\#1098](https://github.com/feast-dev/feast/pull/1098) ([terryyylim](https://github.com/terryyylim))
- Make demo notebook work on AWS [\#1097](https://github.com/feast-dev/feast/pull/1097) ([oavdeev](https://github.com/oavdeev))
- Update Kubernetes setup for Feast 0.8 [\#1096](https://github.com/feast-dev/feast/pull/1096) ([khorshuheng](https://github.com/khorshuheng))
- Refactored end-to-end tests fully orchestrated by pytest [\#1094](https://github.com/feast-dev/feast/pull/1094) ([pyalex](https://github.com/pyalex))
- Disable statsd by default for spark [\#1089](https://github.com/feast-dev/feast/pull/1089) ([oavdeev-tt](https://github.com/oavdeev-tt))
- Restructure tutorial subfolder [\#1088](https://github.com/feast-dev/feast/pull/1088) ([terryyylim](https://github.com/terryyylim))
- Remove load test from github action [\#1087](https://github.com/feast-dev/feast/pull/1087) ([khorshuheng](https://github.com/khorshuheng))
- Add more explanations to the demo notebook, use local file system instead of GCS by default [\#1086](https://github.com/feast-dev/feast/pull/1086) ([khorshuheng](https://github.com/khorshuheng))
- Tutorial \(Full demo\) in Jupyter [\#1079](https://github.com/feast-dev/feast/pull/1079) ([pyalex](https://github.com/pyalex))
- Add unit test for historical retrieval with panda dataframe [\#1073](https://github.com/feast-dev/feast/pull/1073) ([khorshuheng](https://github.com/khorshuheng))
- Remove outdated tutorials [\#1069](https://github.com/feast-dev/feast/pull/1069) ([terryyylim](https://github.com/terryyylim))
- Add method to add feature to Feature table [\#1068](https://github.com/feast-dev/feast/pull/1068) ([terryyylim](https://github.com/terryyylim))
- Historical feature retrieval e2e test [\#1067](https://github.com/feast-dev/feast/pull/1067) ([khorshuheng](https://github.com/khorshuheng))
- Use RedisKeyV2 as key serializer and java murmur implementation in Redis Sink [\#1064](https://github.com/feast-dev/feast/pull/1064) ([pyalex](https://github.com/pyalex))
- Use existing staging client for dataproc staging [\#1063](https://github.com/feast-dev/feast/pull/1063) ([khorshuheng](https://github.com/khorshuheng))
- Cleanup CLI and Python dependencies [\#1062](https://github.com/feast-dev/feast/pull/1062) ([terryyylim](https://github.com/terryyylim))
- Refactor Spark Job launcher API [\#1060](https://github.com/feast-dev/feast/pull/1060) ([pyalex](https://github.com/pyalex))
- Create empty Job Service [\#1059](https://github.com/feast-dev/feast/pull/1059) ([tsotnet](https://github.com/tsotnet))
- Replace Data Source specific format options with DataFormat message [\#1049](https://github.com/feast-dev/feast/pull/1049) ([mrzzy](https://github.com/mrzzy))
- Add created\_timestamp\_column to DataSource. Rename timestamp\_column -\> event\_timestamp\_column [\#1048](https://github.com/feast-dev/feast/pull/1048) ([pyalex](https://github.com/pyalex))
- Cleanup e2e and docker-compose tests [\#1035](https://github.com/feast-dev/feast/pull/1035) ([terryyylim](https://github.com/terryyylim))
- Add svc account volume mount to prow jobs [\#1034](https://github.com/feast-dev/feast/pull/1034) ([terryyylim](https://github.com/terryyylim))
- Update prow config and makefile [\#1024](https://github.com/feast-dev/feast/pull/1024) ([terryyylim](https://github.com/terryyylim))
- Refactor Python SDK to remove v1 concepts [\#1023](https://github.com/feast-dev/feast/pull/1023) ([terryyylim](https://github.com/terryyylim))


## [v0.7.1](https://github.com/feast-dev/feast/tree/v0.7.1) (2020-10-07)
[Full Changelog](https://github.com/feast-dev/feast/compare/sdk/go/v0.7.0...v0.7.1)

**Fixed bugs:**

- Provide stable jobName in RowMetrics labels [\#1028](https://github.com/feast-dev/feast/pull/1028) ([pyalex](https://github.com/pyalex))

## [v0.7.0](https://github.com/feast-dev/feast/tree/v0.7.0) (2020-09-09)
[Full Changelog](https://github.com/feast-dev/feast/compare/sdk/go/v0.6.2...v0.7.0)

**Breaking changes:**

- Add request response logging via fluentd [\#961](https://github.com/feast-dev/feast/pull/961) ([terryyylim](https://github.com/terryyylim))
- Run JobCoontroller as separate application [\#951](https://github.com/feast-dev/feast/pull/951) ([pyalex](https://github.com/pyalex))
- Output Subject Claim as Identity in Logging interceptor [\#946](https://github.com/feast-dev/feast/pull/946) ([mrzzy](https://github.com/mrzzy))
- Use JobManager's backend as persistent storage and source of truth [\#903](https://github.com/feast-dev/feast/pull/903) ([pyalex](https://github.com/pyalex))
- Fix invalid characters for project, featureset, entity and features creation [\#976](https://github.com/feast-dev/feast/pull/976) ([terryyylim](https://github.com/terryyylim))

**Implemented enhancements:**

- Add redis key prefix as an option to Redis cluster [\#975](https://github.com/feast-dev/feast/pull/975) ([khorshuheng](https://github.com/khorshuheng))
- Authentication Support for Java & Go SDKs [\#971](https://github.com/feast-dev/feast/pull/971) ([mrzzy](https://github.com/mrzzy))
- Add configurable prefix to Consumer Group in IngestionJob's Kafka reader [\#969](https://github.com/feast-dev/feast/pull/969) ([terryyylim](https://github.com/terryyylim))
- Configurable kafka consumer in IngestionJob [\#959](https://github.com/feast-dev/feast/pull/959) ([pyalex](https://github.com/pyalex))
- Restart Ingestion Job on code version update [\#949](https://github.com/feast-dev/feast/pull/949) ([pyalex](https://github.com/pyalex))
- Add REST endpoints for Feast UI [\#878](https://github.com/feast-dev/feast/pull/878) ([SwampertX](https://github.com/SwampertX))
- Upgrade Feast dependencies [\#876](https://github.com/feast-dev/feast/pull/876) ([pyalex](https://github.com/pyalex))

**Fixed bugs:**

- Fix Java & Go SDK TLS support [\#986](https://github.com/feast-dev/feast/pull/986) ([mrzzy](https://github.com/mrzzy))
- Fix Python SDK setuptools not supporting tags required for Go SDK to be versioned. [\#983](https://github.com/feast-dev/feast/pull/983) ([mrzzy](https://github.com/mrzzy))
- Fix Python native types multiple entities online retrieval [\#977](https://github.com/feast-dev/feast/pull/977) ([terryyylim](https://github.com/terryyylim))
- Prevent historical retrieval from failing on dash in project / featureSet name [\#970](https://github.com/feast-dev/feast/pull/970) ([pyalex](https://github.com/pyalex))
- Fetch Job's labels from dataflow [\#968](https://github.com/feast-dev/feast/pull/968) ([pyalex](https://github.com/pyalex))
- Fetch Job's Created Datetime from Dataflow [\#966](https://github.com/feast-dev/feast/pull/966) ([pyalex](https://github.com/pyalex))
- Fix flaky tests [\#953](https://github.com/feast-dev/feast/pull/953) ([pyalex](https://github.com/pyalex))
- Prevent field duplications on schema merge in BigQuery sink [\#945](https://github.com/feast-dev/feast/pull/945) ([pyalex](https://github.com/pyalex))
- Fix Audit Message Logging Interceptor Race Condition [\#938](https://github.com/feast-dev/feast/pull/938) ([mrzzy](https://github.com/mrzzy))
- Bypass authentication for metric endpoints on Serving. [\#936](https://github.com/feast-dev/feast/pull/936) ([mrzzy](https://github.com/mrzzy))
- Fix grpc security variables name and missing exec qualifier in docker.dev [\#935](https://github.com/feast-dev/feast/pull/935) ([jmelinav](https://github.com/jmelinav))
- Remove extra line that duplicates statistics list [\#934](https://github.com/feast-dev/feast/pull/934) ([terryyylim](https://github.com/terryyylim))
- Fix empty array when retrieving stats data [\#930](https://github.com/feast-dev/feast/pull/930) ([terryyylim](https://github.com/terryyylim))
- Allow unauthenticated access when Authorization is disabled and to Health Probe [\#927](https://github.com/feast-dev/feast/pull/927) ([mrzzy](https://github.com/mrzzy))
- Impute default project if empty before authorization is called [\#926](https://github.com/feast-dev/feast/pull/926) ([jmelinav](https://github.com/jmelinav))
- Fix Github Actions CI load-test job failing due inability to install Feast Python SDK. [\#914](https://github.com/feast-dev/feast/pull/914) ([mrzzy](https://github.com/mrzzy))
- Fix Online Serving unable to retrieve feature data after Feature Set update. [\#908](https://github.com/feast-dev/feast/pull/908) ([mrzzy](https://github.com/mrzzy))
- Fix unit tests not running in feast.core package. [\#883](https://github.com/feast-dev/feast/pull/883) ([mrzzy](https://github.com/mrzzy))
- Exclude dependencies signatures from IngestionJob package [\#879](https://github.com/feast-dev/feast/pull/879) ([pyalex](https://github.com/pyalex))
- Prevent race condition in BQ sink jobId generation [\#877](https://github.com/feast-dev/feast/pull/877) ([pyalex](https://github.com/pyalex))
- Add IngestionId & EventTimestamp to FeatureRowBatch to calculate lag metric correctly [\#874](https://github.com/feast-dev/feast/pull/874) ([pyalex](https://github.com/pyalex))
- Fix typo for fluentd request response map key [\#989](https://github.com/feast-dev/feast/pull/989) ([terryyylim](https://github.com/terryyylim))
- Reduce polling interval for docker-compose test and fix flaky e2e test [\#982](https://github.com/feast-dev/feast/pull/982) ([terryyylim](https://github.com/terryyylim))
- Fix rate-limiting issue on github actions for master branch [\#974](https://github.com/feast-dev/feast/pull/974) ([terryyylim](https://github.com/terryyylim))
- Fix docker-compose test [\#973](https://github.com/feast-dev/feast/pull/973) ([terryyylim](https://github.com/terryyylim))
- Fix Helm chart requirements lock and version linting [\#925](https://github.com/feast-dev/feast/pull/925) ([woop](https://github.com/woop))
- Fix Github Actions failures due to possible rate limiting. [\#972](https://github.com/feast-dev/feast/pull/972) ([mrzzy](https://github.com/mrzzy))
- Fix docker image building for PR commits [\#907](https://github.com/feast-dev/feast/pull/907) ([woop](https://github.com/woop))
- Fix Github Actions versioned image push [\#994](https://github.com/feast-dev/feast/pull/994)([mrzzy](https://github.com/mrzzy))
- Fix Go SDK extra colon in metadata header for Authentication [\#1001](https://github.com/feast-dev/feast/pull/1001)([mrzzy](https://github.com/mrzzy))
- Fix lint version not pulling tags. [\#999](https://github.com/feast-dev/feast/pull/999)([mrzzy](https://github.com/mrzzy))
- Call fallback only when theres missing keys [\#1009](https://github.com/feast-dev/feast/pull/751) ([pyalex](https://github.com/pyalex))

**Merged pull requests:**

- Add cryptography to python ci-requirements [\#988](https://github.com/feast-dev/feast/pull/988) ([pyalex](https://github.com/pyalex))
- Allow maps in environment variables in helm charts [\#987](https://github.com/feast-dev/feast/pull/987) ([pyalex](https://github.com/pyalex))
- Speed up Github Actions Docker builds [\#980](https://github.com/feast-dev/feast/pull/980) ([mrzzy](https://github.com/mrzzy))
- Use setup.py develop instead of pip install -e [\#967](https://github.com/feast-dev/feast/pull/967) ([pyalex](https://github.com/pyalex))
- Peg black version [\#963](https://github.com/feast-dev/feast/pull/963) ([terryyylim](https://github.com/terryyylim))
- Remove FeatureRow compaction in BQ sink [\#960](https://github.com/feast-dev/feast/pull/960) ([pyalex](https://github.com/pyalex))
- Get job controller deployment for docker compose back [\#958](https://github.com/feast-dev/feast/pull/958) ([pyalex](https://github.com/pyalex))
- Revert job controller deployment for docker compose [\#957](https://github.com/feast-dev/feast/pull/957) ([woop](https://github.com/woop))
- JobCoordinator use public API to communicate with Core [\#943](https://github.com/feast-dev/feast/pull/943) ([pyalex](https://github.com/pyalex))
- Allow Logging Interceptor to be toggled by Message Logging Enabled Flag [\#940](https://github.com/feast-dev/feast/pull/940) ([mrzzy](https://github.com/mrzzy))
- Clean up Feast CI, docker compose, and notebooks [\#916](https://github.com/feast-dev/feast/pull/916) ([woop](https://github.com/woop))
- Allow use of Kubernetes for Github Actions [\#910](https://github.com/feast-dev/feast/pull/910) ([woop](https://github.com/woop))
- Wait for docker images to be ready for e2e dataflow test [\#909](https://github.com/feast-dev/feast/pull/909) ([woop](https://github.com/woop))
- Add docker image building to GitHub Actions and consolidate workflows [\#898](https://github.com/feast-dev/feast/pull/898) ([woop](https://github.com/woop))
- Add load test GitHub Action [\#897](https://github.com/feast-dev/feast/pull/897) ([woop](https://github.com/woop))
- Typo in feature sets example. [\#894](https://github.com/feast-dev/feast/pull/894) ([ashwinath](https://github.com/ashwinath))
- Add auth integration tests [\#892](https://github.com/feast-dev/feast/pull/892) ([woop](https://github.com/woop))
- Integration Test for Job Coordinator [\#886](https://github.com/feast-dev/feast/pull/886) ([pyalex](https://github.com/pyalex))
- BQ sink produces sample of successful inserts [\#875](https://github.com/feast-dev/feast/pull/875) ([pyalex](https://github.com/pyalex))
- Add Branch and RC Awareness to Version Lint & Fix Semver Regex [\#998](https://github.com/feast-dev/feast/pull/998) ([mrzzy](https://github.com/mrzzy))

## [v0.6.2](https://github.com/feast-dev/feast/tree/v0.6.2) (2020-08-02)                                                                                                                                                                                                         
[Full Changelog](https://github.com/feast-dev/feast/compare/v0.6.1...v0.6.2)

**Implemented enhancements:**

- Redis sink flushes only rows that have more recent eventTimestamp [\#913](https://github.com/feast-dev/feast/pull/913) ([pyalex](https://github.com/pyalex))
- Dataflow runner options: disk type & streaming engine [\#906](https://github.com/feast-dev/feast/pull/906) ([pyalex](https://github.com/pyalex))
- Add Structured Audit Logging [\#891](https://github.com/feast-dev/feast/pull/891) ([mrzzy](https://github.com/mrzzy))
- Add Authentication and Authorization for feast serving [\#865](https://github.com/feast-dev/feast/pull/865) ([jmelinav](https://github.com/jmelinav))
- Throw more informative exception when write\_triggering\_frequency\_seconds is missing [\#917](https://github.com/feast-dev/feast/pull/917) ([pyalex](https://github.com/pyalex))
- Add caching to authorization [\#884](https://github.com/feast-dev/feast/pull/884) ([jmelinav](https://github.com/jmelinav))
- Add Auth header [\#885](https://github.com/feast-dev/feast/pull/885) ([AnujaVane](https://github.com/AnujaVane))

**Fixed bugs:**

- Fix Online Serving unable to retrieve feature data after Feature Set update. [\#908](https://github.com/feast-dev/feast/pull/908) ([mrzzy](https://github.com/mrzzy))
- Fix Python SDK ingestion for featureset name that exist in multiple projects [\#868](https://github.com/feast-dev/feast/pull/868) ([terryyylim](https://github.com/terryyylim))
- Backport delay in Redis acknowledgement of spec [\#915](https://github.com/feast-dev/feast/pull/915) ([woop](https://github.com/woop))
- Allow unauthenticated access when Authorization is disabled and to Health Probe [\#927](https://github.com/feast-dev/feast/pull/927) ([mrzzy](https://github.com/mrzzy))

**Merged pull requests:**

- Upgrade Feast dependencies [\#876](https://github.com/feast-dev/feast/pull/876) ([pyalex](https://github.com/pyalex))

## [v0.6.1](https://github.com/feast-dev/feast/tree/v0.6.1) (2020-07-17)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.6.0...v0.6.1)

**Implemented enhancements:**

- Improve parallelization in Redis Sink [\#866](https://github.com/feast-dev/feast/pull/866) ([pyalex](https://github.com/pyalex))
- BQ sink produces sample of successful inserts [\#875](https://github.com/feast-dev/feast/pull/875) ([pyalex](https://github.com/pyalex))

**Fixed bugs:**

- Add IngestionId & EventTimestamp to FeatureRowBatch to calculate lag metric correctly [\#874](https://github.com/feast-dev/feast/pull/874) ([pyalex](https://github.com/pyalex))
- Prevent race condition in BQ sink jobId generation [\#877](https://github.com/feast-dev/feast/pull/877) ([pyalex](https://github.com/pyalex))

## [v0.6.0](https://github.com/feast-dev/feast/tree/v0.6.0) (2020-07-13)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.5.1...v0.6.0)

**Breaking changes:**

- Compute and write metrics for rows prior to store writes [\#763](https://github.com/feast-dev/feast/pull/763) ([zhilingc](https://github.com/zhilingc))

**Implemented enhancements:**

- Allow users compute statistics over retrieved batch datasets [\#799](https://github.com/feast-dev/feast/pull/799) ([zhilingc](https://github.com/zhilingc))
- Replace Keto Authorization with External HTTP Authorization [\#864](https://github.com/feast-dev/feast/pull/864) ([woop](https://github.com/woop))
- Add disk size as Dataflow Job Configuration [\#841](https://github.com/feast-dev/feast/pull/841) ([khorshuheng](https://github.com/khorshuheng))
- JobCoordinator may be turned off by configuration [\#829](https://github.com/feast-dev/feast/pull/829) ([pyalex](https://github.com/pyalex))
- Allow ingestion job grouping/consolidation to be configurable [\#825](https://github.com/feast-dev/feast/pull/825) ([pyalex](https://github.com/pyalex))
- Add subscriptions blacklist functionality [\#813](https://github.com/feast-dev/feast/pull/813) ([terryyylim](https://github.com/terryyylim))
- Add Common module [\#801](https://github.com/feast-dev/feast/pull/801) ([terryyylim](https://github.com/terryyylim))
- FeatureSets are delivered to Ingestion Job through Kafka [\#792](https://github.com/feast-dev/feast/pull/792) ([pyalex](https://github.com/pyalex))
- Add YAML export to Python SDK [\#782](https://github.com/feast-dev/feast/pull/782) ([woop](https://github.com/woop))
- Add support to Python SDK for staging files on Amazon S3 [\#769](https://github.com/feast-dev/feast/pull/769) ([jmelinav](https://github.com/jmelinav))
- Add support for version method in Feast SDK and Core [\#759](https://github.com/feast-dev/feast/pull/759) ([woop](https://github.com/woop))
- Upgrade ingestion to allow for in-flight updates to feature sets for sinks [\#757](https://github.com/feast-dev/feast/pull/757) ([pyalex](https://github.com/pyalex))
- Add Discovery API for listing features [\#797](https://github.com/feast-dev/feast/pull/797) ([terryyylim](https://github.com/terryyylim))
- Authentication and authorization support [\#793](https://github.com/feast-dev/feast/pull/793) ([dr3s](https://github.com/dr3s))
- Add API for listing feature sets using labels [\#785](https://github.com/feast-dev/feast/pull/785) ([terryyylim](https://github.com/terryyylim))

**Fixed bugs:**

- Bypass authentication for metric endpoints [\#862](https://github.com/feast-dev/feast/pull/862) ([woop](https://github.com/woop))
- Python SDK listing of ingestion job fails for featureset reference filter [\#861](https://github.com/feast-dev/feast/pull/861) ([terryyylim](https://github.com/terryyylim))
- Fix BigQuerySink successful output to produce only once [\#858](https://github.com/feast-dev/feast/pull/858) ([pyalex](https://github.com/pyalex))
- Re-applying of featuresets does not update label changes [\#857](https://github.com/feast-dev/feast/pull/857) ([terryyylim](https://github.com/terryyylim))
- BQ Sink is failing when Feature consists of only null values [\#853](https://github.com/feast-dev/feast/pull/853) ([pyalex](https://github.com/pyalex))
- Fix FeatureSetJobStatus removal [\#848](https://github.com/feast-dev/feast/pull/848) ([pyalex](https://github.com/pyalex))
- Fix: JobCoordinator tries to create duplicate FeatureSetJobStatuses [\#847](https://github.com/feast-dev/feast/pull/847) ([pyalex](https://github.com/pyalex))
- Replace IngestionJob when store was updated [\#846](https://github.com/feast-dev/feast/pull/846) ([pyalex](https://github.com/pyalex))
- Don't send unrecognized featureSets to deadletter in IngestionJob [\#845](https://github.com/feast-dev/feast/pull/845) ([pyalex](https://github.com/pyalex))
- Deallocate featureSet from job when source changed [\#844](https://github.com/feast-dev/feast/pull/844) ([pyalex](https://github.com/pyalex))
- Fix CPU count selection in Python SDK for non-Unix [\#839](https://github.com/feast-dev/feast/pull/839) ([pyalex](https://github.com/pyalex))
- Write metrics for store allocated rows only [\#830](https://github.com/feast-dev/feast/pull/830) ([zhilingc](https://github.com/zhilingc))
- Prevent reserved fields from being registered [\#819](https://github.com/feast-dev/feast/pull/819) ([terryyylim](https://github.com/terryyylim))
- Fix Optional\#get\(\) and string comparison bugs in JobService [\#804](https://github.com/feast-dev/feast/pull/804) ([ches](https://github.com/ches))
- Publish helm chart script should not modify the chart content [\#779](https://github.com/feast-dev/feast/pull/779) ([khorshuheng](https://github.com/khorshuheng))
- Fix pipeline options toArgs\(\) returning empty list [\#765](https://github.com/feast-dev/feast/pull/765) ([zhilingc](https://github.com/zhilingc))
- Remove usage of parallel stream for feature value map generation [\#751](https://github.com/feast-dev/feast/pull/751) ([khorshuheng](https://github.com/khorshuheng))

**Merged pull requests:**

- Remove Spring Boot from auth tests [\#859](https://github.com/feast-dev/feast/pull/859) ([woop](https://github.com/woop))
- Authentication and Authorization into feast-auth module. [\#856](https://github.com/feast-dev/feast/pull/856) ([jmelinav](https://github.com/jmelinav))
- Keep StoreProto inside JobStore to decouple JobCoordination from SpecService internals [\#852](https://github.com/feast-dev/feast/pull/852) ([pyalex](https://github.com/pyalex))
- Enable isort for Python SDK [\#843](https://github.com/feast-dev/feast/pull/843) ([woop](https://github.com/woop))
- Replace batch with historical for Python SDK retrieval [\#842](https://github.com/feast-dev/feast/pull/842) ([woop](https://github.com/woop))
- Upgrade pandas to 1.0.x [\#840](https://github.com/feast-dev/feast/pull/840) ([duongnt](https://github.com/duongnt))
- Ensure store subscriptions are migrated to allow exclusion schema [\#838](https://github.com/feast-dev/feast/pull/838) ([pyalex](https://github.com/pyalex))
- Remove project reference from feature set id in stats example notebook [\#836](https://github.com/feast-dev/feast/pull/836) ([zhilingc](https://github.com/zhilingc))
- Enable linting and formatting for e2e tests [\#832](https://github.com/feast-dev/feast/pull/832) ([woop](https://github.com/woop))
- IngestionJob is being gracefully replaced to minimize downtime [\#828](https://github.com/feast-dev/feast/pull/828) ([pyalex](https://github.com/pyalex))
- Add native types for Python SDK online retrieval [\#826](https://github.com/feast-dev/feast/pull/826) ([terryyylim](https://github.com/terryyylim))
- Send acknowledgment on Spec Update only after sinks are ready [\#822](https://github.com/feast-dev/feast/pull/822) ([pyalex](https://github.com/pyalex))
- Remove Duplicated Strip Projects Code from SDKs [\#820](https://github.com/feast-dev/feast/pull/820) ([mrzzy](https://github.com/mrzzy))
- Consolidate ingestion jobs to one job per source [\#817](https://github.com/feast-dev/feast/pull/817) ([pyalex](https://github.com/pyalex))
- Add missing key count metric [\#816](https://github.com/feast-dev/feast/pull/816) ([terryyylim](https://github.com/terryyylim))
- Create table in BigQuery if doesn't exists when new FeatureSetSpec arrived to IngestionJob [\#815](https://github.com/feast-dev/feast/pull/815) ([pyalex](https://github.com/pyalex))
- Refactor common module's feature string reference method [\#814](https://github.com/feast-dev/feast/pull/814) ([terryyylim](https://github.com/terryyylim))
- Fix typo in documentation [\#811](https://github.com/feast-dev/feast/pull/811) ([ravisuhag](https://github.com/ravisuhag))
- Database Schema migration for RELEASE 0.6 with Flyway [\#810](https://github.com/feast-dev/feast/pull/810) ([pyalex](https://github.com/pyalex))
- Update helm installation docs - Fix broken link [\#808](https://github.com/feast-dev/feast/pull/808) ([davidheryanto](https://github.com/davidheryanto))
- Add authentication support for end-to-end tests [\#807](https://github.com/feast-dev/feast/pull/807) ([jmelinav](https://github.com/jmelinav))
- Use latest instead of dev as the default image tag in helm charts [\#806](https://github.com/feast-dev/feast/pull/806) ([duongnt](https://github.com/duongnt))
- Build Feast Jupyter image and clean up examples [\#803](https://github.com/feast-dev/feast/pull/803) ([woop](https://github.com/woop))
- Move communication with IngestionJob to JobCoordinator [\#800](https://github.com/feast-dev/feast/pull/800) ([pyalex](https://github.com/pyalex))
- Compression of FeatureRows collection in memory [\#798](https://github.com/feast-dev/feast/pull/798) ([pyalex](https://github.com/pyalex))
- Add Kubernetes Pod labels to Core and Serving. [\#795](https://github.com/feast-dev/feast/pull/795) ([ashwinath](https://github.com/ashwinath))
- Add v0.3.8 changelog [\#788](https://github.com/feast-dev/feast/pull/788) ([ches](https://github.com/ches))
- Update change log due to release 0.5.1 [\#783](https://github.com/feast-dev/feast/pull/783) ([khorshuheng](https://github.com/khorshuheng))
- Refactor end-to-end tests to reduce duplication [\#758](https://github.com/feast-dev/feast/pull/758) ([woop](https://github.com/woop))
- Recompile golang protos to include new FeatureSetStatus [\#755](https://github.com/feast-dev/feast/pull/755) ([zhilingc](https://github.com/zhilingc))
- Merge Redis cluster connector with Redis connector [\#752](https://github.com/feast-dev/feast/pull/752) ([pyalex](https://github.com/pyalex))

## [0.5.1](https://github.com/feast-dev/feast/tree/0.5.1) (2020-06-06)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.5.0...v0.5.1)

**Implemented enhancements:**
- Add support for version method in Feast SDK and Core [\#759](https://github.com/feast-dev/feast/pull/759) ([woop](https://github.com/woop))
- Refactor runner configuration, add labels to dataflow options [\#718](https://github.com/feast-dev/feast/pull/718) ([zhilingc](https://github.com/zhilingc))

**Fixed bugs:**
- Fix pipeline options toArgs\(\) returning empty list [\#765](https://github.com/feast-dev/feast/pull/765) ([zhilingc](https://github.com/zhilingc))
- Fix project argument for feature set describe in CLI [\#731](https://github.com/feast-dev/feast/pull/731) ([terryyylim](https://github.com/terryyylim))
- Fix Go and Java SDK Regressions [\#729](https://github.com/feast-dev/feast/pull/729) ([mrzzy](https://github.com/mrzzy))
- Remove usage of parallel stream for feature value map generation [\#751](https://github.com/feast-dev/feast/pull/751) ([khorshuheng](https://github.com/khorshuheng))
- Restore Feast Java SDK and Ingestion compatibility with Java 8 runtimes [\#722](https://github.com/feast-dev/feast/pull/722) ([ches](https://github.com/ches))
- Python sdk bug fixes [\#723](https://github.com/feast-dev/feast/pull/723) ([zhilingc](https://github.com/zhilingc))

**Merged pull requests:**
- Increase Jaeger Tracing coverage [\#719](https://github.com/feast-dev/feast/pull/719) ([terryyylim](https://github.com/terryyylim))
- Recompile golang protos to include new FeatureSetStatus [\#755](https://github.com/feast-dev/feast/pull/755) ([zhilingc](https://github.com/zhilingc))
- Merge Redis cluster connector with Redis connector [\#752](https://github.com/feast-dev/feast/pull/752) ([pyalex](https://github.com/pyalex))
- Remove unused Hibernate dep from Serving [\#721](https://github.com/feast-dev/feast/pull/721) ([ches](https://github.com/ches))

## [v0.5.0](https://github.com/feast-dev/feast/tree/v0.5.0) (2020-05-19)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.7...v0.5.0)

**Breaking changes:**

- Add .proto to packages of Protobuf generated Java classes [\#700](https://github.com/feast-dev/feast/pull/700) ([woop](https://github.com/woop))
- Add support for feature set updates and remove versions [\#676](https://github.com/feast-dev/feast/pull/676) ([zhilingc](https://github.com/zhilingc))
- Feast configuration files refactored [\#611](https://github.com/feast-dev/feast/pull/611) ([woop](https://github.com/woop))

See [Feast 0.5 Release Issue](https://github.com/feast-dev/feast/issues/527) for more details.

**Implemented enhancements:**

- Add general storage API and refactor existing store implementations [\#567](https://github.com/feast-dev/feast/pull/567) ([zhilingc](https://github.com/zhilingc))
- Add support for feature set updates and remove versions [\#676](https://github.com/feast-dev/feast/pull/676) ([zhilingc](https://github.com/zhilingc))
- Add unique ingestion id for all batch ingestions [\#656](https://github.com/feast-dev/feast/pull/656) ([zhilingc](https://github.com/zhilingc))
- Add storage interfaces [\#529](https://github.com/feast-dev/feast/pull/529) ([zhilingc](https://github.com/zhilingc))
- Add BigQuery storage implementation [\#546](https://github.com/feast-dev/feast/pull/546) ([zhilingc](https://github.com/zhilingc))
- Add Redis storage implementation [\#547](https://github.com/feast-dev/feast/pull/547) ([zhilingc](https://github.com/zhilingc))
- Add Support for Redis Cluster [\#502](https://github.com/feast-dev/feast/pull/502) ([lavkesh](https://github.com/lavkesh))
- Add Ingestion Job management API for Feast Core [\#548](https://github.com/feast-dev/feast/pull/548) ([mrzzy](https://github.com/mrzzy))
- Add feature and feature set labels for metadata [\#536](https://github.com/feast-dev/feast/pull/536) ([imjuanleonard](https://github.com/imjuanleonard))
- Update Python SDK so FeatureSet can import Schema from Tensorflow metadata [\#450](https://github.com/feast-dev/feast/pull/450) ([davidheryanto](https://github.com/davidheryanto))

**Fixed bugs:**

- Add feature set status JOB\_STARTING to denote feature sets waiting for job to get to RUNNING state [\#714](https://github.com/feast-dev/feast/pull/714) ([zhilingc](https://github.com/zhilingc))
- Remove feature set status check for job update requirement [\#708](https://github.com/feast-dev/feast/pull/708) ([khorshuheng](https://github.com/khorshuheng))
- Fix Feast Core docker image [\#703](https://github.com/feast-dev/feast/pull/703) ([khorshuheng](https://github.com/khorshuheng))
- Include server port config on the generated application.yml [\#696](https://github.com/feast-dev/feast/pull/696) ([khorshuheng](https://github.com/khorshuheng))
- Fix typo in all types parquet yml file \(e2e test\) [\#683](https://github.com/feast-dev/feast/pull/683) ([khorshuheng](https://github.com/khorshuheng))
- Add grpc health probe implementation to core [\#680](https://github.com/feast-dev/feast/pull/680) ([zhilingc](https://github.com/zhilingc))
- Ensure that generated python code are considered as module [\#679](https://github.com/feast-dev/feast/pull/679) ([khorshuheng](https://github.com/khorshuheng))
- Fix DataflowJobManager to update existing job instance instead of creating new one [\#678](https://github.com/feast-dev/feast/pull/678) ([zhilingc](https://github.com/zhilingc))
- Fix config validation for feast.jobs.metrics.host [\#662](https://github.com/feast-dev/feast/pull/662) ([davidheryanto](https://github.com/davidheryanto))
- Docker compose bug fix  [\#661](https://github.com/feast-dev/feast/pull/661) ([woop](https://github.com/woop))
- Swap join columns [\#647](https://github.com/feast-dev/feast/pull/647) ([zhilingc](https://github.com/zhilingc))
- Fix Feast Serving not registering its store in Feast Core [\#641](https://github.com/feast-dev/feast/pull/641) ([mrzzy](https://github.com/mrzzy))
- Kafka producer should raise an exception when it fails to connect to broker [\#636](https://github.com/feast-dev/feast/pull/636) ([junhui096](https://github.com/junhui096))

**Merged pull requests:**

- Change organization from gojek to feast-dev [\#712](https://github.com/feast-dev/feast/pull/712) ([woop](https://github.com/woop))
- Extract feature set update tests so CI doesn't run it [\#709](https://github.com/feast-dev/feast/pull/709) ([zhilingc](https://github.com/zhilingc))
- Ensure that batch retrieval tests clean up after themselves [\#704](https://github.com/feast-dev/feast/pull/704) ([zhilingc](https://github.com/zhilingc))
- Apply default project to rows without project during ingestion [\#701](https://github.com/feast-dev/feast/pull/701) ([zhilingc](https://github.com/zhilingc))
- Update tests to correct compute region [\#699](https://github.com/feast-dev/feast/pull/699) ([terryyylim](https://github.com/terryyylim))
- Make Projects optional & Update Feature References [\#693](https://github.com/feast-dev/feast/pull/693) ([mrzzy](https://github.com/mrzzy))
- Add Java code coverage reporting [\#686](https://github.com/feast-dev/feast/pull/686) ([ches](https://github.com/ches))
- Update e2e tests to allow non-SNAPSHOT testing [\#672](https://github.com/feast-dev/feast/pull/672) ([woop](https://github.com/woop))
- Move TFDV stats to higher-numbered protobuf fields [\#669](https://github.com/feast-dev/feast/pull/669) ([ches](https://github.com/ches))
- Clean up Docker Compose and add test [\#668](https://github.com/feast-dev/feast/pull/668) ([woop](https://github.com/woop))
- Enable Prow e2e tests by default [\#666](https://github.com/feast-dev/feast/pull/666) ([woop](https://github.com/woop))
- Add label checking to Prow [\#665](https://github.com/feast-dev/feast/pull/665) ([woop](https://github.com/woop))
- Upgrade Github Checkout action to v2 [\#660](https://github.com/feast-dev/feast/pull/660) ([khorshuheng](https://github.com/khorshuheng))
- Fix Redis cluster e2e [\#659](https://github.com/feast-dev/feast/pull/659) ([terryyylim](https://github.com/terryyylim))
- Split Field model into distinct Feature and Entity objects [\#655](https://github.com/feast-dev/feast/pull/655) ([zhilingc](https://github.com/zhilingc))
- Use Runner enum type instead of string for Job model [\#651](https://github.com/feast-dev/feast/pull/651) ([ches](https://github.com/ches))
- JobUpdateTask cleanups [\#650](https://github.com/feast-dev/feast/pull/650) ([ches](https://github.com/ches))
- Update approvers list [\#648](https://github.com/feast-dev/feast/pull/648) ([khorshuheng](https://github.com/khorshuheng))
- Update end-to-end test config [\#645](https://github.com/feast-dev/feast/pull/645) ([zhilingc](https://github.com/zhilingc))
- Fix bigquery config for serving store [\#644](https://github.com/feast-dev/feast/pull/644) ([zhilingc](https://github.com/zhilingc))
- Fix Dataflow translator bug [\#643](https://github.com/feast-dev/feast/pull/643) ([zhilingc](https://github.com/zhilingc))
- Fix subscription config and doctests [\#634](https://github.com/feast-dev/feast/pull/634) ([woop](https://github.com/woop))
- Correct links to why-feast and concepts doc site [\#627](https://github.com/feast-dev/feast/pull/627) ([anderseriksson](https://github.com/anderseriksson))
- Make error on retrieval of nonexistent feature humanly readable [\#625](https://github.com/feast-dev/feast/pull/625) ([mrzzy](https://github.com/mrzzy))
- Generate golang code for non-serving protos [\#618](https://github.com/feast-dev/feast/pull/618) ([zhilingc](https://github.com/zhilingc))
- Regenerate golang code, fix proto comparisons [\#616](https://github.com/feast-dev/feast/pull/616) ([zhilingc](https://github.com/zhilingc))
- Fix doc building [\#603](https://github.com/feast-dev/feast/pull/603) ([woop](https://github.com/woop))
- Pin Jupyter Notebook version [\#597](https://github.com/feast-dev/feast/pull/597) ([imjuanleonard](https://github.com/imjuanleonard))
- Create project if not exists on applyFeatureSet [\#596](https://github.com/feast-dev/feast/pull/596) ([Joostrothweiler](https://github.com/Joostrothweiler))
- Apply a fixed window before writing row metrics [\#590](https://github.com/feast-dev/feast/pull/590) ([davidheryanto](https://github.com/davidheryanto))
- Allow tests to run on non-master branches [\#588](https://github.com/feast-dev/feast/pull/588) ([woop](https://github.com/woop))

## [v0.4.7](https://github.com/feast-dev/feast/tree/v0.4.7) (2020-03-17)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.6...v0.4.7)

**Merged pull requests:**
- Add log4j-web jar to core and serving. [\#498](https://github.com/feast-dev/feast/pull/498) ([Yanson](https://github.com/Yanson))
- Clear all the futures when sync is called. [\#501](https://github.com/feast-dev/feast/pull/501) ([lavkesh](https://github.com/lavkesh))
- Encode feature row before storing in Redis [\#530](https://github.com/feast-dev/feast/pull/530) ([khorshuheng](https://github.com/khorshuheng))
- Remove transaction when listing projects [\#522](https://github.com/feast-dev/feast/pull/522) ([davidheryanto](https://github.com/davidheryanto))
- Remove unused ingestion deps [\#520](https://github.com/feast-dev/feast/pull/520) ([ches](https://github.com/ches))
- Parameterize end to end test scripts. [\#433](https://github.com/feast-dev/feast/pull/433) ([Yanson](https://github.com/Yanson))
- Replacing Jedis With Lettuce in ingestion and serving [\#485](https://github.com/feast-dev/feast/pull/485) ([lavkesh](https://github.com/lavkesh))

## [v0.4.6](https://github.com/feast-dev/feast/tree/v0.4.6) (2020-02-26)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.5...v0.4.6)

**Merged pull requests:**
- Rename metric name for request latency in feast serving [\#488](https://github.com/feast-dev/feast/pull/488) ([davidheryanto](https://github.com/davidheryanto))
- Allow use of secure gRPC in Feast Python client [\#459](https://github.com/feast-dev/feast/pull/459) ([Yanson](https://github.com/Yanson))
- Extend WriteMetricsTransform in Ingestion to write feature value stats to StatsD [\#486](https://github.com/feast-dev/feast/pull/486) ([davidheryanto](https://github.com/davidheryanto))
- Remove transaction from Ingestion [\#480](https://github.com/feast-dev/feast/pull/480) ([imjuanleonard](https://github.com/imjuanleonard))
- Fix fastavro version used in Feast to avoid Timestamp delta error [\#490](https://github.com/feast-dev/feast/pull/490) ([davidheryanto](https://github.com/davidheryanto))
- Fail Spotless formatting check before tests execute [\#487](https://github.com/feast-dev/feast/pull/487) ([ches](https://github.com/ches))
- Reduce refresh rate of specification refresh in Serving to 10 seconds [\#481](https://github.com/feast-dev/feast/pull/481) ([woop](https://github.com/woop))

## [v0.4.5](https://github.com/feast-dev/feast/tree/v0.4.5) (2020-02-14)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.4...v0.4.5)

**Merged pull requests:**
- Use bzip2 compressed feature set json as pipeline option [\#466](https://github.com/feast-dev/feast/pull/466) ([khorshuheng](https://github.com/khorshuheng))
- Make redis key creation more determinisitic [\#471](https://github.com/feast-dev/feast/pull/471) ([zhilingc](https://github.com/zhilingc))
- Helm Chart Upgrades [\#458](https://github.com/feast-dev/feast/pull/458) ([Yanson](https://github.com/Yanson))
- Exclude version from grouping [\#441](https://github.com/feast-dev/feast/pull/441) ([khorshuheng](https://github.com/khorshuheng))
- Use concrete class for AvroCoder compatibility [\#465](https://github.com/feast-dev/feast/pull/465) ([zhilingc](https://github.com/zhilingc))
- Fix typo in split string length check [\#464](https://github.com/feast-dev/feast/pull/464) ([zhilingc](https://github.com/zhilingc))
- Update README.md and remove versions from Helm Charts [\#457](https://github.com/feast-dev/feast/pull/457) ([woop](https://github.com/woop))
- Deduplicate example notebooks [\#456](https://github.com/feast-dev/feast/pull/456) ([woop](https://github.com/woop))
- Allow users not to set max age for batch retrieval [\#446](https://github.com/feast-dev/feast/pull/446) ([zhilingc](https://github.com/zhilingc))

## [v0.4.4](https://github.com/feast-dev/feast/tree/v0.4.4) (2020-01-28)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.3...v0.4.4)

**Merged pull requests:**

- Change RedisBackedJobService to use a connection pool [\#439](https://github.com/feast-dev/feast/pull/439) ([zhilingc](https://github.com/zhilingc))
- Update GKE installation and chart values to work with 0.4.3 [\#434](https://github.com/feast-dev/feast/pull/434) ([lgvital](https://github.com/lgvital))
- Remove "resource" concept and the need to specify a kind in feature sets [\#432](https://github.com/feast-dev/feast/pull/432) ([woop](https://github.com/woop))
- Add retry options to BigQuery [\#431](https://github.com/feast-dev/feast/pull/431) ([Yanson](https://github.com/Yanson))
- Fix logging [\#430](https://github.com/feast-dev/feast/pull/430) ([Yanson](https://github.com/Yanson))
- Add documentation for bigquery batch retrieval [\#428](https://github.com/feast-dev/feast/pull/428) ([zhilingc](https://github.com/zhilingc))
- Publish datatypes/java along with sdk/java [\#426](https://github.com/feast-dev/feast/pull/426) ([ches](https://github.com/ches))
- Update basic Feast example to Feast 0.4 [\#424](https://github.com/feast-dev/feast/pull/424) ([woop](https://github.com/woop))
- Introduce datatypes/java module for proto generation [\#391](https://github.com/feast-dev/feast/pull/391) ([ches](https://github.com/ches))

## [v0.4.3](https://github.com/feast-dev/feast/tree/v0.4.3) (2020-01-08)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.2...v0.4.3)

**Fixed bugs:**

- Bugfix for redis ingestion retries throwing NullPointerException on remote runners [\#417](https://github.com/feast-dev/feast/pull/417) ([khorshuheng](https://github.com/khorshuheng))

## [v0.4.2](https://github.com/feast-dev/feast/tree/v0.4.2) (2020-01-07)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.1...v0.4.2)

**Fixed bugs:**

- Missing argument in error string in ValidateFeatureRowDoFn [\#401](https://github.com/feast-dev/feast/issues/401)

**Merged pull requests:**

- Define maven revision property when packaging jars in Dockerfile so the images are built successfully [\#410](https://github.com/feast-dev/feast/pull/410) ([davidheryanto](https://github.com/davidheryanto))
- Deduplicate rows in subquery [\#409](https://github.com/feast-dev/feast/pull/409) ([zhilingc](https://github.com/zhilingc))
- Filter out extra fields, deduplicate fields in ingestion [\#404](https://github.com/feast-dev/feast/pull/404) ([zhilingc](https://github.com/zhilingc))
- Automatic documentation generation for gRPC API [\#403](https://github.com/feast-dev/feast/pull/403) ([woop](https://github.com/woop))
- Update feast core default values to include hibernate merge strategy [\#400](https://github.com/feast-dev/feast/pull/400) ([zhilingc](https://github.com/zhilingc))
- Move cli into feast package [\#398](https://github.com/feast-dev/feast/pull/398) ([zhilingc](https://github.com/zhilingc))
- Use Nexus staging plugin for deployment [\#394](https://github.com/feast-dev/feast/pull/394) ([khorshuheng](https://github.com/khorshuheng))
- Handle retry for redis io flow [\#274](https://github.com/feast-dev/feast/pull/274) ([khorshuheng](https://github.com/khorshuheng))

## [v0.4.1](https://github.com/feast-dev/feast/tree/v0.4.1) (2019-12-30)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.0...v0.4.1)

**Merged pull requests:**

- Add project-related commands to CLI [\#397](https://github.com/feast-dev/feast/pull/397) ([zhilingc](https://github.com/zhilingc))

## [v0.4.0](https://github.com/feast-dev/feast/tree/v0.4.0) (2019-12-28)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.5...v0.4.0)

**Implemented enhancements:**

- Edit description in feature specification to also reflect in BigQuery schema description. [\#239](https://github.com/feast-dev/feast/issues/239)
- Allow for disabling of metrics pushing [\#57](https://github.com/feast-dev/feast/issues/57)

**Merged pull requests:**

- Java SDK release script [\#406](https://github.com/feast-dev/feast/pull/406) ([davidheryanto](https://github.com/davidheryanto))
- Use fixed 'dev' revision for test-e2e-batch [\#395](https://github.com/feast-dev/feast/pull/395) ([davidheryanto](https://github.com/davidheryanto))
- Project Namespacing [\#393](https://github.com/feast-dev/feast/pull/393) ([woop](https://github.com/woop))
- \<docs\>\(concepts\): change data types to upper case because lower case … [\#389](https://github.com/feast-dev/feast/pull/389) ([david30907d](https://github.com/david30907d))
- Remove alpha v1 from java package name [\#387](https://github.com/feast-dev/feast/pull/387) ([khorshuheng](https://github.com/khorshuheng))
- Minor bug fixes for Python SDK [\#383](https://github.com/feast-dev/feast/pull/383) ([voonhous](https://github.com/voonhous))
- Allow user to override job options [\#377](https://github.com/feast-dev/feast/pull/377) ([khorshuheng](https://github.com/khorshuheng))
- Add documentation to default values.yaml in Feast chart [\#376](https://github.com/feast-dev/feast/pull/376) ([davidheryanto](https://github.com/davidheryanto))
- Add support for file paths for providing entity rows during batch retrieval  [\#375](https://github.com/feast-dev/feast/pull/375) ([voonhous](https://github.com/voonhous))
- Update sync helm chart script to ensure requirements.lock in in sync with requirements.yaml [\#373](https://github.com/feast-dev/feast/pull/373) ([davidheryanto](https://github.com/davidheryanto))
- Catch errors thrown by BQ during entity table loading [\#371](https://github.com/feast-dev/feast/pull/371) ([zhilingc](https://github.com/zhilingc))
- Async job management [\#361](https://github.com/feast-dev/feast/pull/361) ([zhilingc](https://github.com/zhilingc))
- Infer schema of PyArrow table directly [\#355](https://github.com/feast-dev/feast/pull/355) ([voonhous](https://github.com/voonhous))
- Add readiness checks for Feast services in end to end test [\#337](https://github.com/feast-dev/feast/pull/337) ([davidheryanto](https://github.com/davidheryanto))
- Create CHANGELOG.md [\#321](https://github.com/feast-dev/feast/pull/321) ([woop](https://github.com/woop))

## [v0.3.8](https://github.com/feast-dev/feast/tree/v0.3.8) (2020-06-10)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.7...v0.3.8)

**Implemented enhancements:**

- v0.3 backport: Add feature and feature set labels [\#737](https://github.com/feast-dev/feast/pull/737) ([ches](https://github.com/ches))

**Merged pull requests:**

- v0.3 backport: Add Java coverage reporting [\#734](https://github.com/feast-dev/feast/pull/734) ([ches](https://github.com/ches))

## [v0.3.7](https://github.com/feast-dev/feast/tree/v0.3.7) (2020-05-01)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.6...v0.3.7)

**Merged pull requests:**

- Moved end-to-end test scripts from .prow to infra [\#657](https://github.com/feast-dev/feast/pull/657) ([khorshuheng](https://github.com/khorshuheng))
- Backported \#566 & \#647 to v0.3 [\#654](https://github.com/feast-dev/feast/pull/654) ([ches](https://github.com/ches))

## [v0.3.6](https://github.com/feast-dev/feast/tree/v0.3.6) (2020-01-03)

**Merged pull requests:**

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.5...v0.3.6)

- Add support for file paths for providing entity rows during batch retrieval [\#375](https://github.com/feast-dev/feast/pull/375) ([voonhous](https://github.com/voonhous))

## [v0.3.5](https://github.com/feast-dev/feast/tree/v0.3.5) (2019-12-26)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.4...v0.3.5)

**Merged pull requests:**

- Always set destination table in BigQuery query config in Feast Batch Serving so it can handle large results [\#392](https://github.com/feast-dev/feast/pull/392) ([davidheryanto](https://github.com/davidheryanto))

## [v0.3.4](https://github.com/feast-dev/feast/tree/v0.3.4) (2019-12-23)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.3...v0.3.4)

**Merged pull requests:**

- Make redis key creation more determinisitic [\#380](https://github.com/feast-dev/feast/pull/380) ([zhilingc](https://github.com/zhilingc))

## [v0.3.3](https://github.com/feast-dev/feast/tree/v0.3.3) (2019-12-18)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.2...v0.3.3)

**Implemented enhancements:**

- Added Docker Compose for Feast [\#272](https://github.com/feast-dev/feast/issues/272)
- Added ability to check import job status and cancel job through Python SDK [\#194](https://github.com/feast-dev/feast/issues/194)
- Added basic customer transactions example [\#354](https://github.com/feast-dev/feast/pull/354) ([woop](https://github.com/woop))

**Merged pull requests:**

- Added Prow jobs to automate the release of Docker images and Python SDK [\#369](https://github.com/feast-dev/feast/pull/369) ([davidheryanto](https://github.com/davidheryanto))
- Fixed installation link in README.md [\#368](https://github.com/feast-dev/feast/pull/368) ([Jeffwan](https://github.com/Jeffwan))
- Fixed Java SDK tests not actually running \(missing dependencies\) [\#366](https://github.com/feast-dev/feast/pull/366) ([woop](https://github.com/woop))
- Added more batch retrieval tests [\#357](https://github.com/feast-dev/feast/pull/357) ([zhilingc](https://github.com/zhilingc))
- Python SDK and Feast Core Bug Fixes [\#353](https://github.com/feast-dev/feast/pull/353) ([woop](https://github.com/woop))
- Updated buildFeatureSets method in Golang SDK [\#351](https://github.com/feast-dev/feast/pull/351) ([davidheryanto](https://github.com/davidheryanto))
- Python SDK cleanup [\#348](https://github.com/feast-dev/feast/pull/348) ([woop](https://github.com/woop))
- Broke up queries for point in time correctness joins [\#347](https://github.com/feast-dev/feast/pull/347) ([zhilingc](https://github.com/zhilingc))
- Exports gRPC call metrics and Feast resource metrics in Core [\#345](https://github.com/feast-dev/feast/pull/345) ([davidheryanto](https://github.com/davidheryanto))
- Fixed broken Google Group link on Community page [\#343](https://github.com/feast-dev/feast/pull/343) ([ches](https://github.com/ches))
- Ensured ImportJobTest is not flaky by checking WriteToStore metric and requesting adequate resources for testing [\#332](https://github.com/feast-dev/feast/pull/332) ([davidheryanto](https://github.com/davidheryanto))
- Added docker-compose file with Jupyter notebook [\#328](https://github.com/feast-dev/feast/pull/328) ([khorshuheng](https://github.com/khorshuheng))
- Added minimal implementation of ingesting Parquet and CSV files [\#327](https://github.com/feast-dev/feast/pull/327) ([voonhous](https://github.com/voonhous))

## [v0.3.2](https://github.com/feast-dev/feast/tree/v0.3.2) (2019-11-29)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.1...v0.3.2)

**Merged pull requests:**

- Fixed incorrect BigQuery schema creation from FeatureSetSpec [\#340](https://github.com/feast-dev/feast/pull/340) ([davidheryanto](https://github.com/davidheryanto))
- Filtered out feature sets that dont share the same source [\#339](https://github.com/feast-dev/feast/pull/339) ([zhilingc](https://github.com/zhilingc))
- Changed latency calculation method to not use Timer [\#338](https://github.com/feast-dev/feast/pull/338) ([zhilingc](https://github.com/zhilingc))
- Moved Prometheus annotations to pod template for serving [\#336](https://github.com/feast-dev/feast/pull/336) ([zhilingc](https://github.com/zhilingc))
- Removed metrics windowing, cleaned up step names for metrics writing [\#334](https://github.com/feast-dev/feast/pull/334) ([zhilingc](https://github.com/zhilingc))
- Set BigQuery table time partition inside get table function [\#333](https://github.com/feast-dev/feast/pull/333) ([zhilingc](https://github.com/zhilingc))
- Added unit test in Redis to return values with no max age set [\#329](https://github.com/feast-dev/feast/pull/329) ([smadarasmi](https://github.com/smadarasmi))
- Consolidated jobs into single steps instead of branching out [\#326](https://github.com/feast-dev/feast/pull/326) ([zhilingc](https://github.com/zhilingc))
- Pinned Python SDK to minor versions for dependencies [\#322](https://github.com/feast-dev/feast/pull/322) ([woop](https://github.com/woop))
- Added Auto format to Google style with Spotless [\#317](https://github.com/feast-dev/feast/pull/317) ([ches](https://github.com/ches))

## [v0.3.1](https://github.com/feast-dev/feast/tree/v0.3.1) (2019-11-25)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.0...v0.3.1)

**Merged pull requests:**

- Added Prometheus metrics to serving [\#316](https://github.com/feast-dev/feast/pull/316) ([zhilingc](https://github.com/zhilingc))
- Changed default job metrics sink to Statsd [\#315](https://github.com/feast-dev/feast/pull/315) ([zhilingc](https://github.com/zhilingc))
- Fixed module import error in Feast CLI [\#314](https://github.com/feast-dev/feast/pull/314) ([davidheryanto](https://github.com/davidheryanto))

## [v0.3.0](https://github.com/feast-dev/feast/tree/v0.3.0) (2019-11-19)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.1.8...v0.3.0)

**Summary:**

* Introduced "Feature Sets" as a concept with a new [Feast Core API](https://github.com/feast-dev/feast/blob/v0.3.0/protos/feast/core/CoreService.proto), [Feast Serving API](https://github.com/feast-dev/feast/blob/v0.3.0/protos/feast/serving/ServingService.proto)
* Upgraded [Python SDK](https://github.com/feast-dev/feast/tree/v0.3.0/sdk/python) to support new Feast API. Allows for management of Feast as a library or through the command line.
* Implemented a [Golang SDK](https://github.com/feast-dev/feast/tree/v0.3.0/sdk/go) and [Java SDK](https://github.com/feast-dev/feast/tree/v0.3.0/sdk/java) to support the new Feast Core and Feast Serving APIs.
* Added support for multi-feature set retrieval and joins.
* Added point-in-time correct retrieval for both batch and online serving.
* Added support for an external source in Kafka.
* Added job management to Feast Core to manage ingestion/population jobs to remote Feast deployments
* Added metric support through Prometheus

**Merged pull requests:**

- Regenerate go protos [\#313](https://github.com/feast-dev/feast/pull/313) ([zhilingc](https://github.com/zhilingc))
- Bump chart version to 0.3.0 [\#311](https://github.com/feast-dev/feast/pull/311) ([zhilingc](https://github.com/zhilingc))
- Refactored Core API: ListFeatureSets, ListStore, and GetFeatureSet [\#309](https://github.com/feast-dev/feast/pull/309) ([woop](https://github.com/woop))
- Use Maven's --also-make by default [\#308](https://github.com/feast-dev/feast/pull/308) ([ches](https://github.com/ches))
- Python SDK Ingestion and schema inference updates [\#307](https://github.com/feast-dev/feast/pull/307) ([woop](https://github.com/woop))
- Batch ingestion fix [\#299](https://github.com/feast-dev/feast/pull/299) ([zhilingc](https://github.com/zhilingc))
- Update values-demo.yaml to make Minikube installation simpler [\#298](https://github.com/feast-dev/feast/pull/298) ([woop](https://github.com/woop))
- Fix bug in core not setting default Kafka source [\#297](https://github.com/feast-dev/feast/pull/297) ([woop](https://github.com/woop))
- Replace Prometheus logging in ingestion with StatsD logging [\#293](https://github.com/feast-dev/feast/pull/293) ([woop](https://github.com/woop))
- Feast Core: Stage files manually when launching Dataflow jobs [\#291](https://github.com/feast-dev/feast/pull/291) ([davidheryanto](https://github.com/davidheryanto))
- Database tweaks [\#290](https://github.com/feast-dev/feast/pull/290) ([smadarasmi](https://github.com/smadarasmi))
- Feast Helm charts and build script [\#289](https://github.com/feast-dev/feast/pull/289) ([davidheryanto](https://github.com/davidheryanto))
- Fix max\_age changes not updating specs and add TQDM silencing flag [\#292](https://github.com/feast-dev/feast/pull/292) ([woop](https://github.com/woop))
- Ingestion fixes [\#286](https://github.com/feast-dev/feast/pull/286) ([zhilingc](https://github.com/zhilingc))
- Consolidate jobs [\#279](https://github.com/feast-dev/feast/pull/279) ([zhilingc](https://github.com/zhilingc))
- Import Spring Boot's dependency BOM, fix spring-boot:run at parent project level [\#276](https://github.com/feast-dev/feast/pull/276) ([ches](https://github.com/ches))
- Feast 0.3 Continuous Integration \(CI\) Update  [\#271](https://github.com/feast-dev/feast/pull/271) ([davidheryanto](https://github.com/davidheryanto))
- Add batch feature retrieval to Python SDK [\#268](https://github.com/feast-dev/feast/pull/268) ([woop](https://github.com/woop))
- Set Maven build requirements and some project POM metadata [\#267](https://github.com/feast-dev/feast/pull/267) ([ches](https://github.com/ches))
- Python SDK enhancements [\#264](https://github.com/feast-dev/feast/pull/264) ([woop](https://github.com/woop))
- Use a symlink for Java SDK's protos [\#263](https://github.com/feast-dev/feast/pull/263) ([ches](https://github.com/ches))
- Clean up the Maven build [\#262](https://github.com/feast-dev/feast/pull/262) ([ches](https://github.com/ches))
- Add golang SDK [\#261](https://github.com/feast-dev/feast/pull/261) ([zhilingc](https://github.com/zhilingc))
- Move storage configuration to serving [\#254](https://github.com/feast-dev/feast/pull/254) ([zhilingc](https://github.com/zhilingc))
- Serving API changes for 0.3 [\#253](https://github.com/feast-dev/feast/pull/253) ([zhilingc](https://github.com/zhilingc))

## [v0.1.8](https://github.com/feast-dev/feast/tree/v0.1.8) (2019-10-30)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.1.2...v0.1.8)

**Implemented enhancements:**

- Feast cli config file should be settable by an env var [\#149](https://github.com/feast-dev/feast/issues/149)
- Helm chart for deploying feast using Flink as runner [\#64](https://github.com/feast-dev/feast/issues/64)
- Get ingestion metrics when running on Flink runner [\#63](https://github.com/feast-dev/feast/issues/63)
- Move source types into their own package and discover them using java.util.ServiceLoader [\#61](https://github.com/feast-dev/feast/issues/61)
- Change config to yaml [\#51](https://github.com/feast-dev/feast/issues/51)
- Ability to pass runner option during ingestion job submission [\#50](https://github.com/feast-dev/feast/issues/50)

**Fixed bugs:**

- Fix Print Method in Feast CLI [\#211](https://github.com/feast-dev/feast/issues/211)
- Dataflow monitoring by core is failing with incorrect job id [\#153](https://github.com/feast-dev/feast/issues/153)
- Feast core crashes without logger set [\#150](https://github.com/feast-dev/feast/issues/150)

**Merged pull requests:**

- Remove redis transaction [\#280](https://github.com/feast-dev/feast/pull/280) ([pradithya](https://github.com/pradithya))
- Fix tracing to continue from existing trace created by grpc client [\#245](https://github.com/feast-dev/feast/pull/245) ([pradithya](https://github.com/pradithya))

## [v0.1.2](https://github.com/feast-dev/feast/tree/v0.1.2) (2019-08-23)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.1.1...v0.1.2)

**Fixed bugs:**

- Batch Import, feature with datetime format issue [\#203](https://github.com/feast-dev/feast/issues/203)
- Serving not correctly reporting readiness check if there is no activity [\#190](https://github.com/feast-dev/feast/issues/190)
- Serving stop periodically reloading feature specification after a while [\#188](https://github.com/feast-dev/feast/issues/188)

**Merged pull requests:**

- Add `romanwozniak` to prow owners config [\#216](https://github.com/feast-dev/feast/pull/216) ([romanwozniak](https://github.com/romanwozniak))
- Implement filter for create dataset api [\#215](https://github.com/feast-dev/feast/pull/215) ([pradithya](https://github.com/pradithya))
- expand raw column to accomodate more features ingested in one go [\#213](https://github.com/feast-dev/feast/pull/213) ([budi](https://github.com/budi))
- update feast installation docs [\#210](https://github.com/feast-dev/feast/pull/210) ([budi](https://github.com/budi))
- Add Prow job for unit testing Python SDK [\#209](https://github.com/feast-dev/feast/pull/209) ([davidheryanto](https://github.com/davidheryanto))
- fix create\_dataset [\#208](https://github.com/feast-dev/feast/pull/208) ([budi](https://github.com/budi))
- Update Feast installation doc [\#207](https://github.com/feast-dev/feast/pull/207) ([davidheryanto](https://github.com/davidheryanto))
- Fix unit test cli in prow script not returning correct exit code [\#206](https://github.com/feast-dev/feast/pull/206) ([davidheryanto](https://github.com/davidheryanto))
- Fix pytests and make TS conversion conditional [\#205](https://github.com/feast-dev/feast/pull/205) ([zhilingc](https://github.com/zhilingc))
- Use full prow build id as dataset name during test [\#200](https://github.com/feast-dev/feast/pull/200) ([davidheryanto](https://github.com/davidheryanto))
- Add Feast CLI / python SDK documentation [\#199](https://github.com/feast-dev/feast/pull/199) ([romanwozniak](https://github.com/romanwozniak))
- Update library version to fix security vulnerabilities in dependencies [\#198](https://github.com/feast-dev/feast/pull/198) ([davidheryanto](https://github.com/davidheryanto))
- Update Prow configuration for Feast CI [\#197](https://github.com/feast-dev/feast/pull/197) ([davidheryanto](https://github.com/davidheryanto))
- \[budi\] update python sdk quickstart [\#196](https://github.com/feast-dev/feast/pull/196) ([budi](https://github.com/budi))
- Readiness probe [\#191](https://github.com/feast-dev/feast/pull/191) ([pradithya](https://github.com/pradithya))
- Fix periodic feature spec reload [\#189](https://github.com/feast-dev/feast/pull/189) ([pradithya](https://github.com/pradithya))
- Fixed a typo in environment variable in installation [\#187](https://github.com/feast-dev/feast/pull/187) ([gauravkumar37](https://github.com/gauravkumar37))
- Revert "Update Quickstart" [\#185](https://github.com/feast-dev/feast/pull/185) ([zhilingc](https://github.com/zhilingc))
- Update Quickstart [\#184](https://github.com/feast-dev/feast/pull/184) ([pradithya](https://github.com/pradithya))
- Continuous integration and deployment \(CI/CD\) update [\#183](https://github.com/feast-dev/feast/pull/183) ([davidheryanto](https://github.com/davidheryanto))
- Remove feature specs being able to declare their serving or warehouse stores [\#159](https://github.com/feast-dev/feast/pull/159) ([tims](https://github.com/tims))

## [v0.1.1](https://github.com/feast-dev/feast/tree/v0.1.1) (2019-04-18)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.1.0...v0.1.1)

**Fixed bugs:**

- Fix BigQuery query template to retrieve training data [\#182](https://github.com/feast-dev/feast/pull/182) ([davidheryanto](https://github.com/davidheryanto))

**Merged pull requests:**

- Add python init files [\#176](https://github.com/feast-dev/feast/pull/176) ([zhilingc](https://github.com/zhilingc))
- Change pypi package from Feast to feast [\#173](https://github.com/feast-dev/feast/pull/173) ([zhilingc](https://github.com/zhilingc))

## [v0.1.0](https://github.com/feast-dev/feast/tree/v0.1.0) (2019-04-09)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.0.2...v0.1.0)

**Implemented enhancements:**

- Removal of storing historical value of feature in serving storage [\#53](https://github.com/feast-dev/feast/issues/53)
- Remove feature "granularity" and relegate to metadata [\#17](https://github.com/feast-dev/feast/issues/17)

**Closed issues:**

- Add ability to name an import job [\#167](https://github.com/feast-dev/feast/issues/167)
- Ingestion retrying an invalid FeatureRow endlessly [\#163](https://github.com/feast-dev/feast/issues/163)
- Ability to associate data ingested in Warehouse store to its ingestion job [\#145](https://github.com/feast-dev/feast/issues/145)
- Missing \(Fixing\) unit test for FeatureRowKafkaIO [\#132](https://github.com/feast-dev/feast/issues/132)

**Merged pull requests:**

- Catch all kind of exception to avoid retrying [\#171](https://github.com/feast-dev/feast/pull/171) ([pradithya](https://github.com/pradithya))
- Integration test [\#170](https://github.com/feast-dev/feast/pull/170) ([zhilingc](https://github.com/zhilingc))
- Proto error [\#169](https://github.com/feast-dev/feast/pull/169) ([pradithya](https://github.com/pradithya))
- Add --name flag to submit job [\#168](https://github.com/feast-dev/feast/pull/168) ([pradithya](https://github.com/pradithya))
- Prevent throwing RuntimeException when invalid proto is received [\#166](https://github.com/feast-dev/feast/pull/166) ([pradithya](https://github.com/pradithya))
- Add davidheryanto to OWNER file [\#165](https://github.com/feast-dev/feast/pull/165) ([pradithya](https://github.com/pradithya))
- Check validity of event timestamp in ValidateFeatureRowDoFn [\#164](https://github.com/feast-dev/feast/pull/164) ([pradithya](https://github.com/pradithya))
- Remove granularity [\#162](https://github.com/feast-dev/feast/pull/162) ([pradithya](https://github.com/pradithya))
- Better Kafka test [\#160](https://github.com/feast-dev/feast/pull/160) ([tims](https://github.com/tims))
- Simplify and document CLI building steps [\#158](https://github.com/feast-dev/feast/pull/158) ([thirteen37](https://github.com/thirteen37))
- Fix link typo in README.md [\#156](https://github.com/feast-dev/feast/pull/156) ([pradithya](https://github.com/pradithya))
- Add Feast admin quickstart guide [\#155](https://github.com/feast-dev/feast/pull/155) ([thirteen37](https://github.com/thirteen37))
- Pass all specs to ingestion by file [\#154](https://github.com/feast-dev/feast/pull/154) ([tims](https://github.com/tims))
- Preload spec in serving cache [\#152](https://github.com/feast-dev/feast/pull/152) ([pradithya](https://github.com/pradithya))
- Add job identifier to FeatureRow  [\#147](https://github.com/feast-dev/feast/pull/147) ([mansiib](https://github.com/mansiib))
- Fix unit tests [\#146](https://github.com/feast-dev/feast/pull/146) ([mansiib](https://github.com/mansiib))
- Add thirteen37 to OWNERS [\#144](https://github.com/feast-dev/feast/pull/144) ([thirteen37](https://github.com/thirteen37))
- Fix import spec created from Importer.from\_csv [\#143](https://github.com/feast-dev/feast/pull/143) ([pradithya](https://github.com/pradithya))
- Regenerate go [\#142](https://github.com/feast-dev/feast/pull/142) ([zhilingc](https://github.com/zhilingc))
- Flat JSON for pubsub and text files [\#141](https://github.com/feast-dev/feast/pull/141) ([tims](https://github.com/tims))
- Add wait flag for jobs, fix go proto path for dataset service [\#138](https://github.com/feast-dev/feast/pull/138) ([zhilingc](https://github.com/zhilingc))
- Fix Python SDK importer's ability to apply features [\#135](https://github.com/feast-dev/feast/pull/135) ([woop](https://github.com/woop))
- Refactor stores [\#110](https://github.com/feast-dev/feast/pull/110) ([tims](https://github.com/tims))
- Coalesce rows [\#89](https://github.com/feast-dev/feast/pull/89) ([tims](https://github.com/tims))
- Remove historical feature in serving store [\#87](https://github.com/feast-dev/feast/pull/87) ([pradithya](https://github.com/pradithya))

## [v0.0.2](https://github.com/feast-dev/feast/tree/v0.0.2) (2019-03-11)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.0.1...v0.0.2)

**Implemented enhancements:**

- Coalesce FeatureRows for improved "latest" value consistency in serving stores [\#88](https://github.com/feast-dev/feast/issues/88)
- Kafka source [\#22](https://github.com/feast-dev/feast/issues/22)

**Closed issues:**

- Preload Feast's spec in serving cache [\#151](https://github.com/feast-dev/feast/issues/151)
- Feast csv data upload job [\#137](https://github.com/feast-dev/feast/issues/137)
- Blocking call to start feast ingestion job [\#136](https://github.com/feast-dev/feast/issues/136)
- Python SDK fails to apply feature when submitting job [\#134](https://github.com/feast-dev/feast/issues/134)
- Default dump format should be changed for Python SDK [\#133](https://github.com/feast-dev/feast/issues/133)
- Listing resources and finding out system state [\#131](https://github.com/feast-dev/feast/issues/131)
- Reorganise ingestion store classes to match architecture  [\#109](https://github.com/feast-dev/feast/issues/109)

## [v0.0.1](https://github.com/feast-dev/feast/tree/v0.0.1) (2019-02-11)

[Full Changelog](https://github.com/feast-dev/feast/compare/ec9def2bbb06dc759538e4424caadd70f548ea64...v0.0.1)

**Implemented enhancements:**

- Spring boot CLI logs show up as JSON [\#104](https://github.com/feast-dev/feast/issues/104)
- Allow for registering feature that doesn't have warehouse store [\#5](https://github.com/feast-dev/feast/issues/5)

**Fixed bugs:**

- Error when submitting large import spec [\#125](https://github.com/feast-dev/feast/issues/125)
- Ingestion is not ignoring unknown feature in streaming source [\#99](https://github.com/feast-dev/feast/issues/99)
- Vulnerability in dependency \(core - jackson-databind \)  [\#92](https://github.com/feast-dev/feast/issues/92)
- TF file for cloud build trigger broken [\#72](https://github.com/feast-dev/feast/issues/72)
- Job Execution Failure with NullPointerException [\#46](https://github.com/feast-dev/feast/issues/46)
- Runtime Dependency Error After Upgrade to Beam 2.9.0 [\#44](https://github.com/feast-dev/feast/issues/44)
- \[FlinkRunner\] Core should not follow remote flink runner job to completion [\#21](https://github.com/feast-dev/feast/issues/21)
- Go packages in protos use incorrect repo [\#16](https://github.com/feast-dev/feast/issues/16)

**Merged pull requests:**

- Disable test during docker image creation [\#129](https://github.com/feast-dev/feast/pull/129) ([pradithya](https://github.com/pradithya))
- Repackage helm chart [\#127](https://github.com/feast-dev/feast/pull/127) ([pradithya](https://github.com/pradithya))
- Increase the column size for storing raw import spec [\#126](https://github.com/feast-dev/feast/pull/126) ([pradithya](https://github.com/pradithya))
- Update Helm Charts \(Redis, Logging\) [\#123](https://github.com/feast-dev/feast/pull/123) ([woop](https://github.com/woop))
- Added LOG\_TYPE environmental variable [\#120](https://github.com/feast-dev/feast/pull/120) ([woop](https://github.com/woop))
- Fix missing Redis write [\#119](https://github.com/feast-dev/feast/pull/119) ([pradithya](https://github.com/pradithya))
- add logging when error on request feature [\#117](https://github.com/feast-dev/feast/pull/117) ([pradithya](https://github.com/pradithya))
- run yarn run build during generate-resource [\#115](https://github.com/feast-dev/feast/pull/115) ([pradithya](https://github.com/pradithya))
- Add loadBalancerSourceRanges option for both serving and core [\#114](https://github.com/feast-dev/feast/pull/114) ([zhilingc](https://github.com/zhilingc))
- Build master [\#112](https://github.com/feast-dev/feast/pull/112) ([pradithya](https://github.com/pradithya))
- Cleanup warning while building proto files [\#108](https://github.com/feast-dev/feast/pull/108) ([pradithya](https://github.com/pradithya))
- Embed ui build & packaging into core's build [\#106](https://github.com/feast-dev/feast/pull/106) ([pradithya](https://github.com/pradithya))
- Add build badge to README [\#103](https://github.com/feast-dev/feast/pull/103) ([woop](https://github.com/woop))
- Ignore features in FeatureRow if it's not requested in import spec [\#101](https://github.com/feast-dev/feast/pull/101) ([pradithya](https://github.com/pradithya))
- Add override for serving service static ip [\#100](https://github.com/feast-dev/feast/pull/100) ([zhilingc](https://github.com/zhilingc))
- Fix go test [\#97](https://github.com/feast-dev/feast/pull/97) ([zhilingc](https://github.com/zhilingc))
- add missing copyright headers and fix test fail due to previous merge [\#95](https://github.com/feast-dev/feast/pull/95) ([tims](https://github.com/tims))
- Allow submission of kafka jobs [\#94](https://github.com/feast-dev/feast/pull/94) ([zhilingc](https://github.com/zhilingc))
- upgrade jackson databind for security vulnerability [\#93](https://github.com/feast-dev/feast/pull/93) ([tims](https://github.com/tims))
- Version revert [\#91](https://github.com/feast-dev/feast/pull/91) ([zhilingc](https://github.com/zhilingc))
- Fix validating feature row when the associated feature spec has no warehouse store [\#90](https://github.com/feast-dev/feast/pull/90) ([pradithya](https://github.com/pradithya))
- Add get command [\#85](https://github.com/feast-dev/feast/pull/85) ([zhilingc](https://github.com/zhilingc))
- Avoid error thrown when no storage for warehouse/serving is registered [\#83](https://github.com/feast-dev/feast/pull/83) ([pradithya](https://github.com/pradithya))
- Fix jackson dependency issue [\#82](https://github.com/feast-dev/feast/pull/82) ([zhilingc](https://github.com/zhilingc))
- Allow registration of feature without warehouse store [\#80](https://github.com/feast-dev/feast/pull/80) ([pradithya](https://github.com/pradithya))
- Remove branch from cloud build trigger [\#79](https://github.com/feast-dev/feast/pull/79) ([woop](https://github.com/woop))
- move read transforms into "source" package as FeatureSources [\#74](https://github.com/feast-dev/feast/pull/74) ([tims](https://github.com/tims))
- Fix tag regex in tf file [\#73](https://github.com/feast-dev/feast/pull/73) ([zhilingc](https://github.com/zhilingc))
- Update charts [\#71](https://github.com/feast-dev/feast/pull/71) ([mansiib](https://github.com/mansiib))
- Deduplicate storage ids before we fetch them [\#68](https://github.com/feast-dev/feast/pull/68) ([tims](https://github.com/tims))
- Check the size of result against deduplicated request [\#67](https://github.com/feast-dev/feast/pull/67) ([pradithya](https://github.com/pradithya))
- Add ability to submit ingestion job using Flink [\#62](https://github.com/feast-dev/feast/pull/62) ([pradithya](https://github.com/pradithya))
- Fix vulnerabilities for webpack-dev [\#59](https://github.com/feast-dev/feast/pull/59) ([budi](https://github.com/budi))
- Build push [\#56](https://github.com/feast-dev/feast/pull/56) ([zhilingc](https://github.com/zhilingc))
- Fix github vulnerability issue with webpack [\#54](https://github.com/feast-dev/feast/pull/54) ([budi](https://github.com/budi))
- Only lookup storage specs that we actually need [\#52](https://github.com/feast-dev/feast/pull/52) ([tims](https://github.com/tims))
- Link Python SDK RFC to PR and Issue [\#49](https://github.com/feast-dev/feast/pull/49) ([woop](https://github.com/woop))
- Python SDK [\#47](https://github.com/feast-dev/feast/pull/47) ([zhilingc](https://github.com/zhilingc))
- Update com.google.httpclient to be same as Beam's dependency [\#45](https://github.com/feast-dev/feast/pull/45) ([pradithya](https://github.com/pradithya))
- Bump Beam SDK to 2.9.0 [\#43](https://github.com/feast-dev/feast/pull/43) ([pradithya](https://github.com/pradithya))
- Add fix for tests failing in docker image [\#40](https://github.com/feast-dev/feast/pull/40) ([zhilingc](https://github.com/zhilingc))
- Change error store to be part of configuration instead [\#39](https://github.com/feast-dev/feast/pull/39) ([zhilingc](https://github.com/zhilingc))
- Fix location of Prow's Tide configuration [\#35](https://github.com/feast-dev/feast/pull/35) ([woop](https://github.com/woop))
- Add testing folder for deploying test infrastructure and running tests [\#34](https://github.com/feast-dev/feast/pull/34) ([woop](https://github.com/woop))
- skeleton contributing guide [\#33](https://github.com/feast-dev/feast/pull/33) ([tims](https://github.com/tims))
- allow empty string to select a NoOp write transform [\#30](https://github.com/feast-dev/feast/pull/30) ([tims](https://github.com/tims))
- Remove packaging ingestion as separate profile \(fix \#28\) [\#29](https://github.com/feast-dev/feast/pull/29) ([pradithya](https://github.com/pradithya))
- Change gopath to point to feast-dev repo [\#26](https://github.com/feast-dev/feast/pull/26) ([zhilingc](https://github.com/zhilingc))
- Fixes \#31 - errors during kafka deserializer \(passing\) test execution [\#25](https://github.com/feast-dev/feast/pull/25) ([baskaranz](https://github.com/baskaranz))
- Kafka IO fixes [\#23](https://github.com/feast-dev/feast/pull/23) ([tims](https://github.com/tims))
- KafkaIO implementation for feast [\#19](https://github.com/feast-dev/feast/pull/19) ([baskaranz](https://github.com/baskaranz))
- Return same type string for warehouse and serving NoOp stores [\#18](https://github.com/feast-dev/feast/pull/18) ([tims](https://github.com/tims))
- \#12: prefetch specs and validate on job expansion [\#15](https://github.com/feast-dev/feast/pull/15) ([tims](https://github.com/tims))
- Added RFC for Feast Python SDK [\#14](https://github.com/feast-dev/feast/pull/14) ([woop](https://github.com/woop))
- Add more validation in feature spec registration [\#11](https://github.com/feast-dev/feast/pull/11) ([pradithya](https://github.com/pradithya))
- Added rfcs/ folder with readme and template [\#10](https://github.com/feast-dev/feast/pull/10) ([woop](https://github.com/woop))
- Expose ui service rpc [\#9](https://github.com/feast-dev/feast/pull/9) ([pradithya](https://github.com/pradithya))
- Add Feast overview to README [\#8](https://github.com/feast-dev/feast/pull/8) ([woop](https://github.com/woop))
- Directory structure changes [\#7](https://github.com/feast-dev/feast/pull/7) ([zhilingc](https://github.com/zhilingc))
- Change register to apply [\#4](https://github.com/feast-dev/feast/pull/4) ([zhilingc](https://github.com/zhilingc))
- Empty response handling in serving api [\#3](https://github.com/feast-dev/feast/pull/3) ([pradithya](https://github.com/pradithya))
- Proto file fixes [\#1](https://github.com/feast-dev/feast/pull/1) ([pradithya](https://github.com/pradithya))
