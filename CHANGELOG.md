# Changelog

## [v0.18.1](https://github.com/feast-dev/feast/tree/v0.18.1) (2022-02-15)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.18.0...v0.18.1)

**Fixed bugs:**

- ODFVs raise a PerformanceWarning for very large sets of features [\#2293](https://github.com/feast-dev/feast/issues/2293)
- Don't require `snowflake` to always be installed [\#2309](https://github.com/feast-dev/feast/pull/2309) ([judahrand](https://github.com/judahrand))
- podAnnotations Values in the feature-server chart [\#2304](https://github.com/feast-dev/feast/pull/2304) ([tpvasconcelos](https://github.com/tpvasconcelos))
- Fixing the Java helm charts and adding a demo tutorial on how to use them [\#2298](https://github.com/feast-dev/feast/pull/2298) ([adchia](https://github.com/adchia))
- avoid using transactions on OSS Redis [\#2296](https://github.com/feast-dev/feast/pull/2296) ([DvirDukhan](https://github.com/DvirDukhan))
- Include infra objects in registry dump and fix Infra's from\_proto  [\#2295](https://github.com/feast-dev/feast/pull/2295) ([adchia](https://github.com/adchia))
- Expose snowflake credentials  for unit testing [\#2288](https://github.com/feast-dev/feast/pull/2288) ([sfc-gh-madkins](https://github.com/sfc-gh-madkins))
- Fix flaky tests \(test\_online\_store\_cleanup & test\_feature\_get\_online\_features\_types\_match\) [\#2276](https://github.com/feast-dev/feast/pull/2276) ([pyalex](https://github.com/pyalex))

**Merged pull requests:**

- Remove old flag warning with the python feature server [\#2300](https://github.com/feast-dev/feast/pull/2300) ([adchia](https://github.com/adchia))
- Use an OFFLINE schema for Snowflake offline store tests [\#2291](https://github.com/feast-dev/feast/pull/2291) ([sfc-gh-madkins](https://github.com/sfc-gh-madkins))
- fix typos in markdown files [\#2289](https://github.com/feast-dev/feast/pull/2289) ([charliec443](https://github.com/charliec443))
- Add -SNAPSHOT suffix to pom.xml version [\#2286](https://github.com/feast-dev/feast/pull/2286) ([tsotnet](https://github.com/tsotnet))
- Update CONTRIBUTING.md [\#2282](https://github.com/feast-dev/feast/pull/2282) ([adchia](https://github.com/adchia))

## [v0.18.0](https://github.com/feast-dev/feast/tree/v0.18.0) (2022-02-05)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.17.0...v0.18.0)

**Implemented enhancements:**

- Tutorial on validation of historical features [\#2277](https://github.com/feast-dev/feast/pull/2277) ([pyalex](https://github.com/pyalex))
- Feast plan clean up [\#2256](https://github.com/feast-dev/feast/pull/2256) ([felixwang9817](https://github.com/felixwang9817))
- Return `UNIX\_TIMESTAMP` as Python `datetime` [\#2244](https://github.com/feast-dev/feast/pull/2244) ([judahrand](https://github.com/judahrand))
- Validating historical features against reference dataset with "great expectations" profiler [\#2243](https://github.com/feast-dev/feast/pull/2243) ([pyalex](https://github.com/pyalex))
- Implement feature\_store.\_apply\_diffs to handle registry and infra diffs [\#2238](https://github.com/feast-dev/feast/pull/2238) ([felixwang9817](https://github.com/felixwang9817))
- Compare Python objects instead of proto objects [\#2227](https://github.com/feast-dev/feast/pull/2227) ([felixwang9817](https://github.com/felixwang9817))
- Modify feature\_store.plan to produce an InfraDiff [\#2211](https://github.com/feast-dev/feast/pull/2211) ([felixwang9817](https://github.com/felixwang9817))
- Implement diff\_infra\_protos method for feast plan [\#2204](https://github.com/feast-dev/feast/pull/2204) ([felixwang9817](https://github.com/felixwang9817))
- Persisting results of historical retrieval [\#2197](https://github.com/feast-dev/feast/pull/2197) ([pyalex](https://github.com/pyalex))
- Merge feast-snowflake plugin into main repo with documentation [\#2193](https://github.com/feast-dev/feast/pull/2193) ([sfc-gh-madkins](https://github.com/sfc-gh-madkins))
- Add InfraDiff class for feast plan [\#2190](https://github.com/feast-dev/feast/pull/2190) ([felixwang9817](https://github.com/felixwang9817))
- Use FeatureViewProjection instead of FeatureView in ODFV [\#2186](https://github.com/feast-dev/feast/pull/2186) ([judahrand](https://github.com/judahrand))

**Fixed bugs:**

- Set `created\_timestamp` and `last\_updated\_timestamp` fields [\#2266](https://github.com/feast-dev/feast/pull/2266) ([judahrand](https://github.com/judahrand))
- Use `datetime.utcnow\(\)` to avoid timezone issues [\#2265](https://github.com/feast-dev/feast/pull/2265) ([judahrand](https://github.com/judahrand))
- Fix Redis key serialization in java feature server [\#2264](https://github.com/feast-dev/feast/pull/2264) ([pyalex](https://github.com/pyalex))
- modify registry.db s3 object initialization to work in S3 subdirectory with Java Feast Server [\#2259](https://github.com/feast-dev/feast/pull/2259) ([NalinGHub](https://github.com/NalinGHub))
- Add snowflake environment variables to allow testing on snowflake infra [\#2258](https://github.com/feast-dev/feast/pull/2258) ([sfc-gh-madkins](https://github.com/sfc-gh-madkins))
- Correct inconsistent dependency [\#2255](https://github.com/feast-dev/feast/pull/2255) ([judahrand](https://github.com/judahrand))
- Fix for historical field mappings [\#2252](https://github.com/feast-dev/feast/pull/2252) ([michelle-rascati-sp](https://github.com/michelle-rascati-sp))
- Add backticks to left\_table\_query\_string [\#2250](https://github.com/feast-dev/feast/pull/2250) ([dmille](https://github.com/dmille))
- Fix inference of BigQuery ARRAY types. [\#2245](https://github.com/feast-dev/feast/pull/2245) ([judahrand](https://github.com/judahrand))
- Fix Redshift data creator [\#2242](https://github.com/feast-dev/feast/pull/2242) ([felixwang9817](https://github.com/felixwang9817))
- Delete entity key from Redis only when all attached feature views are gone [\#2240](https://github.com/feast-dev/feast/pull/2240) ([pyalex](https://github.com/pyalex))
- Tests for transformation service integration in java feature server [\#2236](https://github.com/feast-dev/feast/pull/2236) ([pyalex](https://github.com/pyalex))
- Feature server helm chart produces invalid YAML [\#2234](https://github.com/feast-dev/feast/pull/2234) ([pyalex](https://github.com/pyalex))
- Docker build fails for java feature server [\#2230](https://github.com/feast-dev/feast/pull/2230) ([pyalex](https://github.com/pyalex))
- Fix ValueType.UNIX\_TIMESTAMP conversions [\#2219](https://github.com/feast-dev/feast/pull/2219) ([judahrand](https://github.com/judahrand))
- Add on demand feature views deletion [\#2203](https://github.com/feast-dev/feast/pull/2203) ([corentinmarek](https://github.com/corentinmarek))
- Compare only specs in integration tests [\#2200](https://github.com/feast-dev/feast/pull/2200) ([felixwang9817](https://github.com/felixwang9817))
- Bump log4j-core from 2.17.0 to 2.17.1 in /java [\#2189](https://github.com/feast-dev/feast/pull/2189) ([dependabot[bot]](https://github.com/apps/dependabot))
- Support multiple application properties files \(incl from classpath\) [\#2187](https://github.com/feast-dev/feast/pull/2187) ([pyalex](https://github.com/pyalex))
- Avoid requesting features from OnlineStore twice [\#2185](https://github.com/feast-dev/feast/pull/2185) ([judahrand](https://github.com/judahrand))
- Speed up Datastore deletes by batch deletions with multithreading [\#2182](https://github.com/feast-dev/feast/pull/2182) ([ptoman-pa](https://github.com/ptoman-pa))
- Fixes large payload runtime exception in Datastore \(issue 1633\) [\#2181](https://github.com/feast-dev/feast/pull/2181) ([ptoman-pa](https://github.com/ptoman-pa))

**Merged pull requests:**

- Add link to community plugin for Spark offline store [\#2279](https://github.com/feast-dev/feast/pull/2279) ([adchia](https://github.com/adchia))
- Fix broken links on documentation [\#2278](https://github.com/feast-dev/feast/pull/2278) ([adchia](https://github.com/adchia))
- Publish alternative python package with FEAST\_USAGE=False by default [\#2275](https://github.com/feast-dev/feast/pull/2275) ([pyalex](https://github.com/pyalex))
- Unify all helm charts versions [\#2274](https://github.com/feast-dev/feast/pull/2274) ([pyalex](https://github.com/pyalex))
- Fix / update helm chart workflows to push the feast python server [\#2273](https://github.com/feast-dev/feast/pull/2273) ([adchia](https://github.com/adchia))
- Update Feast Serving documentation with ways to run and debug locally [\#2272](https://github.com/feast-dev/feast/pull/2272) ([adchia](https://github.com/adchia))
- Fix Snowflake docs [\#2270](https://github.com/feast-dev/feast/pull/2270) ([felixwang9817](https://github.com/felixwang9817))
- Update local-feature-server.md [\#2269](https://github.com/feast-dev/feast/pull/2269) ([tsotnet](https://github.com/tsotnet))
- Update docs to include Snowflake/DQM and removing unused docs from old versions of Feast [\#2268](https://github.com/feast-dev/feast/pull/2268) ([adchia](https://github.com/adchia))
- Graduate Python feature server [\#2263](https://github.com/feast-dev/feast/pull/2263) ([felixwang9817](https://github.com/felixwang9817))
- Fix benchmark tests at HEAD by passing in Snowflake secrets [\#2262](https://github.com/feast-dev/feast/pull/2262) ([adchia](https://github.com/adchia))
- Refactor `pa\_to\_feast\_value\_type` [\#2246](https://github.com/feast-dev/feast/pull/2246) ([judahrand](https://github.com/judahrand))
- Allow using pandas.StringDtype to support on-demand features with STRING type [\#2229](https://github.com/feast-dev/feast/pull/2229) ([pyalex](https://github.com/pyalex))
- Bump jackson-databind from 2.10.1 to 2.10.5.1 in /java/common [\#2228](https://github.com/feast-dev/feast/pull/2228) ([dependabot[bot]](https://github.com/apps/dependabot))
- Split apply total parse repo [\#2226](https://github.com/feast-dev/feast/pull/2226) ([mickey-liu](https://github.com/mickey-liu))
- Publish renamed java packages to maven central \(via Sonatype\) [\#2225](https://github.com/feast-dev/feast/pull/2225) ([pyalex](https://github.com/pyalex))
- Make online store nullable [\#2224](https://github.com/feast-dev/feast/pull/2224) ([mirayyuce](https://github.com/mirayyuce))
- Optimize `\_populate\_result\_rows\_from\_feature\_view` [\#2223](https://github.com/feast-dev/feast/pull/2223) ([judahrand](https://github.com/judahrand))
- Update to newer `redis-py` [\#2221](https://github.com/feast-dev/feast/pull/2221) ([judahrand](https://github.com/judahrand))
- Adding a local feature server test [\#2217](https://github.com/feast-dev/feast/pull/2217) ([adchia](https://github.com/adchia))
- replace GetOnlineFeaturesResponse with GetOnlineFeaturesResponseV2 in… [\#2214](https://github.com/feast-dev/feast/pull/2214) ([tsotnet](https://github.com/tsotnet))
- Updates to click==8.\* [\#2210](https://github.com/feast-dev/feast/pull/2210) ([diogommartins](https://github.com/diogommartins))
- Bump protobuf-java from 3.12.2 to 3.16.1 in /java [\#2208](https://github.com/feast-dev/feast/pull/2208) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add default priority for bug reports [\#2207](https://github.com/feast-dev/feast/pull/2207) ([adchia](https://github.com/adchia))
- Modify issue templates to automatically attach labels [\#2205](https://github.com/feast-dev/feast/pull/2205) ([adchia](https://github.com/adchia))
- Python FeatureServer optimization [\#2202](https://github.com/feast-dev/feast/pull/2202) ([judahrand](https://github.com/judahrand))
- Refactor all importer logic to belong in feast.importer [\#2199](https://github.com/feast-dev/feast/pull/2199) ([felixwang9817](https://github.com/felixwang9817))
- Refactor `OnlineResponse.to\_dict\(\)` [\#2196](https://github.com/feast-dev/feast/pull/2196) ([judahrand](https://github.com/judahrand))
- \[Java feature server\] Converge ServingService API to make Python and Java feature servers consistent [\#2166](https://github.com/feast-dev/feast/pull/2166) ([pyalex](https://github.com/pyalex))
- Add a unit test for the tag\_proto\_objects method [\#2163](https://github.com/feast-dev/feast/pull/2163) ([achals](https://github.com/achals))


## [v0.17.0](https://github.com/feast-dev/feast/tree/v0.17.0) (2021-12-31)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.16.1...v0.17.0)

**Implemented enhancements:**

- Add feast-python-server helm chart [\#2177](https://github.com/feast-dev/feast/pull/2177) ([michelle-rascati-sp](https://github.com/michelle-rascati-sp))
- Add SqliteTable as an InfraObject [\#2157](https://github.com/feast-dev/feast/pull/2157) ([felixwang9817](https://github.com/felixwang9817))
- Compute property-level diffs for repo objects [\#2156](https://github.com/feast-dev/feast/pull/2156) ([achals](https://github.com/achals))
- Add a feast plan command, and have CLI output differentiates between created, deleted and unchanged objects [\#2147](https://github.com/feast-dev/feast/pull/2147) ([achals](https://github.com/achals))
- Refactor tag methods to infer created, deleted, and kept repo objects  [\#2142](https://github.com/feast-dev/feast/pull/2142) ([achals](https://github.com/achals))
- Add DatastoreTable infra object [\#2140](https://github.com/feast-dev/feast/pull/2140) ([felixwang9817](https://github.com/felixwang9817))
- Dynamodb infra object [\#2131](https://github.com/feast-dev/feast/pull/2131) ([felixwang9817](https://github.com/felixwang9817))
- Add Infra and InfraObjects classes [\#2125](https://github.com/feast-dev/feast/pull/2125) ([felixwang9817](https://github.com/felixwang9817))
- Pre compute the timestamp range for feature views [\#2103](https://github.com/feast-dev/feast/pull/2103) ([judahrand](https://github.com/judahrand))

**Fixed bugs:**

- Fix issues with java docker building [\#2178](https://github.com/feast-dev/feast/pull/2178) ([achals](https://github.com/achals))
- unpin boto dependency in setup [\#2168](https://github.com/feast-dev/feast/pull/2168) ([fengyu05](https://github.com/fengyu05))
- Fix issue with numpy datetimes in feast\_value\_type\_to\_pandas\_type [\#2167](https://github.com/feast-dev/feast/pull/2167) ([achals](https://github.com/achals))
- Fix `BYTES` and `BYTES_LIST` type conversion [\#2158](https://github.com/feast-dev/feast/pull/2158) ([judahrand](https://github.com/judahrand))
- Use correct name when deleting dynamo table [\#2154](https://github.com/feast-dev/feast/pull/2154) ([pyalex](https://github.com/pyalex))
- Bump log4j-core from 2.15.0 to 2.16.0 in /java [\#2146](https://github.com/feast-dev/feast/pull/2146) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump log4j-api from 2.15.0 to 2.16.0 in /java [\#2145](https://github.com/feast-dev/feast/pull/2145) ([dependabot[bot]](https://github.com/apps/dependabot))
- Respect `full_feature_names` for ODFVs [\#2144](https://github.com/feast-dev/feast/pull/2144) ([judahrand](https://github.com/judahrand))
- Cache dynamodb client and resource in DynamoDB online store implement… [\#2138](https://github.com/feast-dev/feast/pull/2138) ([felixwang9817](https://github.com/felixwang9817))
- Bump log4j-api from 2.13.2 to 2.15.0 in /java [\#2133](https://github.com/feast-dev/feast/pull/2133) ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix release workflow to use the new GCP action [\#2132](https://github.com/feast-dev/feast/pull/2132) ([adchia](https://github.com/adchia))
- Remove spring-boot from the feast serving application [\#2127](https://github.com/feast-dev/feast/pull/2127) ([achals](https://github.com/achals))
- Fix Makefile to properly create the ECR\_VERSION [\#2123](https://github.com/feast-dev/feast/pull/2123) ([adchia](https://github.com/adchia))

**Closed issues:**

- In GH workflow docker images are being built but not published [\#2152](https://github.com/feast-dev/feast/issues/2152)
- Any plan to make Feast 0.10+ support docker [\#2148](https://github.com/feast-dev/feast/issues/2148)
- ODFVs don't respect `full_feature_names` [\#2143](https://github.com/feast-dev/feast/issues/2143)
- Release workflow does not work [\#2136](https://github.com/feast-dev/feast/issues/2136)
- Redis Online Store - Truncate and Load [\#2129](https://github.com/feast-dev/feast/issues/2129)

**Merged pull requests:**

- Update roadmap to include Snowflake + Trino. Also fix docs + update FAQ [\#2175](https://github.com/feast-dev/feast/pull/2175) ([adchia](https://github.com/adchia))
- Convert python values into proto values in bulk [\#2172](https://github.com/feast-dev/feast/pull/2172) ([pyalex](https://github.com/pyalex))
- Push docker image after build in GH workflow [\#2171](https://github.com/feast-dev/feast/pull/2171) ([pyalex](https://github.com/pyalex))
- Improve serialization performance [\#2165](https://github.com/feast-dev/feast/pull/2165) ([judahrand](https://github.com/judahrand))
- Improve online deserialization latency [\#2164](https://github.com/feast-dev/feast/pull/2164) ([judahrand](https://github.com/judahrand))
- Add a unit test for the tag\_proto\_objects method [\#2163](https://github.com/feast-dev/feast/pull/2163) ([achals](https://github.com/achals))
- Bump log4j-core from 2.16.0 to 2.17.0 in /java [\#2161](https://github.com/feast-dev/feast/pull/2161) ([dependabot[bot]](https://github.com/apps/dependabot))
- \[Java Feature Server\] Use hgetall in redis connector when number of retrieved fields is big enough [\#2159](https://github.com/feast-dev/feast/pull/2159) ([pyalex](https://github.com/pyalex))
- Do not run benchmarks on pull requests [\#2155](https://github.com/feast-dev/feast/pull/2155) ([felixwang9817](https://github.com/felixwang9817))
- Ensure that universal CLI test tears down infrastructure [\#2151](https://github.com/feast-dev/feast/pull/2151) ([felixwang9817](https://github.com/felixwang9817))
- Remove underscores from ECR docker versions [\#2139](https://github.com/feast-dev/feast/pull/2139) ([achals](https://github.com/achals))
- Run PR integration tests only on python 3.7 [\#2137](https://github.com/feast-dev/feast/pull/2137) ([achals](https://github.com/achals))
- Update changelog for 0.16.1 and update helm charts [\#2135](https://github.com/feast-dev/feast/pull/2135) ([adchia](https://github.com/adchia))
- Bump log4j-core from 2.13.2 to 2.15.0 in /java [\#2134](https://github.com/feast-dev/feast/pull/2134) ([dependabot[bot]](https://github.com/apps/dependabot))
- Updating lambda docker image to feature-server-python-aws [\#2130](https://github.com/feast-dev/feast/pull/2130) ([adchia](https://github.com/adchia))
- Fix README to reflect new integration test suites [\#2124](https://github.com/feast-dev/feast/pull/2124) ([adchia](https://github.com/adchia))
- Change the feast serve endpoint to be sync rather than async. [\#2119](https://github.com/feast-dev/feast/pull/2119) ([nossrannug](https://github.com/nossrannug))
- Remove  argument `feature_refs` [\#2115](https://github.com/feast-dev/feast/pull/2115) ([judahrand](https://github.com/judahrand))
- Fix leaking dynamodb tables in integration tests [\#2104](https://github.com/feast-dev/feast/pull/2104) ([pyalex](https://github.com/pyalex))
- Remove untested and undocumented interfaces [\#2084](https://github.com/feast-dev/feast/pull/2084) ([judahrand](https://github.com/judahrand))
- Update creating-a-custom-provider.md [\#2070](https://github.com/feast-dev/feast/pull/2070) ([ChaitanyaKN](https://github.com/ChaitanyaKN))
## [v0.16.1](https://github.com/feast-dev/feast/tree/v0.16.1) (2021-12-10)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.16.0...v0.16.1)

**Fixed bugs:**

- Bump log4j-api from 2.13.2 to 2.15.0 in /java [\#2133](https://github.com/feast-dev/feast/pull/2133) ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix release workflow to use the new GCP action [\#2132](https://github.com/feast-dev/feast/pull/2132) ([adchia](https://github.com/adchia))
- Fix Makefile to properly create the ECR\_VERSION [\#2123](https://github.com/feast-dev/feast/pull/2123) ([adchia](https://github.com/adchia))

**Merged pull requests:**

- Updating lambda docker image to feature-server-python-aws [\#2130](https://github.com/feast-dev/feast/pull/2130) ([adchia](https://github.com/adchia))
- Fix README to reflect new integration test suites [\#2124](https://github.com/feast-dev/feast/pull/2124) ([adchia](https://github.com/adchia))
- Remove  argument `feature_refs` [\#2115](https://github.com/feast-dev/feast/pull/2115) ([judahrand](https://github.com/judahrand))

## [v0.16.0](https://github.com/feast-dev/feast/tree/v0.16.0) (2021-12-08)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.15.1...v0.16.0)

**Implemented enhancements:**

- Install redis extra in AWS Lambda feature server & add hiredis depend… [\#2057](https://github.com/feast-dev/feast/pull/2057) ([tsotnet](https://github.com/tsotnet))
- Support of GC and S3 storages for registry in Java Feature Server [\#2043](https://github.com/feast-dev/feast/pull/2043) ([pyalex](https://github.com/pyalex))
- Adding stream ingestion alpha documentation [\#2005](https://github.com/feast-dev/feast/pull/2005) ([adchia](https://github.com/adchia))

**Fixed bugs:**

- requested\_features are not passed to online\_read\(\) from passthrough\_provider [\#2106](https://github.com/feast-dev/feast/issues/2106)
- `feast apply` broken with 0.15.\* if the registry already exists [\#2086](https://github.com/feast-dev/feast/issues/2086)
- Inconsistent logic with `on_demand_feature_views` [\#2072](https://github.com/feast-dev/feast/issues/2072)
- Fix release workflow to pass the python version and docker build targets [\#2122](https://github.com/feast-dev/feast/pull/2122) ([adchia](https://github.com/adchia))
- requested\_features is passed to online\_read from passthrough\_provider [\#2107](https://github.com/feast-dev/feast/pull/2107) ([aurobindoc](https://github.com/aurobindoc))
- Don't materialize FeatureViews where `online is False` [\#2101](https://github.com/feast-dev/feast/pull/2101) ([judahrand](https://github.com/judahrand))
- Have apply\_total use the repo\_config that's passed in as a parameter \(makes it more compatible with custom wrapper code\) [\#2099](https://github.com/feast-dev/feast/pull/2099) ([mavysavydav](https://github.com/mavysavydav))
- Do not attempt to compute ODFVs when there are no ODFVs [\#2090](https://github.com/feast-dev/feast/pull/2090) ([felixwang9817](https://github.com/felixwang9817))
- Duplicate feast apply bug [\#2087](https://github.com/feast-dev/feast/pull/2087) ([felixwang9817](https://github.com/felixwang9817))
- Add --host as an option for feast serve [\#2078](https://github.com/feast-dev/feast/pull/2078) ([nossrannug](https://github.com/nossrannug))
- Fix feature server docker image tag generation in pr integration tests [\#2077](https://github.com/feast-dev/feast/pull/2077) ([tsotnet](https://github.com/tsotnet))
- Fix ECR Image build on master branch [\#2076](https://github.com/feast-dev/feast/pull/2076) ([tsotnet](https://github.com/tsotnet))
- Optimize memory usage during materialization [\#2073](https://github.com/feast-dev/feast/pull/2073) ([judahrand](https://github.com/judahrand))
- Fix unexpected feature view deletion when applying edited odfv [\#2054](https://github.com/feast-dev/feast/pull/2054) ([ArrichM](https://github.com/ArrichM))
- Properly exclude entities from feature inference [\#2048](https://github.com/feast-dev/feast/pull/2048) ([mavysavydav](https://github.com/mavysavydav))
- Don't allow FeatureStore.apply with commit=False [\#2047](https://github.com/feast-dev/feast/pull/2047) ([nossrannug](https://github.com/nossrannug))
- Fix bug causing OnDemandFeatureView.infer\_features\(\) to fail when the… [\#2046](https://github.com/feast-dev/feast/pull/2046) ([ArrichM](https://github.com/ArrichM))
- Add missing comma in setup.py [\#2031](https://github.com/feast-dev/feast/pull/2031) ([achals](https://github.com/achals))
- Correct cleanup after usage e2e tests [\#2015](https://github.com/feast-dev/feast/pull/2015) ([pyalex](https://github.com/pyalex))
- Change Environment timestamps to be in UTC [\#2007](https://github.com/feast-dev/feast/pull/2007) ([felixwang9817](https://github.com/felixwang9817))
- get\_online\_features on demand transform bug fixes + local integration test mode [\#2004](https://github.com/feast-dev/feast/pull/2004) ([adchia](https://github.com/adchia))
- Always pass full and partial feature names to ODFV [\#2003](https://github.com/feast-dev/feast/pull/2003) ([judahrand](https://github.com/judahrand))
- ODFV UDFs should handle list types [\#2002](https://github.com/feast-dev/feast/pull/2002) ([Agent007](https://github.com/Agent007))
- Update bq\_to\_feast\_value\_type with BOOLEAN type as a legacy sql data type [\#1996](https://github.com/feast-dev/feast/pull/1996) ([mavysavydav](https://github.com/mavysavydav))
- Fix bug where using some Pandas dtypes in the output of an ODFV fails [\#1994](https://github.com/feast-dev/feast/pull/1994) ([judahrand](https://github.com/judahrand))
- Fix duplicate update infra [\#1990](https://github.com/feast-dev/feast/pull/1990) ([felixwang9817](https://github.com/felixwang9817))
- Improve performance of \_convert\_arrow\_to\_proto [\#1984](https://github.com/feast-dev/feast/pull/1984) ([nossrannug](https://github.com/nossrannug))

**Merged pull requests:**

- Add changelog for v0.16.0 [\#2120](https://github.com/feast-dev/feast/pull/2120) ([adchia](https://github.com/adchia))
- Update FAQ [\#2118](https://github.com/feast-dev/feast/pull/2118) ([felixwang9817](https://github.com/felixwang9817))
- Move helm chart back to main repo [\#2113](https://github.com/feast-dev/feast/pull/2113) ([pyalex](https://github.com/pyalex))
- Set package long description encoding to UTF-8 [\#2111](https://github.com/feast-dev/feast/pull/2111) ([danilopeixoto](https://github.com/danilopeixoto))
- Update release workflow to include new docker images [\#2108](https://github.com/feast-dev/feast/pull/2108) ([adchia](https://github.com/adchia))
- Use the maintainers group in Codeowners instead of individuals [\#2102](https://github.com/feast-dev/feast/pull/2102) ([achals](https://github.com/achals))
- Remove tfx schema from ValueType [\#2098](https://github.com/feast-dev/feast/pull/2098) ([pyalex](https://github.com/pyalex))
- Add data source implementations to RTD docs [\#2097](https://github.com/feast-dev/feast/pull/2097) ([felixwang9817](https://github.com/felixwang9817))
- Updated feature view documentation to include blurb about feature inferencing [\#2096](https://github.com/feast-dev/feast/pull/2096) ([mavysavydav](https://github.com/mavysavydav))
- Fix integration test that is unstable due to incorrect materialization boundaries [\#2095](https://github.com/feast-dev/feast/pull/2095) ([pyalex](https://github.com/pyalex))
- Broaden google-cloud-core dependency [\#2094](https://github.com/feast-dev/feast/pull/2094) ([ptoman-pa](https://github.com/ptoman-pa))
- Use pip-tools to lock versions of dependent packages [\#2093](https://github.com/feast-dev/feast/pull/2093) ([ysk24ok](https://github.com/ysk24ok))
- Fix typo in feature retrieval doc [\#2092](https://github.com/feast-dev/feast/pull/2092) ([olivierlabreche](https://github.com/olivierlabreche))
- Fix typo in FeatureView example \(doc\) [\#2091](https://github.com/feast-dev/feast/pull/2091) ([olivierlabreche](https://github.com/olivierlabreche))
- Use request.addfinalizer instead of the yield based approach in integ tests [\#2089](https://github.com/feast-dev/feast/pull/2089) ([achals](https://github.com/achals))
- Odfv logic [\#2088](https://github.com/feast-dev/feast/pull/2088) ([felixwang9817](https://github.com/felixwang9817))
- Refactor `_convert_arrow_to_proto` [\#2085](https://github.com/feast-dev/feast/pull/2085) ([judahrand](https://github.com/judahrand))
- Add github run id into the integration test projects for debugging [\#2069](https://github.com/feast-dev/feast/pull/2069) ([achals](https://github.com/achals))
- Fixing broken entity key link in quickstart [\#2068](https://github.com/feast-dev/feast/pull/2068) ([adchia](https://github.com/adchia))
- Fix java\_release workflow by removing step without users/with [\#2067](https://github.com/feast-dev/feast/pull/2067) ([achals](https://github.com/achals))
- Allow using cached registry when writing to the online store [\#2066](https://github.com/feast-dev/feast/pull/2066) ([achals](https://github.com/achals))
- Raise import error when repo configs module cannot be imported [\#2065](https://github.com/feast-dev/feast/pull/2065) ([felixwang9817](https://github.com/felixwang9817))
- Remove refs to tensorflow\_metadata [\#2063](https://github.com/feast-dev/feast/pull/2063) ([achals](https://github.com/achals))
- Add detailed error messages for test\_univerisal\_e2e failures [\#2062](https://github.com/feast-dev/feast/pull/2062) ([achals](https://github.com/achals))
- Remove unused protos & deprecated java modules [\#2061](https://github.com/feast-dev/feast/pull/2061) ([pyalex](https://github.com/pyalex))
- Asynchronously refresh registry in transformation service [\#2060](https://github.com/feast-dev/feast/pull/2060) ([pyalex](https://github.com/pyalex))
- Fix GH workflow for docker build of java parts [\#2059](https://github.com/feast-dev/feast/pull/2059) ([pyalex](https://github.com/pyalex))
- Dedicated workflow for java PRs [\#2050](https://github.com/feast-dev/feast/pull/2050) ([pyalex](https://github.com/pyalex))
- Run java integration test with real google cloud and aws [\#2049](https://github.com/feast-dev/feast/pull/2049) ([pyalex](https://github.com/pyalex))
- Fixing typo enabling on\_demand\_transforms [\#2044](https://github.com/feast-dev/feast/pull/2044) ([ArrichM](https://github.com/ArrichM))
- Make `feast registry-dump` print the whole registry as one json [\#2040](https://github.com/feast-dev/feast/pull/2040) ([nossrannug](https://github.com/nossrannug))
- Remove tensorflow-metadata folders [\#2038](https://github.com/feast-dev/feast/pull/2038) ([casassg](https://github.com/casassg))
- Update CHANGELOG for Feast v0.15.1 [\#2034](https://github.com/feast-dev/feast/pull/2034) ([felixwang9817](https://github.com/felixwang9817))
- Remove unsupported java parts [\#2029](https://github.com/feast-dev/feast/pull/2029) ([pyalex](https://github.com/pyalex))
- Fix checked out branch for PR docker image build workflow [\#2018](https://github.com/feast-dev/feast/pull/2018) ([tsotnet](https://github.com/tsotnet))
- Extend "feast in production" page  with description of java feature server [\#2017](https://github.com/feast-dev/feast/pull/2017) ([pyalex](https://github.com/pyalex))
- Remove duplicates in setup.py and run rudimentary verifications [\#2016](https://github.com/feast-dev/feast/pull/2016) ([achals](https://github.com/achals))
- Upload feature server docker image to ECR on approved PRs [\#2014](https://github.com/feast-dev/feast/pull/2014) ([tsotnet](https://github.com/tsotnet))
- GitBook: \[\#1\] Plugin standards documentation [\#2011](https://github.com/feast-dev/feast/pull/2011) ([felixwang9817](https://github.com/felixwang9817))
- Add changelog for v0.15.0 [\#2006](https://github.com/feast-dev/feast/pull/2006) ([adchia](https://github.com/adchia))
- Add integration tests for AWS Lambda feature server [\#2001](https://github.com/feast-dev/feast/pull/2001) ([tsotnet](https://github.com/tsotnet))

## [v0.15.1](https://github.com/feast-dev/feast/tree/v0.15.1) (2021-11-13)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.15.0...v0.15.1)

**Fixed bugs:**

- Add missing comma in setup.py [\#2031](https://github.com/feast-dev/feast/pull/2031) ([achals](https://github.com/achals))
- Correct cleanup after usage e2e tests [\#2015](https://github.com/feast-dev/feast/pull/2015) ([pyalex](https://github.com/pyalex))
- Change Environment timestamps to be in UTC [\#2007](https://github.com/feast-dev/feast/pull/2007) ([felixwang9817](https://github.com/felixwang9817))
- ODFV UDFs should handle list types [\#2002](https://github.com/feast-dev/feast/pull/2002) ([Agent007](https://github.com/Agent007))

**Merged pull requests:**

- Remove unsupported java parts [\#2029](https://github.com/feast-dev/feast/pull/2029) ([pyalex](https://github.com/pyalex))
- Fix checked out branch for PR docker image build workflow [\#2018](https://github.com/feast-dev/feast/pull/2018) ([tsotnet](https://github.com/tsotnet))
- Remove duplicates in setup.py and run rudimentary verifications [\#2016](https://github.com/feast-dev/feast/pull/2016) ([achals](https://github.com/achals))
- Upload feature server docker image to ECR on approved PRs [\#2014](https://github.com/feast-dev/feast/pull/2014) ([tsotnet](https://github.com/tsotnet))
- Add integration tests for AWS Lambda feature server [\#2001](https://github.com/feast-dev/feast/pull/2001) ([tsotnet](https://github.com/tsotnet))
- Moving Feast Java back into main repo under java/ package [\#1997](https://github.com/feast-dev/feast/pull/1997) ([adchia](https://github.com/adchia))

## [v0.15.0](https://github.com/feast-dev/feast/tree/v0.15.0) (2021-11-08)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.14.1...v0.15.0)

**Implemented enhancements:**

- Adding stream ingestion alpha documentation [\#2005](https://github.com/feast-dev/feast/pull/2005) ([adchia](https://github.com/adchia))
- Feature transformation server docker image [\#1972](https://github.com/feast-dev/feast/pull/1972) ([felixwang9817](https://github.com/felixwang9817))
- eventtime check before writing features, use pipelines, ttl [\#1961](https://github.com/feast-dev/feast/pull/1961) ([vas28r13](https://github.com/vas28r13))
- Plugin repo universal tests [\#1946](https://github.com/feast-dev/feast/pull/1946) ([felixwang9817](https://github.com/felixwang9817))
- direct data ingestion into Online store [\#1939](https://github.com/feast-dev/feast/pull/1939) ([vas28r13](https://github.com/vas28r13))
- Add an interface for TransformationService and a basic implementation [\#1932](https://github.com/feast-dev/feast/pull/1932) ([achals](https://github.com/achals))
- Allows registering of features in request data as RequestFeatureView. Refactors common logic into a BaseFeatureView class [\#1931](https://github.com/feast-dev/feast/pull/1931) ([adchia](https://github.com/adchia))
- Add final\_output\_feature\_names in Query context to avoid SELECT \* EXCEPT [\#1911](https://github.com/feast-dev/feast/pull/1911) ([MattDelac](https://github.com/MattDelac))
- Add Dockerfile for GCP CloudRun FeatureServer [\#1887](https://github.com/feast-dev/feast/pull/1887) ([judahrand](https://github.com/judahrand))

**Fixed bugs:**

- feast=0.14.0 `query_generator()` unecessary used twice [\#1978](https://github.com/feast-dev/feast/issues/1978)
- get\_online\_features on demand transform bug fixes + local integration test mode [\#2004](https://github.com/feast-dev/feast/pull/2004) ([adchia](https://github.com/adchia))
- Always pass full and partial feature names to ODFV [\#2003](https://github.com/feast-dev/feast/pull/2003) ([judahrand](https://github.com/judahrand))
- Update bq\_to\_feast\_value\_type with BOOLEAN type as a legacy sql data type [\#1996](https://github.com/feast-dev/feast/pull/1996) ([mavysavydav](https://github.com/mavysavydav))
- Fix bug where using some Pandas dtypes in the output of an ODFV fails [\#1994](https://github.com/feast-dev/feast/pull/1994) ([judahrand](https://github.com/judahrand))
- Fix duplicate update infra [\#1990](https://github.com/feast-dev/feast/pull/1990) ([felixwang9817](https://github.com/felixwang9817))
- Improve performance of \_convert\_arrow\_to\_proto [\#1984](https://github.com/feast-dev/feast/pull/1984) ([nossrannug](https://github.com/nossrannug))
- Fix duplicate upload entity [\#1981](https://github.com/feast-dev/feast/pull/1981) ([achals](https://github.com/achals))
- fix redis cluster materialization [\#1968](https://github.com/feast-dev/feast/pull/1968) ([qooba](https://github.com/qooba))
- Allow plugin repos to actually overwrite repo configs [\#1966](https://github.com/feast-dev/feast/pull/1966) ([felixwang9817](https://github.com/felixwang9817))
- Delete keys from Redis when tearing down online store [\#1965](https://github.com/feast-dev/feast/pull/1965) ([achals](https://github.com/achals))
- Fix issues with lint test and upgrade pip version [\#1964](https://github.com/feast-dev/feast/pull/1964) ([felixwang9817](https://github.com/felixwang9817))
- Move IntegrationTestRepoConfig class to another module [\#1962](https://github.com/feast-dev/feast/pull/1962) ([felixwang9817](https://github.com/felixwang9817))
- Solve package conflict in \[gcp\] and \[ci\] [\#1955](https://github.com/feast-dev/feast/pull/1955) ([ysk24ok](https://github.com/ysk24ok))
- Remove some paths from unit test cache [\#1944](https://github.com/feast-dev/feast/pull/1944) ([achals](https://github.com/achals))
- Fix bug in feast alpha enable CLI command [\#1940](https://github.com/feast-dev/feast/pull/1940) ([felixwang9817](https://github.com/felixwang9817))
- Fix conditional statements for if OnDemandFVs exist [\#1937](https://github.com/feast-dev/feast/pull/1937) ([codyjlin](https://github.com/codyjlin))
- Fix \_\_getitem\_\_ return value for feature view and on-demand feature view [\#1936](https://github.com/feast-dev/feast/pull/1936) ([mavysavydav](https://github.com/mavysavydav))
- Corrected setup.py BigQuery version that's needed for a contributor's merged PR 1844 [\#1934](https://github.com/feast-dev/feast/pull/1934) ([mavysavydav](https://github.com/mavysavydav))

**Merged pull requests:**

- Fix protobuf version conflict in \[gcp\] and \[ci\] packages [\#1992](https://github.com/feast-dev/feast/pull/1992) ([ysk24ok](https://github.com/ysk24ok))
- Improve aws lambda deployment \(logging, idempotency, etc\) [\#1985](https://github.com/feast-dev/feast/pull/1985) ([tsotnet](https://github.com/tsotnet))
- Extend context for usage statistics collection & add latencies for performance analysis [\#1983](https://github.com/feast-dev/feast/pull/1983) ([pyalex](https://github.com/pyalex))
- Update CHANGELOG for Feast v0.14.1 [\#1982](https://github.com/feast-dev/feast/pull/1982) ([felixwang9817](https://github.com/felixwang9817))
- Document AWS Lambda permissions [\#1970](https://github.com/feast-dev/feast/pull/1970) ([tsotnet](https://github.com/tsotnet))
- Update online store helper docstring [\#1957](https://github.com/feast-dev/feast/pull/1957) ([amommendes](https://github.com/amommendes))
- Add public docs for entity aliasing [\#1951](https://github.com/feast-dev/feast/pull/1951) ([codyjlin](https://github.com/codyjlin))
- Updating roadmap + hero image [\#1950](https://github.com/feast-dev/feast/pull/1950) ([adchia](https://github.com/adchia))
- Add David and Matt as approvers as well [\#1943](https://github.com/feast-dev/feast/pull/1943) ([achals](https://github.com/achals))
- Add David and Matt as reviewers, and add actions for issue/PR assignment [\#1942](https://github.com/feast-dev/feast/pull/1942) ([achals](https://github.com/achals))
- Simplify BigQuery load jobs [\#1935](https://github.com/feast-dev/feast/pull/1935) ([judahrand](https://github.com/judahrand))

## [v0.14.1](https://github.com/feast-dev/feast/tree/v0.14.1) (2021-10-28)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.14.0...v0.14.1)

**Fixed bugs:**

- Fix duplicate upload entity [\#1981](https://github.com/feast-dev/feast/pull/1981) ([achals](https://github.com/achals))
- Fix bug in feast alpha enable CLI command [\#1940](https://github.com/feast-dev/feast/pull/1940) ([felixwang9817](https://github.com/felixwang9817))
- Fix conditional statements for if OnDemandFVs exist [\#1937](https://github.com/feast-dev/feast/pull/1937) ([codyjlin](https://github.com/codyjlin))
- Fix \_\_getitem\_\_ return value for feature view and on-demand feature view [\#1936](https://github.com/feast-dev/feast/pull/1936) ([mavysavydav](https://github.com/mavysavydav))
- Corrected setup.py BigQuery version that's needed for a contributor's merged PR 1844 [\#1934](https://github.com/feast-dev/feast/pull/1934) ([mavysavydav](https://github.com/mavysavydav))

**Merged pull requests:**

- Updating roadmap + hero image [\#1950](https://github.com/feast-dev/feast/pull/1950) ([adchia](https://github.com/adchia))
- Simplify BigQuery load jobs [\#1935](https://github.com/feast-dev/feast/pull/1935) ([judahrand](https://github.com/judahrand))

## [v0.14.0](https://github.com/feast-dev/feast/tree/v0.14.0) (2021-10-08)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.13.0...v0.14.0)

**Implemented enhancements:**

- Changed FVProjection 'name\_to\_use' field to 'name\_alias' and changed '.set\_projection' in FeatureView to ".with\_projection". Also adjustments for some edge cases [\#1929](https://github.com/feast-dev/feast/pull/1929) ([mavysavydav](https://github.com/mavysavydav))
- Make serverless alpha feature [\#1928](https://github.com/feast-dev/feast/pull/1928) ([felixwang9817](https://github.com/felixwang9817))
- Feast endpoint [\#1927](https://github.com/feast-dev/feast/pull/1927) ([felixwang9817](https://github.com/felixwang9817))
- Add location to BigQueryOfflineStoreConfig [\#1921](https://github.com/feast-dev/feast/pull/1921) ([loftiskg](https://github.com/loftiskg))
- Create & teardown Lambda & API Gateway resources for serverless feature server [\#1900](https://github.com/feast-dev/feast/pull/1900) ([tsotnet](https://github.com/tsotnet))
- Hide FeatureViewProjections from user interface & have FeatureViews carry FVProjections that carries the modified info of the FeatureView [\#1899](https://github.com/feast-dev/feast/pull/1899) ([mavysavydav](https://github.com/mavysavydav))
- Upload docker image to ECR during feast apply [\#1877](https://github.com/feast-dev/feast/pull/1877) ([felixwang9817](https://github.com/felixwang9817))
- Added .with\_name method in FeatureView/OnDemandFeatureView classes for name aliasing. FeatureViewProjection will hold this information [\#1872](https://github.com/feast-dev/feast/pull/1872) ([mavysavydav](https://github.com/mavysavydav))

**Fixed bugs:**

- Update makefile to use pip installed dependencies [\#1920](https://github.com/feast-dev/feast/pull/1920) ([loftiskg](https://github.com/loftiskg))
- Delete tables [\#1916](https://github.com/feast-dev/feast/pull/1916) ([felixwang9817](https://github.com/felixwang9817))
- Set a 5 minute limit for redshift statement execution [\#1915](https://github.com/feast-dev/feast/pull/1915) ([achals](https://github.com/achals))
- Use set when parsing repos to prevent duplicates [\#1913](https://github.com/feast-dev/feast/pull/1913) ([achals](https://github.com/achals))
- resolve environment variables in repo config [\#1909](https://github.com/feast-dev/feast/pull/1909) ([samuel100](https://github.com/samuel100))
- Respect specified ValueTypes for features during materialization [\#1906](https://github.com/feast-dev/feast/pull/1906) ([Agent007](https://github.com/Agent007))
- Fix issue with feature views being detected as duplicated when imported [\#1905](https://github.com/feast-dev/feast/pull/1905) ([achals](https://github.com/achals))
- Use contextvars to maintain a call stack during the usage calls [\#1882](https://github.com/feast-dev/feast/pull/1882) ([achals](https://github.com/achals))

**Merged pull requests:**

- Update concepts/README.md [\#1926](https://github.com/feast-dev/feast/pull/1926) ([ysk24ok](https://github.com/ysk24ok))
- Add CI for feature server Docker image [\#1925](https://github.com/feast-dev/feast/pull/1925) ([felixwang9817](https://github.com/felixwang9817))
- cache provider in feature store instance [\#1924](https://github.com/feast-dev/feast/pull/1924) ([DvirDukhan](https://github.com/DvirDukhan))
- Refactor logging and error messages in serverless [\#1923](https://github.com/feast-dev/feast/pull/1923) ([felixwang9817](https://github.com/felixwang9817))
- Add a caching step to our github actions [\#1919](https://github.com/feast-dev/feast/pull/1919) ([achals](https://github.com/achals))
- Add provider, offline store, online store, registry to RTD [\#1918](https://github.com/feast-dev/feast/pull/1918) ([felixwang9817](https://github.com/felixwang9817))
- Cleanup tests [\#1901](https://github.com/feast-dev/feast/pull/1901) ([felixwang9817](https://github.com/felixwang9817))

## [v0.13.0](https://github.com/feast-dev/feast/tree/v0.13.0) (2021-09-22)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.12.1...v0.13.0)

**Breaking changes:**

- Enforce case-insensitively unique feature view names [\#1835](https://github.com/feast-dev/feast/pull/1835) ([codyjlin](https://github.com/codyjlin))
- Add init to Provider contract [\#1796](https://github.com/feast-dev/feast/pull/1796) ([woop](https://github.com/woop))

**Implemented enhancements:**

- Add on demand feature view experimental docs [\#1880](https://github.com/feast-dev/feast/pull/1880) ([adchia](https://github.com/adchia))
- Adding telemetry for on demand feature views and making existing usage calls async [\#1873](https://github.com/feast-dev/feast/pull/1873) ([adchia](https://github.com/adchia))
- Read registry & config from env variables in AWS Lambda feature server [\#1870](https://github.com/feast-dev/feast/pull/1870) ([tsotnet](https://github.com/tsotnet))
- Add feature server configuration for AWS lambda [\#1865](https://github.com/feast-dev/feast/pull/1865) ([felixwang9817](https://github.com/felixwang9817))
- Add MVP support for on demand transforms for AWS to\_s3 and to\_redshift [\#1856](https://github.com/feast-dev/feast/pull/1856) ([adchia](https://github.com/adchia))
- Add MVP support for on demand transforms for bigquery [\#1855](https://github.com/feast-dev/feast/pull/1855) ([adchia](https://github.com/adchia))
- Add arrow support for on demand feature views [\#1853](https://github.com/feast-dev/feast/pull/1853) ([adchia](https://github.com/adchia))
- Support adding request data in on demand transforms [\#1851](https://github.com/feast-dev/feast/pull/1851) ([adchia](https://github.com/adchia))
- Support on demand feature views in feature services [\#1849](https://github.com/feast-dev/feast/pull/1849) ([achals](https://github.com/achals))
- Infer features for on demand feature views, support multiple output features [\#1845](https://github.com/feast-dev/feast/pull/1845) ([achals](https://github.com/achals))
- Add Registry and CLI operations for on demand feature views [\#1828](https://github.com/feast-dev/feast/pull/1828) ([achals](https://github.com/achals))
- Implementing initial on demand transforms for historical retrieval to\_df [\#1824](https://github.com/feast-dev/feast/pull/1824) ([adchia](https://github.com/adchia))
- Registry store plugin [\#1812](https://github.com/feast-dev/feast/pull/1812) ([DvirDukhan](https://github.com/DvirDukhan))
- Enable entityless featureviews [\#1804](https://github.com/feast-dev/feast/pull/1804) ([codyjlin](https://github.com/codyjlin))
- Initial scaffolding for on demand feature view [\#1803](https://github.com/feast-dev/feast/pull/1803) ([adchia](https://github.com/adchia))
- Add s3 support \(with custom endpoints\) [\#1789](https://github.com/feast-dev/feast/pull/1789) ([woop](https://github.com/woop))
- Local feature server implementation \(HTTP endpoint\) [\#1780](https://github.com/feast-dev/feast/pull/1780) ([tsotnet](https://github.com/tsotnet))

**Fixed bugs:**

- Fixing odfv cli group description [\#1890](https://github.com/feast-dev/feast/pull/1890) ([adchia](https://github.com/adchia))
- Fix list feature format for BigQuery offline datasources.  [\#1889](https://github.com/feast-dev/feast/pull/1889) ([judahrand](https://github.com/judahrand))
- Add `dill` to main dependencies [\#1886](https://github.com/feast-dev/feast/pull/1886) ([judahrand](https://github.com/judahrand))
- Fix pytest\_collection\_modifyitems to select benchmark tests only [\#1874](https://github.com/feast-dev/feast/pull/1874) ([achals](https://github.com/achals))
- Add support for multiple entities in Redshift [\#1850](https://github.com/feast-dev/feast/pull/1850) ([felixwang9817](https://github.com/felixwang9817))
- Move apply\(dummy\_entity\) to apply time to ensure it persists in FeatureStore [\#1848](https://github.com/feast-dev/feast/pull/1848) ([codyjlin](https://github.com/codyjlin))
- Add schema parameter to RedshiftSource [\#1847](https://github.com/feast-dev/feast/pull/1847) ([felixwang9817](https://github.com/felixwang9817))
- Pass bigquery job object to get\_job [\#1844](https://github.com/feast-dev/feast/pull/1844) ([LarsKlingen](https://github.com/LarsKlingen))
- Simplify \_python\_value\_to\_proto\_value by looking up values in a dict [\#1837](https://github.com/feast-dev/feast/pull/1837) ([achals](https://github.com/achals))
- Update historical retrieval integration test for on demand feature views [\#1836](https://github.com/feast-dev/feast/pull/1836) ([achals](https://github.com/achals))
- Fix flaky connection to redshift data API [\#1834](https://github.com/feast-dev/feast/pull/1834) ([achals](https://github.com/achals))
- Init registry during create\_test\_environment [\#1829](https://github.com/feast-dev/feast/pull/1829) ([achals](https://github.com/achals))
- Randomly generating new BQ dataset for offline\_online\_store\_consistency test [\#1818](https://github.com/feast-dev/feast/pull/1818) ([adchia](https://github.com/adchia))
- Ensure docstring tests always teardown [\#1817](https://github.com/feast-dev/feast/pull/1817) ([felixwang9817](https://github.com/felixwang9817))
- Use get\_multi instead of get for datastore reads [\#1814](https://github.com/feast-dev/feast/pull/1814) ([achals](https://github.com/achals))
- Fix Redshift query for external tables [\#1810](https://github.com/feast-dev/feast/pull/1810) ([woop](https://github.com/woop))
- Use a random dataset and table name for simple\_bq\_source [\#1801](https://github.com/feast-dev/feast/pull/1801) ([achals](https://github.com/achals))
- Refactor Environment class and DataSourceCreator API, and use fixtures for datasets and data sources [\#1790](https://github.com/feast-dev/feast/pull/1790) ([achals](https://github.com/achals))
- Fix get\_online\_features telemetry to only log every 10000 times [\#1786](https://github.com/feast-dev/feast/pull/1786) ([felixwang9817](https://github.com/felixwang9817))
- Add a description field the Feature Service class and proto [\#1771](https://github.com/feast-dev/feast/pull/1771) ([achals](https://github.com/achals))
- Validate project name upon feast.apply [\#1766](https://github.com/feast-dev/feast/pull/1766) ([tedhtchang](https://github.com/tedhtchang))
- Fix BQ historical retrieval with rows that got backfilled [\#1744](https://github.com/feast-dev/feast/pull/1744) ([MattDelac](https://github.com/MattDelac))
- Handle case where`_LIST` type is empty [\#1703](https://github.com/feast-dev/feast/pull/1703) ([judahrand](https://github.com/judahrand))

**Merged pull requests:**

- Add `ValueType.NULL` [\#1893](https://github.com/feast-dev/feast/pull/1893) ([judahrand](https://github.com/judahrand))
- Adding more details to the CONTRIBUTING.md [\#1888](https://github.com/feast-dev/feast/pull/1888) ([adchia](https://github.com/adchia))
- Parse BQ `DATETIME` and `TIMESTAMP` [\#1885](https://github.com/feast-dev/feast/pull/1885) ([judahrand](https://github.com/judahrand))
- Add durations to list the slowest tests [\#1881](https://github.com/feast-dev/feast/pull/1881) ([achals](https://github.com/achals))
- Upload benchmark information to S3 after integration test runs [\#1878](https://github.com/feast-dev/feast/pull/1878) ([achals](https://github.com/achals))
- Refactor providers to remove duplicate implementations [\#1876](https://github.com/feast-dev/feast/pull/1876) ([achals](https://github.com/achals))
- Add Felix & Danny to code owners file [\#1869](https://github.com/feast-dev/feast/pull/1869) ([tsotnet](https://github.com/tsotnet))
- Initial docker image for aws lambda feature server [\#1866](https://github.com/feast-dev/feast/pull/1866) ([tsotnet](https://github.com/tsotnet))
- Add flags file to include experimental flags and test/usage flags [\#1864](https://github.com/feast-dev/feast/pull/1864) ([adchia](https://github.com/adchia))
- Hookup pytest-benchmark to online retreival [\#1858](https://github.com/feast-dev/feast/pull/1858) ([achals](https://github.com/achals))
- Add feature server docs & small changes in local server [\#1852](https://github.com/feast-dev/feast/pull/1852) ([tsotnet](https://github.com/tsotnet))
- Add roadmap to README.md [\#1843](https://github.com/feast-dev/feast/pull/1843) ([woop](https://github.com/woop))
- Enable the types test to run on all compatible environments [\#1840](https://github.com/feast-dev/feast/pull/1840) ([adchia](https://github.com/adchia))
- Update reviewers/approvers to include Danny/Felix [\#1833](https://github.com/feast-dev/feast/pull/1833) ([adchia](https://github.com/adchia))
- Fix wrong links in README [\#1832](https://github.com/feast-dev/feast/pull/1832) ([baineng](https://github.com/baineng))
- Remove older offline/online consistency tests [\#1831](https://github.com/feast-dev/feast/pull/1831) ([achals](https://github.com/achals))
- Replace individual cli tests with parametrized tests [\#1830](https://github.com/feast-dev/feast/pull/1830) ([achals](https://github.com/achals))
- Reducing wait interval for BQ integration tests [\#1827](https://github.com/feast-dev/feast/pull/1827) ([adchia](https://github.com/adchia))
- Reducing size of universal repo to decrease integration test time [\#1826](https://github.com/feast-dev/feast/pull/1826) ([adchia](https://github.com/adchia))
- Refactor the datastore online\_read method to be slightly more efficient [\#1819](https://github.com/feast-dev/feast/pull/1819) ([achals](https://github.com/achals))
- Remove old doc [\#1815](https://github.com/feast-dev/feast/pull/1815) ([achals](https://github.com/achals))
- Rename telemetry to usage [\#1800](https://github.com/feast-dev/feast/pull/1800) ([felixwang9817](https://github.com/felixwang9817))
- Updating quickstart colab to explain more concepts and highlight value prop of Feast [\#1799](https://github.com/feast-dev/feast/pull/1799) ([adchia](https://github.com/adchia))
- Fix Azure Terraform installation. [\#1793](https://github.com/feast-dev/feast/pull/1793) ([mmurdoch](https://github.com/mmurdoch))
- Disable integration test reruns to identify flaky tests [\#1787](https://github.com/feast-dev/feast/pull/1787) ([achals](https://github.com/achals))
- Rerun failed python integration tests [\#1785](https://github.com/feast-dev/feast/pull/1785) ([achals](https://github.com/achals))
- Add Redis to the universal integration tests [\#1784](https://github.com/feast-dev/feast/pull/1784) ([achals](https://github.com/achals))
- Add online feature retrieval integration test using the universal repo [\#1783](https://github.com/feast-dev/feast/pull/1783) ([achals](https://github.com/achals))
- Fix wrong description in README.md [\#1779](https://github.com/feast-dev/feast/pull/1779) ([WingCode](https://github.com/WingCode))
- Clean up docstring tests [\#1778](https://github.com/feast-dev/feast/pull/1778) ([felixwang9817](https://github.com/felixwang9817))
- Add offline retrival integration tests using the universal repo [\#1769](https://github.com/feast-dev/feast/pull/1769) ([achals](https://github.com/achals))
- Adding initial type support related tests for BQ [\#1768](https://github.com/feast-dev/feast/pull/1768) ([adchia](https://github.com/adchia))
- Add release-patch script [\#1554](https://github.com/feast-dev/feast/pull/1554) ([jklegar](https://github.com/jklegar))

## [v0.12.1](https://github.com/feast-dev/feast/tree/v0.12.1) (2021-08-20)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.12.0...v0.12.1)

**Fixed bugs:**

- Fix get\_online\_features telemetry to only log every 10000 times [\#1786](https://github.com/feast-dev/feast/pull/1786) ([felixwang9817](https://github.com/felixwang9817))

## [v0.12.0](https://github.com/feast-dev/feast/tree/v0.12.0) (2021-08-05)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.11.0...v0.12.0)

**Breaking changes:**

- Set default feature naming to not include feature view name. Add option to include feature view name in feature naming. [\#1641](https://github.com/feast-dev/feast/pull/1641) ([Mwad22](https://github.com/Mwad22))

**Implemented enhancements:**

- AWS Template improvements \(input prompt for configs, default to Redshift\) [\#1731](https://github.com/feast-dev/feast/pull/1731) ([tsotnet](https://github.com/tsotnet))
- Clean up uploaded entities in Redshift offline store [\#1730](https://github.com/feast-dev/feast/pull/1730) ([tsotnet](https://github.com/tsotnet))
- Implement Redshift historical retrieval [\#1720](https://github.com/feast-dev/feast/pull/1720) ([tsotnet](https://github.com/tsotnet))
- Add custom data sources [\#1713](https://github.com/feast-dev/feast/pull/1713) ([achals](https://github.com/achals))
- Added --skip-source-validation flag to feast apply [\#1702](https://github.com/feast-dev/feast/pull/1702) ([mavysavydav](https://github.com/mavysavydav))
- Allow specifying FeatureServices in FeatureStore methods [\#1691](https://github.com/feast-dev/feast/pull/1691) ([achals](https://github.com/achals))
- Implement materialization for RedshiftOfflineStore & RedshiftRetrievalJob [\#1680](https://github.com/feast-dev/feast/pull/1680) ([tsotnet](https://github.com/tsotnet))
- Add FeatureService proto definition [\#1676](https://github.com/feast-dev/feast/pull/1676) ([achals](https://github.com/achals))
- Add RedshiftDataSource [\#1669](https://github.com/feast-dev/feast/pull/1669) ([tsotnet](https://github.com/tsotnet))
- Add streaming sources to the FeatureView API [\#1664](https://github.com/feast-dev/feast/pull/1664) ([achals](https://github.com/achals))
- Add to\_table\(\) to RetrievalJob object [\#1663](https://github.com/feast-dev/feast/pull/1663) ([MattDelac](https://github.com/MattDelac))
- Provide the user with more options for setting the to\_bigquery config [\#1661](https://github.com/feast-dev/feast/pull/1661) ([codyjlin](https://github.com/codyjlin))

**Fixed bugs:**

- Fix `feast apply` bugs [\#1754](https://github.com/feast-dev/feast/pull/1754) ([tsotnet](https://github.com/tsotnet))
- Teardown integration tests resources for aws [\#1740](https://github.com/feast-dev/feast/pull/1740) ([achals](https://github.com/achals))
- Fix GCS version [\#1732](https://github.com/feast-dev/feast/pull/1732) ([potatochip](https://github.com/potatochip))
- Fix unit test warnings related to file\_url [\#1726](https://github.com/feast-dev/feast/pull/1726) ([tedhtchang](https://github.com/tedhtchang))
- Refactor data source classes to fix import issues [\#1723](https://github.com/feast-dev/feast/pull/1723) ([achals](https://github.com/achals))
- Append ns time and random integer to redshift test tables [\#1716](https://github.com/feast-dev/feast/pull/1716) ([achals](https://github.com/achals))
- Add randomness to bigquery table name [\#1711](https://github.com/feast-dev/feast/pull/1711) ([felixwang9817](https://github.com/felixwang9817))
- Fix dry\_run bug that was making to\_bigquery hang indefinitely [\#1706](https://github.com/feast-dev/feast/pull/1706) ([codyjlin](https://github.com/codyjlin))
- Stringify WhichOneof to make mypy happy [\#1705](https://github.com/feast-dev/feast/pull/1705) ([achals](https://github.com/achals))
- Update redis options parsing  [\#1704](https://github.com/feast-dev/feast/pull/1704) ([DvirDukhan](https://github.com/DvirDukhan))
- Cancel BigQuery job if block\_until\_done call times out or is interrupted [\#1699](https://github.com/feast-dev/feast/pull/1699) ([codyjlin](https://github.com/codyjlin))
- Teardown infrastructure after integration tests [\#1697](https://github.com/feast-dev/feast/pull/1697) ([achals](https://github.com/achals))
- Fix unit tests that got broken by Pandas 1.3.0 release [\#1683](https://github.com/feast-dev/feast/pull/1683) ([tsotnet](https://github.com/tsotnet))
- Remove default list from the FeatureView constructor [\#1679](https://github.com/feast-dev/feast/pull/1679) ([achals](https://github.com/achals))
- BQ exception should be raised first before we check the timedout [\#1675](https://github.com/feast-dev/feast/pull/1675) ([MattDelac](https://github.com/MattDelac))
- Allow strings for online/offline store instead of dicts [\#1673](https://github.com/feast-dev/feast/pull/1673) ([achals](https://github.com/achals))
- Cancel BigQuery job if timeout hits [\#1672](https://github.com/feast-dev/feast/pull/1672) ([MattDelac](https://github.com/MattDelac))
- Make sure FeatureViews with same name can not be applied at the same … [\#1651](https://github.com/feast-dev/feast/pull/1651) ([tedhtchang](https://github.com/tedhtchang))

**Merged pull requests:**

- Add AWS docs in summary.md [\#1761](https://github.com/feast-dev/feast/pull/1761) ([tsotnet](https://github.com/tsotnet))
- Document permissions for AWS \(DynamoDB & Redshift\) [\#1753](https://github.com/feast-dev/feast/pull/1753) ([tsotnet](https://github.com/tsotnet))
- Adding small note for project naming convention [\#1752](https://github.com/feast-dev/feast/pull/1752) ([codyjlin](https://github.com/codyjlin))
- Fix warning in FeatureView.from\_proto [\#1751](https://github.com/feast-dev/feast/pull/1751) ([tsotnet](https://github.com/tsotnet))
- Add Feature Service to the concepts group [\#1750](https://github.com/feast-dev/feast/pull/1750) ([achals](https://github.com/achals))
- Docstring tests [\#1749](https://github.com/feast-dev/feast/pull/1749) ([felixwang9817](https://github.com/felixwang9817))
- Document how pandas deals with missing values [\#1748](https://github.com/feast-dev/feast/pull/1748) ([achals](https://github.com/achals))
- Restore feature refs [\#1746](https://github.com/feast-dev/feast/pull/1746) ([felixwang9817](https://github.com/felixwang9817))
- Updating CLI apply to use FeatureStore [\#1745](https://github.com/feast-dev/feast/pull/1745) ([adchia](https://github.com/adchia))
- Delete old code [\#1743](https://github.com/feast-dev/feast/pull/1743) ([felixwang9817](https://github.com/felixwang9817))
- Bump dependency on pyyaml [\#1742](https://github.com/feast-dev/feast/pull/1742) ([achals](https://github.com/achals))
- Docstrings [\#1739](https://github.com/feast-dev/feast/pull/1739) ([felixwang9817](https://github.com/felixwang9817))
- Add the foundation of the universal feature repo and a test that uses it [\#1734](https://github.com/feast-dev/feast/pull/1734) ([achals](https://github.com/achals))
- Add AWS documentation \(DynamoDB, Redshift\) [\#1733](https://github.com/feast-dev/feast/pull/1733) ([tsotnet](https://github.com/tsotnet))
- Change internal references from input to batch\_source [\#1729](https://github.com/feast-dev/feast/pull/1729) ([felixwang9817](https://github.com/felixwang9817))
- Refactor tests into new directory layout [\#1725](https://github.com/feast-dev/feast/pull/1725) ([achals](https://github.com/achals))
- Registry teardown [\#1718](https://github.com/feast-dev/feast/pull/1718) ([felixwang9817](https://github.com/felixwang9817))
- Redirect telemetry to usage [\#1717](https://github.com/feast-dev/feast/pull/1717) ([felixwang9817](https://github.com/felixwang9817))
- Link to offline and online store specs in docs summary [\#1715](https://github.com/feast-dev/feast/pull/1715) ([achals](https://github.com/achals))
- Avoid skewed join between entity\_df & feature views [\#1712](https://github.com/feast-dev/feast/pull/1712) ([MattDelac](https://github.com/MattDelac))
- Remove type comments [\#1710](https://github.com/feast-dev/feast/pull/1710) ([achals](https://github.com/achals))
- Increase efficiency of Registry updates [\#1698](https://github.com/feast-dev/feast/pull/1698) ([felixwang9817](https://github.com/felixwang9817))
- Parallelize integration tests [\#1684](https://github.com/feast-dev/feast/pull/1684) ([tsotnet](https://github.com/tsotnet))
- Remove debug logging  [\#1678](https://github.com/feast-dev/feast/pull/1678) ([charliec443](https://github.com/charliec443))
- Docs: Fix Feature References example [\#1674](https://github.com/feast-dev/feast/pull/1674) ([GregKuhlmann](https://github.com/GregKuhlmann))
- Rename to\_table to to\_arrow [\#1671](https://github.com/feast-dev/feast/pull/1671) ([MattDelac](https://github.com/MattDelac))
- Small reference documentation update [\#1668](https://github.com/feast-dev/feast/pull/1668) ([nels](https://github.com/nels))
- Grouped inferencing statements together in apply methods for easier readability [\#1667](https://github.com/feast-dev/feast/pull/1667) ([mavysavydav](https://github.com/mavysavydav))
- Infer min and max timestamps from entity\_df to limit data read from BQ source [\#1665](https://github.com/feast-dev/feast/pull/1665) ([Mwad22](https://github.com/Mwad22))
- Rename telemetry to usage [\#1660](https://github.com/feast-dev/feast/pull/1660) ([tsotnet](https://github.com/tsotnet))
- Update charts README [\#1659](https://github.com/feast-dev/feast/pull/1659) ([szalai1](https://github.com/szalai1))

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