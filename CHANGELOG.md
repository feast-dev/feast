# Changelog

# [0.51.0](https://github.com/feast-dev/feast/compare/v0.50.0...v0.51.0) (2025-07-21)


### Bug Fixes

* FeatureView serialization with cycle detection ([#5502](https://github.com/feast-dev/feast/issues/5502)) ([f287ca5](https://github.com/feast-dev/feast/commit/f287ca532b907c43f74edc8236e2c844412c8b7e))
* Fix current version in publish workflow ([#5499](https://github.com/feast-dev/feast/issues/5499)) ([0af6e94](https://github.com/feast-dev/feast/commit/0af6e94405c517bb6e2badee899fd1f0ad9db6a7))
* Fix NPM authentication ([#5506](https://github.com/feast-dev/feast/issues/5506)) ([9f85892](https://github.com/feast-dev/feast/commit/9f8589205702842772b7a8b350f24bbfb250f41f))
* Fix verify wheels workflow for macos14 ([#5486](https://github.com/feast-dev/feast/issues/5486)) ([07174cc](https://github.com/feast-dev/feast/commit/07174cc380dbfbc3863e8b9e374fe46c9c594406))
* Fixed error thrown for invalid project name on features api ([#5525](https://github.com/feast-dev/feast/issues/5525)) ([4a9a5d0](https://github.com/feast-dev/feast/commit/4a9a5d018c27720699dc2d1031cbe794e605fe30))
* Fixed ODFV on-write transformations ([271ef74](https://github.com/feast-dev/feast/commit/271ef7459347c266752557bb41043d585775f483))
* Move Install OS X dependencies before python setup ([#5488](https://github.com/feast-dev/feast/issues/5488)) ([35f211c](https://github.com/feast-dev/feast/commit/35f211c27fde88bf95f95300474b71b777a89975))
* Normalize current version by removing 'v' prefix if present ([#5500](https://github.com/feast-dev/feast/issues/5500)) ([43f3d52](https://github.com/feast-dev/feast/commit/43f3d525f0b1b82ea8ccc519bda2078b0e638d18))
* Skip macOS 14 with Python 3.10 due to gettext library ([#5490](https://github.com/feast-dev/feast/issues/5490)) ([41d4977](https://github.com/feast-dev/feast/commit/41d49772152f4bb4e900e6fa04f9f505e38a1008))
* Standalone Web UI Publish Workflow ([#5498](https://github.com/feast-dev/feast/issues/5498)) ([c47b134](https://github.com/feast-dev/feast/commit/c47b134c0ef3bbaed4502877c243924db7363404))


### Features

* Added endpoints to allow user to get data for all projects ([4e06965](https://github.com/feast-dev/feast/commit/4e06965a3cf7b37157e3155aa52551c63e7c2247))
* Added grpc and rest endpoint for features ([#5519](https://github.com/feast-dev/feast/issues/5519)) ([0a75696](https://github.com/feast-dev/feast/commit/0a756960b2aa3380ca9bc5406bfc41dbefd68d4c))
* Added relationship support to all API endpoints ([#5496](https://github.com/feast-dev/feast/issues/5496)) ([bea83e7](https://github.com/feast-dev/feast/commit/bea83e7e1f30cf81e2c29ec44ab2b97a65efd86c))
* Continue updating doc ([#5523](https://github.com/feast-dev/feast/issues/5523)) ([ea53b2b](https://github.com/feast-dev/feast/commit/ea53b2b159218cd50c79e776b64baa18d3b3c0b4))
* Hybrid offline store ([#5510](https://github.com/feast-dev/feast/issues/5510)) ([8f1af55](https://github.com/feast-dev/feast/commit/8f1af5594c8328353d5e53873655c00d91afbd10))
* Populate created and updated timestamp on data sources ([af3056b](https://github.com/feast-dev/feast/commit/af3056b8a528ebac9435d7c99caffa087c205323))
* Provide ready-to-use Python definitions in api ([37628d9](https://github.com/feast-dev/feast/commit/37628d995edcd9dac61339d7d5bbbfe8444737f4))
* Snowflake source. fetch MAX in a single query ([#5387](https://github.com/feast-dev/feast/issues/5387)) ([b49cea1](https://github.com/feast-dev/feast/commit/b49cea15d77fd74e698e74eecca8d017fe706407))
* Support compute engine to use multi feature views as source ([#5482](https://github.com/feast-dev/feast/issues/5482)) ([b9ac90b](https://github.com/feast-dev/feast/commit/b9ac90b7efbbe35029408f2ec63333c5651793d0))
* Support pagination and sorting on registry apis ([#5495](https://github.com/feast-dev/feast/issues/5495)) ([c4b6fbe](https://github.com/feast-dev/feast/commit/c4b6fbe4c3dd6b8a2b48d2d16bcff720660875a4))
* Update doc ([#5521](https://github.com/feast-dev/feast/issues/5521)) ([2808ce1](https://github.com/feast-dev/feast/commit/2808ce190658a55971fb919f4a8bb90b0e3c5003))

# [0.50.0](https://github.com/feast-dev/feast/compare/v0.49.0...v0.50.0) (2025-07-01)


### Bug Fixes

* Add asyncio to integration test ([#5418](https://github.com/feast-dev/feast/issues/5418)) ([6765515](https://github.com/feast-dev/feast/commit/67655154c618548ca3df6c49c95b0c695e19bfeb))
* Add clickhouse to OFFLINE_STORE_CLASS_FOR_TYPE map ([#5251](https://github.com/feast-dev/feast/issues/5251)) ([9ed2ffa](https://github.com/feast-dev/feast/commit/9ed2ffaeca7e42d5d3d0f7f5189f2f9249013eca))
* Add missing conn.commit() in SnowflakeOnlineStore.online_write_batch ([#5432](https://github.com/feast-dev/feast/issues/5432)) ([a83dd85](https://github.com/feast-dev/feast/commit/a83dd85f5db17ee96365e26fe8bae127007f37d1))
* Add transformers in required dependencies ([8cde460](https://github.com/feast-dev/feast/commit/8cde460491ee59a50a6949b9e2ec714eeeca32a5))
* Allow custom annotations on Operator installed objects ([#5339](https://github.com/feast-dev/feast/issues/5339)) ([44c7a76](https://github.com/feast-dev/feast/commit/44c7a76028530debd3c0cccb6467d741bd2c9765))
* Dask pulling of latest data ([#5229](https://github.com/feast-dev/feast/issues/5229)) ([571d81f](https://github.com/feast-dev/feast/commit/571d81f30578a4b60bb8dccbabb31e44df756328))
* **dask:** preserve remote URIs (e.g. s3://) in DaskOfflineStore path resolution ([2561cfc](https://github.com/feast-dev/feast/commit/2561cfc19447d843a97bbe15773b95a08920d581))
* Fix Event loop is closed error on dynamodb test ([#5480](https://github.com/feast-dev/feast/issues/5480)) ([fe0f671](https://github.com/feast-dev/feast/commit/fe0f6713abfee0bd13df754478bf875a2566052a))
* Fix lineage entity filtering ([#5321](https://github.com/feast-dev/feast/issues/5321)) ([0d05701](https://github.com/feast-dev/feast/commit/0d05701d2dda547af8be61b70cbfaa1d45bf08be))
* Fix list saved dataset api ([833696c](https://github.com/feast-dev/feast/commit/833696c79fa4067fdd4954c9b4189cbe0cf21261))
* Fix NumPy - PyArrow array type mapping in Trino offline store ([#5393](https://github.com/feast-dev/feast/issues/5393)) ([9ba9ded](https://github.com/feast-dev/feast/commit/9ba9ded702e70ab13b43eaa98ec19b4be7441587))
* Fix pandas 2.x compatibility issue of Trino offline store caused by removed Series.iteritems() method ([#5345](https://github.com/feast-dev/feast/issues/5345)) ([61e3e02](https://github.com/feast-dev/feast/commit/61e3e02c2114fde929417e3e8270935b7943f552))
* Fix polling mechanism for TestApplyAndMaterialize ([#5451](https://github.com/feast-dev/feast/issues/5451)) ([b512a74](https://github.com/feast-dev/feast/commit/b512a748f2304d1e2ca4c78f8750d7a44c3668e8))
* Fix remote rbac integration tests ([#5473](https://github.com/feast-dev/feast/issues/5473)) ([10879ec](https://github.com/feast-dev/feast/commit/10879ec5113dbd20a30be713d5f9a61ad8678768))
* Fix Trino offline store SQL in Jinja template ([#5346](https://github.com/feast-dev/feast/issues/5346)) ([648c53d](https://github.com/feast-dev/feast/commit/648c53dc64ad88077e49e56f0cfc70756d6a5824))
* Fixed CurlGeneratorTab github theme type ([#5425](https://github.com/feast-dev/feast/issues/5425)) ([5f15329](https://github.com/feast-dev/feast/commit/5f15329a03a4bc897c6aee306ef4506989768846))
* Increase the Operator Manager memory limits and requests ([#5441](https://github.com/feast-dev/feast/issues/5441)) ([6c94dbf](https://github.com/feast-dev/feast/commit/6c94dbfb97f121a2237216e7eb14a35272a151ed))
* Method signature for push_async is out of date ([#5413](https://github.com/feast-dev/feast/issues/5413)) ([28c3379](https://github.com/feast-dev/feast/commit/28c33793d214bda350b6ca058855dbf0726ca3b4)), closes [#5410](https://github.com/feast-dev/feast/issues/5410) [#006BB4](https://github.com/feast-dev/feast/issues/006BB4)
* Operator - support securityContext override at Pod level ([#5325](https://github.com/feast-dev/feast/issues/5325)) ([33ea0f5](https://github.com/feast-dev/feast/commit/33ea0f5aaa9c55eae9ff920b5b154e9681227a00))
* Pybuild-deps throws errors w/ latest pip version ([#5311](https://github.com/feast-dev/feast/issues/5311)) ([f2d6a67](https://github.com/feast-dev/feast/commit/f2d6a67b630cffbf43bc1f8495daa2431f307649))
* Reopen for integration test about add s3 storage-based registry store in Go feature server ([#5352](https://github.com/feast-dev/feast/issues/5352)) ([ef75f61](https://github.com/feast-dev/feast/commit/ef75f61936c86169e55a8c1a224b06b08ef79b96))
* resolve Python logger warnings ([#5361](https://github.com/feast-dev/feast/issues/5361)) ([37d5c19](https://github.com/feast-dev/feast/commit/37d5c196f678a90810aa98f3a3e5b1a3bffb7927))
* The ignore_paths not taking effect duration feast apply ([#5353](https://github.com/feast-dev/feast/issues/5353)) ([e4917ca](https://github.com/feast-dev/feast/commit/e4917ca80611ca3560a37ade1a80c41b0d1ea6f9))
* Update generate_answer function to provide correct parameter format to retrieve function ([dc5b2af](https://github.com/feast-dev/feast/commit/dc5b2af825ac33f58d0643cdcca0bf4261ec52a1))
* Update milvus connect function to work with remote instance ([#5382](https://github.com/feast-dev/feast/issues/5382)) ([7e5e7d5](https://github.com/feast-dev/feast/commit/7e5e7d5ae4de733c98b25e6dd552f706f94ee87a))
* Updating milvus connect function to work with remote instance ([#5401](https://github.com/feast-dev/feast/issues/5401)) ([b89fadd](https://github.com/feast-dev/feast/commit/b89faddf430826b72d47ad4015f021a5c065092d))
* Upperbound limit for protobuf generation ([#5309](https://github.com/feast-dev/feast/issues/5309)) ([a114aae](https://github.com/feast-dev/feast/commit/a114aae22c75cc8d1ea9664e0170ea49857a941e))


### Features

* Add CLI, SDK, and API documentation page to Feast UI ([#5337](https://github.com/feast-dev/feast/issues/5337)) ([203e888](https://github.com/feast-dev/feast/commit/203e8889d2d75080680491905f05fe200ba38f21))
* Add dark mode toggle to Feast UI ([#5314](https://github.com/feast-dev/feast/issues/5314)) ([ad02e46](https://github.com/feast-dev/feast/commit/ad02e460f23a19d6abab875cede6a06a9d2c1a27))
* Add data labeling tabs to UI ([#5410](https://github.com/feast-dev/feast/issues/5410)) ([389ceb7](https://github.com/feast-dev/feast/commit/389ceb7b2dd947ccd549ff5ecb792395c6971430)), closes [#006BB4](https://github.com/feast-dev/feast/issues/006BB4)
* Add Decimal to allowed python scalar types ([#5367](https://github.com/feast-dev/feast/issues/5367)) ([4777c03](https://github.com/feast-dev/feast/commit/4777c03dc2b877150bab96aac6fdc798a7a3d69d))
* Add feast rag retriver functionality ([#5405](https://github.com/feast-dev/feast/issues/5405)) ([0173033](https://github.com/feast-dev/feast/commit/0173033ca93d9fefd0fe1d8db5979a9090f56fb2))
* Add feature view curl generator ([#5415](https://github.com/feast-dev/feast/issues/5415)) ([7a5b48f](https://github.com/feast-dev/feast/commit/7a5b48f1a3a5f050010650b8084e86e0cf4ed58d))
* Add feature view lineage tab and filtering to home page lineage ([#5308](https://github.com/feast-dev/feast/issues/5308)) ([308255d](https://github.com/feast-dev/feast/commit/308255d0f1aaa4a94ee3271e764fc94b05818098))
* Add feature view tags to dynamo tags ([#5291](https://github.com/feast-dev/feast/issues/5291)) ([3a787ac](https://github.com/feast-dev/feast/commit/3a787acbe5c4e54a523b87740520c135894c7f0c))
* Add HybridOnlineStore for multi-backend online store routing ([#5423](https://github.com/feast-dev/feast/issues/5423)) ([ebd67d1](https://github.com/feast-dev/feast/commit/ebd67d1a46041310c5d3c5a1050061767e04e534))
* Add max_file_size to Snowflake config ([#5377](https://github.com/feast-dev/feast/issues/5377)) ([e8cdf5d](https://github.com/feast-dev/feast/commit/e8cdf5d12847d8cbc91027244415eee2db66e492))
* Add MCP (Model Context Protocol) support for Feast feature server ([#5406](https://github.com/feast-dev/feast/issues/5406)) ([de650de](https://github.com/feast-dev/feast/commit/de650def7b4e9b2e2b0532938fe72b7f0891b33d)), closes [#5398](https://github.com/feast-dev/feast/issues/5398) [#5382](https://github.com/feast-dev/feast/issues/5382) [#5389](https://github.com/feast-dev/feast/issues/5389) [#5401](https://github.com/feast-dev/feast/issues/5401)
* Add rag project to default dev UI ([#5323](https://github.com/feast-dev/feast/issues/5323)) ([3b3e1c8](https://github.com/feast-dev/feast/commit/3b3e1c8eb82bd1a2bfdf90060153bbcb2dbd5693))
* Add s3 storage-based registry store in Go feature server ([#5336](https://github.com/feast-dev/feast/issues/5336)) ([abe18df](https://github.com/feast-dev/feast/commit/abe18dfca23e95795af032b4852d93c313e0b526))
* Add support for data labeling in UI ([#5409](https://github.com/feast-dev/feast/issues/5409)) ([d183c4b](https://github.com/feast-dev/feast/commit/d183c4bac341aa78a75d76bfaa40730587b1b0b0)), closes [#27](https://github.com/feast-dev/feast/issues/27)
* Added Lineage APIs to get registry objects relationships ([#5472](https://github.com/feast-dev/feast/issues/5472)) ([be004ef](https://github.com/feast-dev/feast/commit/be004ef8e769652a6ffb82b70b8c002766036e3f))
* Added rest-apis serving option for registry server ([#5342](https://github.com/feast-dev/feast/issues/5342)) ([9740fd1](https://github.com/feast-dev/feast/commit/9740fd1ac0361b2089a715a132aa7b4ad079464b))
* Added torch.Tensor as option for online and offline retrieval ([#5381](https://github.com/feast-dev/feast/issues/5381)) ([0b4ae95](https://github.com/feast-dev/feast/commit/0b4ae957d131baf874065723305a039979c492d1))
* Adding feast delete to CLI ([#5344](https://github.com/feast-dev/feast/issues/5344)) ([19fe3ac](https://github.com/feast-dev/feast/commit/19fe3acdb8134500018ea517b90f5011ca4bae7f))
* Adding permissions to UI and refactoring some things ([#5320](https://github.com/feast-dev/feast/issues/5320)) ([6f1b0cc](https://github.com/feast-dev/feast/commit/6f1b0cc2812cddb3a7acc2e175c4359b18f0c3c2))
* Allow to set registry server rest/grpc mode in operator ([#5364](https://github.com/feast-dev/feast/issues/5364)) ([99afd6d](https://github.com/feast-dev/feast/commit/99afd6d2582af3989cbc9d68f1015a4d9701cd36))
* Allow to use env variable FEAST_FS_YAML_FILE_PATH and FEATURE_REPO_DIR ([#5420](https://github.com/feast-dev/feast/issues/5420)) ([6a1b33a](https://github.com/feast-dev/feast/commit/6a1b33a9a1237a83b0cebb114d3965405bd232fc))
* Enable materialization for ODFV Transform on Write ([#5459](https://github.com/feast-dev/feast/issues/5459)) ([3d17892](https://github.com/feast-dev/feast/commit/3d17892fe49352ab270c4b70932141c5e5d4acde))
* Improve search results formatting ([#5326](https://github.com/feast-dev/feast/issues/5326)) ([18cbd7f](https://github.com/feast-dev/feast/commit/18cbd7fe40bfdc74bcf9f873cef16ca93d50abd4))
* Improvements to Lambda materialization engine ([#5379](https://github.com/feast-dev/feast/issues/5379)) ([b486f29](https://github.com/feast-dev/feast/commit/b486f2920a5c88852892e1c5455b58e922af2ee7))
* Make batch_source optional in PushSource ([#5440](https://github.com/feast-dev/feast/issues/5440)) ([#5454](https://github.com/feast-dev/feast/issues/5454)) ([ae7e20e](https://github.com/feast-dev/feast/commit/ae7e20e0e0bec410f16b36be907a158ada58e178))
* Refactor materialization engine ([#5354](https://github.com/feast-dev/feast/issues/5354)) ([f5c5360](https://github.com/feast-dev/feast/commit/f5c5360bb9abcb9ae8129b3cf661304cf868e3f1))
* Remote Write to Online Store completes client / server architecture ([#5422](https://github.com/feast-dev/feast/issues/5422)) ([2368f42](https://github.com/feast-dev/feast/commit/2368f4291dbb75a15159124380cd86ab6edbff10))
* Serialization version 2 and below removed ([#5435](https://github.com/feast-dev/feast/issues/5435)) ([9e50e18](https://github.com/feast-dev/feast/commit/9e50e188a1b94d4436b53649cdc294cff55f4870))
* SQLite online retrieval. Add timezone info into timestamp. ([#5386](https://github.com/feast-dev/feast/issues/5386)) ([6b05153](https://github.com/feast-dev/feast/commit/6b051537535cd9f4cd5090a17a43124cd9ae821b))
* Support dual-mode REST and gRPC for Feast Registry Server ([#5396](https://github.com/feast-dev/feast/issues/5396)) ([fd1f448](https://github.com/feast-dev/feast/commit/fd1f44898ad85769af7ef2a082c47c1ea76e1cbd))
* Support DynamoDB as online store in Go feature server ([#5464](https://github.com/feast-dev/feast/issues/5464)) ([40d25c6](https://github.com/feast-dev/feast/commit/40d25c62e19283396c410cb0ea1ca8eb119b6002))
* Update Spark Compute read source node to be able to use other data sources ([#5445](https://github.com/feast-dev/feast/issues/5445)) ([a93d300](https://github.com/feast-dev/feast/commit/a93d30015b1b6df879c12a237d3712d94f9f6f24))


### Reverts

* Chore Release "chore(release): release 0.50.0" ([#5483](https://github.com/feast-dev/feast/issues/5483)) ([0eef391](https://github.com/feast-dev/feast/commit/0eef391e3a10108c7bc62fbae10f861d7d6ad3f7))
* Feat: Add CLI, SDK, and API documentation page to Feast UI" ([#5341](https://github.com/feast-dev/feast/issues/5341)) ([b492f14](https://github.com/feast-dev/feast/commit/b492f14c3fec7313807f426c3c070ea62e7c9dcb)), closes [#5337](https://github.com/feast-dev/feast/issues/5337)
* Revert "feat: Add s3 storage-based registry store in Go feature server" ([#5351](https://github.com/feast-dev/feast/issues/5351)) ([d5d6766](https://github.com/feast-dev/feast/commit/d5d6766c5cb02a2292ea42e3ee648a005af8a61a)), closes [#5336](https://github.com/feast-dev/feast/issues/5336)
* Revert "fix: Update milvus connect function to work with remote instance" ([#5398](https://github.com/feast-dev/feast/issues/5398)) ([434dd92](https://github.com/feast-dev/feast/commit/434dd92fa72660ff59fd20219970f4ef6de63d67)), closes [#5382](https://github.com/feast-dev/feast/issues/5382)

# [0.49.0](https://github.com/feast-dev/feast/compare/v0.48.0...v0.49.0) (2025-04-29)


### Bug Fixes

* Adding brackets to unit tests ([c46fea3](https://github.com/feast-dev/feast/commit/c46fea36254f011c7a8e63b35971bde78e963d92))
* Adding logic back for a step ([2bb240b](https://github.com/feast-dev/feast/commit/2bb240b1540db589563a9e34e37965101799f587))
* Adjustment for unit test action ([a6f78ae](https://github.com/feast-dev/feast/commit/a6f78ae919d8cd78493bb46d35b8bed57a07596a))
* Allow get_historical_features with only On Demand Feature View ([#5256](https://github.com/feast-dev/feast/issues/5256)) ([0752795](https://github.com/feast-dev/feast/commit/0752795895842bc2555f170b64bbaa2452e0f60c))
* CI adjustment ([3850643](https://github.com/feast-dev/feast/commit/3850643c02e72ac6df833e703fee00905b7db089))
* Embed Query configuration breaks when switching between DataFrame and SQL ([#5257](https://github.com/feast-dev/feast/issues/5257)) ([32375a5](https://github.com/feast-dev/feast/commit/32375a532193703b4ba306b27a12d25736a16665))
* Fix for proto issue in utils ([1b291b2](https://github.com/feast-dev/feast/commit/1b291b219545e07bef633923141421f37bc15c7a))
* Fix milvus online_read ([#5233](https://github.com/feast-dev/feast/issues/5233)) ([4b91f26](https://github.com/feast-dev/feast/commit/4b91f26ded28c709a8aeb220c2a7ce34fbc91a7b))
* Fix tests ([431d9b8](https://github.com/feast-dev/feast/commit/431d9b80ade7d8dd5beb560407336b928518ad6f))
* Fixed Permissions object parameter in example ([#5259](https://github.com/feast-dev/feast/issues/5259)) ([045c100](https://github.com/feast-dev/feast/commit/045c1000295e40b387e1d7b10798a606c91b37eb))
* Java CI [#12](https://github.com/feast-dev/feast/issues/12) ([d7e44ac](https://github.com/feast-dev/feast/commit/d7e44acdde8fd7cb0f6827a4cdad3dcb8985601f))
* Java PR [#15](https://github.com/feast-dev/feast/issues/15) ([a5da3bb](https://github.com/feast-dev/feast/commit/a5da3bbd9fb8b9e8acf729fd8df01a496f134d80))
* Java PR [#16](https://github.com/feast-dev/feast/issues/16) ([e0320fe](https://github.com/feast-dev/feast/commit/e0320fe9dcc867f313ec6e60a3a4be4ec42519bd))
* Java PR [#17](https://github.com/feast-dev/feast/issues/17) ([49da810](https://github.com/feast-dev/feast/commit/49da810edc466d1380e0348df84f8a38946164e3))
* Materialization logs ([#5243](https://github.com/feast-dev/feast/issues/5243)) ([4aa2f49](https://github.com/feast-dev/feast/commit/4aa2f49201ba5490b641d52d9c2f7710a245bd3e))
* Moving to custom github action for checking skip tests ([caf312e](https://github.com/feast-dev/feast/commit/caf312e35c07102f626a78eb5217aacf279369f0))
* Operator - remove default replicas setting from Feast Deployment ([#5294](https://github.com/feast-dev/feast/issues/5294)) ([e416d01](https://github.com/feast-dev/feast/commit/e416d0111c24434dca8c1655d3d38abc7c82afd3))
* Patch java pr [#14](https://github.com/feast-dev/feast/issues/14) ([592526c](https://github.com/feast-dev/feast/commit/592526c9e2e9e421b9d7617e6b14bf7d461ec72a))
* Patch update for test ([a3e8967](https://github.com/feast-dev/feast/commit/a3e8967ef5462457aa951efa56aff2e924b00689))
* Remove conditional from steps ([995307f](https://github.com/feast-dev/feast/commit/995307ff1769b67cf5164ddc63026b09f7296a9e))
* Remove misleading HTTP prefix from gRPC endpoints in logs and doc ([#5280](https://github.com/feast-dev/feast/issues/5280)) ([0ee3a1e](https://github.com/feast-dev/feast/commit/0ee3a1ea22846217fd4324a1cf3623c35a44a853))
* removing id ([268ade2](https://github.com/feast-dev/feast/commit/268ade28e3065556ec6da30f01596d39565a07b7))
* Renaming workflow file ([5f46279](https://github.com/feast-dev/feast/commit/5f462796263a89a77413eea9c3202166cc1fa6c9))
* Resolve `no pq wrapper` import issue ([#5240](https://github.com/feast-dev/feast/issues/5240)) ([d5906f1](https://github.com/feast-dev/feast/commit/d5906f13125a4e63e3ae1f3a6c3830dda2c66d51))
* Update actions to remove check skip tests ([#5275](https://github.com/feast-dev/feast/issues/5275)) ([b976f27](https://github.com/feast-dev/feast/commit/b976f272928b93b964b86362a37ffe77d84204cc))
* Update docling demo ([446efea](https://github.com/feast-dev/feast/commit/446efead36cf51a0471d49eda44be0c8cd220b51))
* Update java pr [#13](https://github.com/feast-dev/feast/issues/13) ([fda7db7](https://github.com/feast-dev/feast/commit/fda7db749b695eac869a48d12ce5e64c458f7d2e))
* Update java_pr ([fa138f4](https://github.com/feast-dev/feast/commit/fa138f421f0645db569455c0829676b1b9b8b4a7))
* Update repo_config.py ([6a59815](https://github.com/feast-dev/feast/commit/6a59815907b6f1424d529bcdd7fc45ad5a6a0504))
* Update unit tests workflow ([06486a0](https://github.com/feast-dev/feast/commit/06486a00573c8a29f55debd0634a6c3026abfc79))
* Updated docs for docling demo ([768e6cc](https://github.com/feast-dev/feast/commit/768e6cc256590be3054fd181a0a3097b4c14029c))
* Updating action for unit tests ([0996c28](https://github.com/feast-dev/feast/commit/0996c28a0061f65bee49f51a1704739b5a543f6f))
* Updating github actions to filter at job level ([0a09622](https://github.com/feast-dev/feast/commit/0a096229a1c8251578c4cbfc55f96c78fd6f76c4))
* Updating Java CI ([c7c3a3c](https://github.com/feast-dev/feast/commit/c7c3a3c7b57d5143c2b988a78c86f23e60d43af3))
* Updating java pr to skip tests ([e997dd9](https://github.com/feast-dev/feast/commit/e997dd919e1d5d204265a62fa54e75ccbaf930ee))
* Updating workflows ([c66bcd2](https://github.com/feast-dev/feast/commit/c66bcd24186f502720627e790da555be6ce2c5d5))


### Features

* Add date_partition_column_format for spark source ([#5273](https://github.com/feast-dev/feast/issues/5273)) ([7a61d6f](https://github.com/feast-dev/feast/commit/7a61d6faaa8be4d1e2c25a453628a3bcdc5aa8a2))
* Add Milvus tutorial with Feast integration ([#5292](https://github.com/feast-dev/feast/issues/5292)) ([a1388a5](https://github.com/feast-dev/feast/commit/a1388a5f7e174f578264a52478bacea14856a88a))
* Add pgvector tutorial with PostgreSQL integration ([#5290](https://github.com/feast-dev/feast/issues/5290)) ([bb1cbea](https://github.com/feast-dev/feast/commit/bb1cbeae658bc79122b5f29d7bb9dc2b7c539eb5))
* Add ReactFlow visualization for Feast registry metadata ([#5297](https://github.com/feast-dev/feast/issues/5297)) ([9768970](https://github.com/feast-dev/feast/commit/9768970e270b235a3657c92c6b8891977e8ed486))
* Add retrieve online documents v2 method into  pgvector  ([#5253](https://github.com/feast-dev/feast/issues/5253)) ([6770ee6](https://github.com/feast-dev/feast/commit/6770ee65a7b79880331e0409b00bbc2cd3a272bc))
* Compute Engine Initial Implementation ([#5223](https://github.com/feast-dev/feast/issues/5223)) ([64bdafd](https://github.com/feast-dev/feast/commit/64bdafdddfac43cac61a3e31354966fccec6dc0f))
* Enable write node for compute engine ([#5287](https://github.com/feast-dev/feast/issues/5287)) ([f9baf97](https://github.com/feast-dev/feast/commit/f9baf974cd334e8758e3b1b8e5b8222463a3cdc8))
* Local compute engine ([#5278](https://github.com/feast-dev/feast/issues/5278)) ([8e06dfe](https://github.com/feast-dev/feast/commit/8e06dfe839152f5ee2a6b8e63494e15ceaa67910))
* Make transform on writes configurable for ingestion ([#5283](https://github.com/feast-dev/feast/issues/5283)) ([ecad170](https://github.com/feast-dev/feast/commit/ecad1708c9989782cbc9a200d21d0c021e4693ff))
* Offline store update pull_all_from_table_or_query to make timestampfield optional ([#5281](https://github.com/feast-dev/feast/issues/5281)) ([4b94608](https://github.com/feast-dev/feast/commit/4b94608f400a3c379b37ce7ea0bdb969eee1527d))
* Serialization version 2 deprecation notice ([#5248](https://github.com/feast-dev/feast/issues/5248)) ([327d99d](https://github.com/feast-dev/feast/commit/327d99d0f8c3ae303b9d47199ebc590a5f1dd7f6))
* Vector length definition moved to Feature View from Config  ([#5289](https://github.com/feast-dev/feast/issues/5289)) ([d8f1c97](https://github.com/feast-dev/feast/commit/d8f1c979f6ec44c3c3bcacb943b0959df732eb58))

## [0.48.1](https://github.com/feast-dev/feast/compare/v0.48.0...v0.48.1) (2025-04-08)


### Bug Fixes

* Fix milvus online_read ([#5233](https://github.com/feast-dev/feast/issues/5233)) ([4b91f26](https://github.com/feast-dev/feast/commit/4b91f26ded28c709a8aeb220c2a7ce34fbc91a7b))
* Resolve `no pq wrapper` import issue ([#5240](https://github.com/feast-dev/feast/issues/5240)) ([d5906f1](https://github.com/feast-dev/feast/commit/d5906f13125a4e63e3ae1f3a6c3830dda2c66d51))

# [0.48.0](https://github.com/feast-dev/feast/compare/v0.47.0...v0.48.0) (2025-04-07)


### Bug Fixes

* Enhance integration logos display and styling in the UI ([#5221](https://github.com/feast-dev/feast/issues/5221)) ([5799257](https://github.com/feast-dev/feast/commit/57992574fd87ebe9d24f7a7e837f5bf4e0c21651))
* Fix space typo in push.md docs ([#5184](https://github.com/feast-dev/feast/issues/5184)) ([81677b2](https://github.com/feast-dev/feast/commit/81677b2197438fc5f5994e40b10421bce8b3f25c))
* Fixed integration tests for qdrant and milvus ([#5224](https://github.com/feast-dev/feast/issues/5224)) ([d6b080d](https://github.com/feast-dev/feast/commit/d6b080d06b1fd27a8f4835d523e93209eea8b9fd))
* Formatting trino ([760ec0e](https://github.com/feast-dev/feast/commit/760ec0ee7e628cdecc9289d56c1d1915f33a4f75))
* Multiple fixes in retrieval of online documents ([#5168](https://github.com/feast-dev/feast/issues/5168)) ([66ddd3e](https://github.com/feast-dev/feast/commit/66ddd3ef376044fc79d7edf64281b8ff85c46647))
* Operator route creation for Feast UI in OpenShift ([e3946b4](https://github.com/feast-dev/feast/commit/e3946b4ad8392440afbf70f9724206bf09a51a9f))
* Remove entity_rows parameter from retrieve_online_documents_v2 call ([#5225](https://github.com/feast-dev/feast/issues/5225)) ([2a2e304](https://github.com/feast-dev/feast/commit/2a2e3047b238db38fcd3129a6cbb1f9f9b9110fb))
* Styling ([#5222](https://github.com/feast-dev/feast/issues/5222)) ([34c393c](https://github.com/feast-dev/feast/commit/34c393cedde6e42b5d0cab118b508cb4de515f1f))
* typo in the chart ([bd3448b](https://github.com/feast-dev/feast/commit/bd3448b969348adfa1945395859a101b65f4f555))
* Update milvus-quickstart and feature_store.yaml with correct Milvus Config ([#5200](https://github.com/feast-dev/feast/issues/5200)) ([306acca](https://github.com/feast-dev/feast/commit/306acca510fb90f51ed3de431cfe20f76dc4c971))
* Update Qdrant online store paths in repo_config.py ([#5207](https://github.com/feast-dev/feast/issues/5207)) ([ab35b0b](https://github.com/feast-dev/feast/commit/ab35b0bf09a3c0185940078b5f52c56af5c5dcc3)), closes [#5206](https://github.com/feast-dev/feast/issues/5206)
* Update the doc ([#5194](https://github.com/feast-dev/feast/issues/5194)) ([726464e](https://github.com/feast-dev/feast/commit/726464e889083fa8f0eadcb42fd466bd6bc64594))
* Updated the operator-rabc example to test RBAC from a Kubernete pod ([#5147](https://github.com/feast-dev/feast/issues/5147)) ([d23a1a5](https://github.com/feast-dev/feast/commit/d23a1a573119b3add54bdfa071ecce2fa2d16c97))


### Features

* add `real`(float32) type for trino offline store ([#4749](https://github.com/feast-dev/feast/issues/4749)) ([0947f96](https://github.com/feast-dev/feast/commit/0947f969dbf49700e8adf85ac23659241c1c2601))
* Add async DynamoDB timeout and retry configuration ([#5178](https://github.com/feast-dev/feast/issues/5178)) ([2f3bcf5](https://github.com/feast-dev/feast/commit/2f3bcf5f0ccbe109bec6bc10cd7c33d6f70cab84))
* Add CronJob capability to the Operator (feast apply & materialize-incremental) ([#5217](https://github.com/feast-dev/feast/issues/5217)) ([285c0dc](https://github.com/feast-dev/feast/commit/285c0dc94365710230985821ada510872e99b084))
* Add RAG tutorial and Use Cases documentation ([#5226](https://github.com/feast-dev/feast/issues/5226)) ([99f4004](https://github.com/feast-dev/feast/commit/99f40047645fd820e4b741d19d20958c03ac9dae))
* Added CLI for features, get historical and online features ([#5197](https://github.com/feast-dev/feast/issues/5197)) ([4ab9f74](https://github.com/feast-dev/feast/commit/4ab9f746050daa0cee10badd5c537bbcc598933b))
* Added export support in feast UI ([#5198](https://github.com/feast-dev/feast/issues/5198)) ([b079553](https://github.com/feast-dev/feast/commit/b0795533b83c984d897179e2b1caa459cf79f7e0))
* Added global registry search support in Feast UI ([#5195](https://github.com/feast-dev/feast/issues/5195)) ([f09ea49](https://github.com/feast-dev/feast/commit/f09ea4958b7cc2ec2c1d3095e538aea278d934fc))
* Added UI for Features list ([#5192](https://github.com/feast-dev/feast/issues/5192)) ([cc7fd47](https://github.com/feast-dev/feast/commit/cc7fd474113871f92747b1b9ca5aa6441fe860f4))
* Adding blog on RAG with Milvus ([#5161](https://github.com/feast-dev/feast/issues/5161)) ([b9e2e6c](https://github.com/feast-dev/feast/commit/b9e2e6cdd9fb425fbe5839285c7f5a61620e63de))
* Adding Docling RAG demo ([#5109](https://github.com/feast-dev/feast/issues/5109)) ([569404b](https://github.com/feast-dev/feast/commit/569404b01d761dd49fd1930143e618f5be6612e1))
* Allow transformations on writes to output list of entities ([#5209](https://github.com/feast-dev/feast/issues/5209)) ([955521a](https://github.com/feast-dev/feast/commit/955521acc949e8bb314e68bcc95ce56cdbf74d11))
* Cache get_any_feature_view results ([#5175](https://github.com/feast-dev/feast/issues/5175)) ([924b8a3](https://github.com/feast-dev/feast/commit/924b8a384f7f8da903f281004f44506a42cd5e13))
* Clickhouse offline store ([#4725](https://github.com/feast-dev/feast/issues/4725)) ([86794c2](https://github.com/feast-dev/feast/commit/86794c22f7b9768da52eeab9d0d50e4f6e673179))
* Enable keyword search for Milvus ([#5199](https://github.com/feast-dev/feast/issues/5199)) ([ac44967](https://github.com/feast-dev/feast/commit/ac449671cb5f59d2b8040f37b665c0cb0bc1235d))
* Enable transformations on PDFs ([#5172](https://github.com/feast-dev/feast/issues/5172)) ([3674971](https://github.com/feast-dev/feast/commit/367497161313dc0984fe815f8be740873c8c8a8c))
* Enable users to use Entity Query as CTE during historical retrieval ([#5202](https://github.com/feast-dev/feast/issues/5202)) ([fe69eaf](https://github.com/feast-dev/feast/commit/fe69eaff7eb9cbc5d2725f113f9ce1bd4539859a))
* helm support more deployment config ([d575372](https://github.com/feast-dev/feast/commit/d575372f54c29ced8bfd6e6df15defa461830136))
* Improved CLI file structuring ([#5201](https://github.com/feast-dev/feast/issues/5201)) ([972ed34](https://github.com/feast-dev/feast/commit/972ed344a39705ad8c4501cf35c59aa502100ac8))
* Kickoff Transformation implementationtransformation code base ([#5181](https://github.com/feast-dev/feast/issues/5181)) ([0083303](https://github.com/feast-dev/feast/commit/00833033eda041d8f8146b322145b6f1d8ff7d1c))
* Make keep-alive timeout configurable for async DynamoDB connections ([#5167](https://github.com/feast-dev/feast/issues/5167)) ([7f3e528](https://github.com/feast-dev/feast/commit/7f3e528f4493e306201e46dfd5bfbda5a59d3ef7))
* Operator mounts the odh-trusted-ca-bundle configmap when deployed on RHOAI or ODH ([d4d7b0d](https://github.com/feast-dev/feast/commit/d4d7b0d2a7ecf2d8daa79c51ecc70e8bba8106f3))
* Spark Transformation ([#5185](https://github.com/feast-dev/feast/issues/5185)) ([be3d85c](https://github.com/feast-dev/feast/commit/be3d85c5048cf469e65264b5dd465e2e585b67b8))

# [0.47.0](https://github.com/feast-dev/feast/compare/v0.46.0...v0.47.0) (2025-03-10)


* feat!: Include PUBLIC_URL in defaultProjectListPromise URL in /ui ([2f0f7b3](https://github.com/feast-dev/feast/commit/2f0f7b386c7297ea8393ad4e3d73b198581f341b))


### Bug Fixes

* Add transformation_service_endpoit to support Go feature server. ([#5071](https://github.com/feast-dev/feast/issues/5071)) ([5627d7c](https://github.com/feast-dev/feast/commit/5627d7cd3676d61290cd716b74feb9212227b5fa))
* Adding extra space on the VM to kind cluster to see if this solves the issue with memory not available with operator e2e tests. ([#5102](https://github.com/feast-dev/feast/issues/5102)) ([e6e928c](https://github.com/feast-dev/feast/commit/e6e928c1f53fe323359275d9ab05b235ace04026))
* Allow unencrypted Snowflake key ([#5097](https://github.com/feast-dev/feast/issues/5097)) ([87a7c23](https://github.com/feast-dev/feast/commit/87a7c23dac7e5b99471b90ee9f2bf375c5705210))
* Cant add different type of list types ([#5118](https://github.com/feast-dev/feast/issues/5118)) ([bebd7be](https://github.com/feast-dev/feast/commit/bebd7be16aeabc2c7f2e0f570393c0fabe219d8b))
* Fixing transformations on writes ([#5127](https://github.com/feast-dev/feast/issues/5127)) ([95ac34a](https://github.com/feast-dev/feast/commit/95ac34a63ce99186be003ba28a21f93fc9d81fce))
* Identify s3/remote uri path correctly ([#5076](https://github.com/feast-dev/feast/issues/5076)) ([93becff](https://github.com/feast-dev/feast/commit/93becff68a3552c0cfa6dbcd2b9e778083328472))
* Increase available action VM storage and reduce dev feature-server image size ([#5112](https://github.com/feast-dev/feast/issues/5112)) ([75f5a90](https://github.com/feast-dev/feast/commit/75f5a90536f7caa566b38b9c368ec33a90d2bfa5))
* Move Feast to pyproject.toml instead of setup.py ([#5067](https://github.com/feast-dev/feast/issues/5067)) ([4231274](https://github.com/feast-dev/feast/commit/4231274800873c1421ffa95602d7ee36a9265eb5))
* Skip refresh if already in progress or if lock is already held ([#5068](https://github.com/feast-dev/feast/issues/5068)) ([f3a24de](https://github.com/feast-dev/feast/commit/f3a24dea078ca5ecaace9fe100b7190972e6617d))


### Features

* Add an OOTB Chat uI to the Feature Server to support RAG demo ([#5106](https://github.com/feast-dev/feast/issues/5106)) ([40ea7a9](https://github.com/feast-dev/feast/commit/40ea7a924bcfa6b9ca55125c1ba7ac9c82f6d280))
* Add Couchbase Columnar as an Offline Store ([#5025](https://github.com/feast-dev/feast/issues/5025)) ([4373cbf](https://github.com/feast-dev/feast/commit/4373cbf9974eff7f8913a1d0ade14c0a1f385295))
* Add Feast Operator RBAC example with Kubernetes Authentication … ([#5077](https://github.com/feast-dev/feast/issues/5077)) ([2179fbe](https://github.com/feast-dev/feast/commit/2179fbe41b53e1c10bf83f142fcc7fba43e83bdf))
* Added docling and pytorch as add on ([#5089](https://github.com/feast-dev/feast/issues/5089)) ([135342b](https://github.com/feast-dev/feast/commit/135342bb1665dbed58912c61ee7c00e2f61b6ef2))
* Feast Operator example with Postgres in TLS mode. ([#5028](https://github.com/feast-dev/feast/issues/5028)) ([2c46f6a](https://github.com/feast-dev/feast/commit/2c46f6a283294fc4c7d90c792d560f01ae0f1b64))
* Operator - Add feastProjectDir section to CR with git & init options ([#5079](https://github.com/feast-dev/feast/issues/5079)) ([d64f01e](https://github.com/feast-dev/feast/commit/d64f01ecf406f9d429196ae0bd08a84ec90f0c15))
* Override the udf name when provided as input to an on demand transformation ([#5094](https://github.com/feast-dev/feast/issues/5094)) ([8a714bb](https://github.com/feast-dev/feast/commit/8a714bb57093a1a77057748ad9eb1f529d34f41e))
* Set value_type of entity directly in from_proto ([#5092](https://github.com/feast-dev/feast/issues/5092)) ([90e7498](https://github.com/feast-dev/feast/commit/90e7498c8a65eac685d35b83d709f647b03f1445))
* Updating retrieve online documents v2 to work for other fields for sq… ([#5082](https://github.com/feast-dev/feast/issues/5082)) ([fc121c3](https://github.com/feast-dev/feast/commit/fc121c3fb96ffb6e8dcaf096ccdda3933492972d))


### BREAKING CHANGES

* The PUBLIC_URL environment variable is now taken into account by default
when fetching the projects list. This is a breaking change only if all
these points apply:

1. You're using Feast UI as a module

2. You're serving the UI files from a non-root path via the PUBLIC_URL
   environment variable

3. You're serving the project list from the root path

4. You're not passing the `feastUIConfigs.projectListPromise` prop to
   the FeastUI component

In this case, you need to explicitly fetch the project list from the
root path via the `feastUIConfigs.projectListPromise` prop:

```diff
 const root = createRoot(document.getElementById("root")!);
 root.render(
   <React.StrictMode>
-    <FeastUI />
+    <FeastUI
+      feastUIConfigs={{
+        projectListPromise: fetch("/projects-list.json", {
+            headers: {
+              "Content-Type": "application/json",
+            },
+          }).then((res) => res.json())
+      }}
+    />
   </React.StrictMode>
 );
```

Signed-off-by: Harri Lehtola <peruukki@hotmail.com>

# [0.46.0](https://github.com/feast-dev/feast/compare/v0.45.0...v0.46.0) (2025-02-17)


### Bug Fixes

* Add scylladb to online stores list in docs ([#5061](https://github.com/feast-dev/feast/issues/5061)) ([08183ed](https://github.com/feast-dev/feast/commit/08183ed38581eb655e2f6055c50b9223fcf9662e))
* Changed feast operator to set status of featurestore cr to ready based on deployment.status = available ([#5020](https://github.com/feast-dev/feast/issues/5020)) ([fce0d35](https://github.com/feast-dev/feast/commit/fce0d35bc00553269fff6abb7a16897577a2421f))
* Ensure Postgres queries are committed or autocommit is used ([#5039](https://github.com/feast-dev/feast/issues/5039)) ([46f8d7a](https://github.com/feast-dev/feast/commit/46f8d7aa87cfaf36d17c162c4f41cd983a2938d5))
* Fixing the release workflow to refresh the stable branch when the release is not running in the dry run mode. ([#5057](https://github.com/feast-dev/feast/issues/5057)) ([a13fa9b](https://github.com/feast-dev/feast/commit/a13fa9bd18be94b349954e5db66fd30ba4db1d1e))
* Operator - make onlineStore the default service ([#5044](https://github.com/feast-dev/feast/issues/5044)) ([6c92447](https://github.com/feast-dev/feast/commit/6c92447d1507bff02451f77f134df0a24cbd8036))
* Operator - resolve infinite reconciler loop in authz controller ([#5056](https://github.com/feast-dev/feast/issues/5056)) ([11e4548](https://github.com/feast-dev/feast/commit/11e45482b0cace1f3c3a0ddc567a8a1172d6792a))
* Resolve module on windows ([#4827](https://github.com/feast-dev/feast/issues/4827)) ([efbffa4](https://github.com/feast-dev/feast/commit/efbffa4be0f38166ff35f133a9b69bcbd243debd))
* Setting the github_token explicitly to see if that solves the problem. ([#5012](https://github.com/feast-dev/feast/issues/5012)) ([3834ffa](https://github.com/feast-dev/feast/commit/3834ffa31f52b9a68b27a9f898538827ee8e5c39))
* Validate entities when running get_online_features ([#5031](https://github.com/feast-dev/feast/issues/5031)) ([3bb0dca](https://github.com/feast-dev/feast/commit/3bb0dca1692fb7087e967a9fc33a4b08720b13d2))


### Features

* Add SQLite retrieve_online_documents_v2 ([#5032](https://github.com/feast-dev/feast/issues/5032)) ([0fffe21](https://github.com/feast-dev/feast/commit/0fffe211be9db18d318634f47bc9401fd6e218a0))
* Adding Click command to display configuration details ([#5036](https://github.com/feast-dev/feast/issues/5036)) ([ae68e4d](https://github.com/feast-dev/feast/commit/ae68e4de0c184dc2990ea7e8d08d2d7f1613b06f))
* Adding volumes and volumeMounts support to Feature Store CR. ([#4983](https://github.com/feast-dev/feast/issues/4983)) ([ec6f1b7](https://github.com/feast-dev/feast/commit/ec6f1b750ed49ef36c5e3aa9f8db1d030bf80047))
* Moving the job to seperate action so that we can test it easily. ([#5013](https://github.com/feast-dev/feast/issues/5013)) ([b9325b7](https://github.com/feast-dev/feast/commit/b9325b7f42b8866fa43b1c7567e3288dd589020f))
* Operator - make server container creation explicit in the CR ([#5024](https://github.com/feast-dev/feast/issues/5024)) ([b16fb40](https://github.com/feast-dev/feast/commit/b16fb400fd63fdc0168cb1f845638fc003724fd4))

# [0.45.0](https://github.com/feast-dev/feast/compare/v0.44.0...v0.45.0) (2025-02-04)


### Features

* Changing refresh stable branch from step to a job. Using github credentials bot so that we can push the changes. ([#5011](https://github.com/feast-dev/feast/issues/5011)) ([7335e26](https://github.com/feast-dev/feast/commit/7335e266455561ebcb5ce8e318a79661e509a1c2))

# [0.44.0](https://github.com/feast-dev/feast/compare/v0.43.0...v0.44.0) (2025-02-04)


### Bug Fixes

* Adding periodic check to fix the sporadic failures of the operator e2e tests.  ([#4952](https://github.com/feast-dev/feast/issues/4952)) ([1d086be](https://github.com/feast-dev/feast/commit/1d086beb9f9726f68ababace87c58c2cc6412ca3))
* Adding the feast-operator/bin to the .gitignore directory. Somehow it… ([#5005](https://github.com/feast-dev/feast/issues/5005)) ([1a027ee](https://github.com/feast-dev/feast/commit/1a027eec3dc38ce8a949aca842c91742b0f68b47))
* Changed Env Vars for e2e tests ([#4975](https://github.com/feast-dev/feast/issues/4975)) ([fa0084f](https://github.com/feast-dev/feast/commit/fa0084f2ed0e9d41ff813538ee63dd4ee7371e6c))
* Fix GitHub Actions to pass authentication ([#4963](https://github.com/feast-dev/feast/issues/4963)) ([22b9138](https://github.com/feast-dev/feast/commit/22b9138a3c0040f5779f7218522f2d96e750fbbf)), closes [#4937](https://github.com/feast-dev/feast/issues/4937) [#4939](https://github.com/feast-dev/feast/issues/4939) [#4941](https://github.com/feast-dev/feast/issues/4941) [#4940](https://github.com/feast-dev/feast/issues/4940) [#4943](https://github.com/feast-dev/feast/issues/4943) [#4944](https://github.com/feast-dev/feast/issues/4944) [#4945](https://github.com/feast-dev/feast/issues/4945) [#4946](https://github.com/feast-dev/feast/issues/4946) [#4947](https://github.com/feast-dev/feast/issues/4947) [#4948](https://github.com/feast-dev/feast/issues/4948) [#4951](https://github.com/feast-dev/feast/issues/4951) [#4954](https://github.com/feast-dev/feast/issues/4954) [#4957](https://github.com/feast-dev/feast/issues/4957) [#4958](https://github.com/feast-dev/feast/issues/4958) [#4959](https://github.com/feast-dev/feast/issues/4959) [#4960](https://github.com/feast-dev/feast/issues/4960) [#4962](https://github.com/feast-dev/feast/issues/4962)
* Fix showing selected navigation item in UI sidebar ([#4969](https://github.com/feast-dev/feast/issues/4969)) ([8ac6a85](https://github.com/feast-dev/feast/commit/8ac6a8547361708fec00a11a33c48ca3ae25f311))
* Invalid column names in get_historical_features when there are field mappings on join keys ([#4886](https://github.com/feast-dev/feast/issues/4886)) ([c9aca2d](https://github.com/feast-dev/feast/commit/c9aca2d42254d1c4dfcc778b0d90303329901bd0))
* Read project data from the 'projects' key while loading the registry state in the Feast UI ([#4772](https://github.com/feast-dev/feast/issues/4772)) ([cb81939](https://github.com/feast-dev/feast/commit/cb8193945932b98d5b8f750ac07d58c034870565))
* Remove grpcurl dependency from Operator ([#4972](https://github.com/feast-dev/feast/issues/4972)) ([439e0b9](https://github.com/feast-dev/feast/commit/439e0b98819ef222b35617dfd6c97f04ca049f2f))
* Removed the dry-run flag to test and we will add it back later. ([#5007](https://github.com/feast-dev/feast/issues/5007)) ([d112b52](https://github.com/feast-dev/feast/commit/d112b529d618f19a5602039b6d347915d7e75b88))
* Render UI navigation items as links instead of buttons ([#4970](https://github.com/feast-dev/feast/issues/4970)) ([1267703](https://github.com/feast-dev/feast/commit/1267703d099491393ca212c38f1a63a36fe6c443))
* Resolve Operator CRD bloat due to long field descriptions ([#4985](https://github.com/feast-dev/feast/issues/4985)) ([7593bb3](https://github.com/feast-dev/feast/commit/7593bb3ec8871dbb83403461e0b6f6863d64abc6))
* Update manifest to add feature server image for odh ([#4973](https://github.com/feast-dev/feast/issues/4973)) ([6a1c102](https://github.com/feast-dev/feast/commit/6a1c1029b5462aaa42c82fdad421176ad1692f81))
* Updating release workflows to refer to yml instead of yaml ([#4935](https://github.com/feast-dev/feast/issues/4935)) ([02b0a68](https://github.com/feast-dev/feast/commit/02b0a68a435ab01f26b20824f3f8a4dd4e21da8d))
* Use locally built feast-ui package in dev feature-server image ([#4998](https://github.com/feast-dev/feast/issues/4998)) ([0145e55](https://github.com/feast-dev/feast/commit/0145e5501e2c7854628d204cb515270fac3bee7d))


### Features

* Added OWNERS file for OpenshiftCI ([#4991](https://github.com/feast-dev/feast/issues/4991)) ([86a2ee8](https://github.com/feast-dev/feast/commit/86a2ee8e3ce1cd4432749928fda7a4386dc7ce0f))
* Adding Milvus demo to examples ([#4910](https://github.com/feast-dev/feast/issues/4910)) ([2daf852](https://github.com/feast-dev/feast/commit/2daf8527c4539a007d639ac6e3061767a9c45110))
* Adding retrieve_online_documents endpoint ([#5002](https://github.com/feast-dev/feast/issues/5002)) ([6607d3d](https://github.com/feast-dev/feast/commit/6607d3dfa1041638d3896b25cb98677412889724))
* Adding support to return additional features from vector retrieval for Milvus db ([#4971](https://github.com/feast-dev/feast/issues/4971)) ([6ce08d3](https://github.com/feast-dev/feast/commit/6ce08d31863b12a7a92bf5207172a05f8da077d1))
* Creating/updating the stable branch after the release. ([#5003](https://github.com/feast-dev/feast/issues/5003)) ([e9b53cc](https://github.com/feast-dev/feast/commit/e9b53cc83ee51b906423ec2e1fac36e159d55db2))
* Implementing online_read for MilvusOnlineStore ([#4996](https://github.com/feast-dev/feast/issues/4996)) ([92dde13](https://github.com/feast-dev/feast/commit/92dde1311c419dc3d8cbb534ed2e706fdeae1e26))
* Improve exception message for unsupported Snowflake data types ([#4779](https://github.com/feast-dev/feast/issues/4779)) ([5992364](https://github.com/feast-dev/feast/commit/59923645e4f6a64a49bcecb7da503528af850d0f))
* Operator add feast ui deployment ([#4930](https://github.com/feast-dev/feast/issues/4930)) ([b026d0c](https://github.com/feast-dev/feast/commit/b026d0ce30d7ce9b621679fbb33f2a9c0edaad84))
* Updating documents to highlight v2 api for Vector Similarity Se… ([#5000](https://github.com/feast-dev/feast/issues/5000)) ([32b82a4](https://github.com/feast-dev/feast/commit/32b82a4b59bceaf9eb6662f35e77d0cae0d36550))

# [0.43.0](https://github.com/feast-dev/feast/compare/v0.42.0...v0.43.0) (2025-01-20)


### Bug Fixes

* Add k8s module to feature-server image ([#4839](https://github.com/feast-dev/feast/issues/4839)) ([f565565](https://github.com/feast-dev/feast/commit/f565565e0132ea5170221dc6af2e93a5dc3e750d))
* Adding input to workflow ([e3e8c97](https://github.com/feast-dev/feast/commit/e3e8c975b4b9891913d0be8d50df909d4d243191))
* Change image push to use --all-tags option ([#4926](https://github.com/feast-dev/feast/issues/4926)) ([02458fd](https://github.com/feast-dev/feast/commit/02458fd7aad49d5daa5b9836f5abdc4dd81d07bb))
* Fix integration build/push for images ([#4923](https://github.com/feast-dev/feast/issues/4923)) ([695e49b](https://github.com/feast-dev/feast/commit/695e49bd93a4c8af2ce5839586295b5e74e1b98e))
* Fix integration operator push ([#4924](https://github.com/feast-dev/feast/issues/4924)) ([13c7267](https://github.com/feast-dev/feast/commit/13c7267b555cca4f3361f34fb384a6fd9f27dedf))
* Fix release.yml ([#4845](https://github.com/feast-dev/feast/issues/4845)) ([b4768a8](https://github.com/feast-dev/feast/commit/b4768a81b94352de037dc305df309fcf06fd2973))
* Fixing some of the warnings with the github actions ([#4763](https://github.com/feast-dev/feast/issues/4763)) ([1119439](https://github.com/feast-dev/feast/commit/1119439c49bc90e62f02da078901509c1d740236))
* Improve status.applied updates & add offline pvc unit test ([#4871](https://github.com/feast-dev/feast/issues/4871)) ([3f49517](https://github.com/feast-dev/feast/commit/3f49517dfeabea5ffbd3f6b589cc0f2280ee4018))
* Made fixes to Go Operator DB persistence ([#4830](https://github.com/feast-dev/feast/issues/4830)) ([cdc0753](https://github.com/feast-dev/feast/commit/cdc075360242bfdf3812d394a3c9c550f81b0f98))
* Make transformation_service_endpoint configuration optional ([#4880](https://github.com/feast-dev/feast/issues/4880)) ([c62377b](https://github.com/feast-dev/feast/commit/c62377bc095a83022d13e5a8a3a9413d7e0f3e2c))
* Move pre-release image builds to quay.io, retire gcr.io pushes ([#4922](https://github.com/feast-dev/feast/issues/4922)) ([40b975b](https://github.com/feast-dev/feast/commit/40b975b8468de2678af8b191e93495e51af0b6aa))
* Performance regression in /get-online-features ([#4892](https://github.com/feast-dev/feast/issues/4892)) ([0db56a2](https://github.com/feast-dev/feast/commit/0db56a2cb5888bc21dbdb331e2b5fc3d33508424))
* Refactor Operator to deploy all feast services to the same Deployment/Pod ([#4863](https://github.com/feast-dev/feast/issues/4863)) ([88854dd](https://github.com/feast-dev/feast/commit/88854dd56fd0becf4a5d5293735a1c9ba394d53d))
* Remove unnecessary google cloud steps & upgrade docker action versions ([#4925](https://github.com/feast-dev/feast/issues/4925)) ([32aaf9a](https://github.com/feast-dev/feast/commit/32aaf9aba96c53e1c69577312982472182e99659))
* Remove verifyClient TLS offlineStore option from the Operator ([#4847](https://github.com/feast-dev/feast/issues/4847)) ([79fa247](https://github.com/feast-dev/feast/commit/79fa247026dd95e75a19308d437997310d061b35))
* Resolving syntax error while querying a feature view with column name starting with a number and BigQuery as data source ([#4908](https://github.com/feast-dev/feast/issues/4908)) ([d3495a0](https://github.com/feast-dev/feast/commit/d3495a09083b1e6a746fff8444f0bbb887d6ac8b))
* Updated python-helm-demo example to use MinIO instead of GS ([#4691](https://github.com/feast-dev/feast/issues/4691)) ([31afd99](https://github.com/feast-dev/feast/commit/31afd99c0969002fe04982e40cf7a857960f7abf))


### Features

* Add date field support to spark ([#4913](https://github.com/feast-dev/feast/issues/4913)) ([a8aeb79](https://github.com/feast-dev/feast/commit/a8aeb79830f12358c2355be44fca68e61992cb46))
* Add date support when converting from python to feast types ([#4918](https://github.com/feast-dev/feast/issues/4918)) ([bd9f071](https://github.com/feast-dev/feast/commit/bd9f071017756e205fbabe6af0d38dfaa9be3d7b))
* Add duckdb extra to multicloud release image ([#4862](https://github.com/feast-dev/feast/issues/4862)) ([b539eba](https://github.com/feast-dev/feast/commit/b539ebaad5ec2c1a199fe08ceccd206754ce82f0))
* Add milvus package to release image & option to Operator ([#4870](https://github.com/feast-dev/feast/issues/4870)) ([ef724b6](https://github.com/feast-dev/feast/commit/ef724b66bd4d5f355b055d6d81525c4a17ce94c1))
* Add Milvus Vector Database Implementation ([#4751](https://github.com/feast-dev/feast/issues/4751)) ([22c7b58](https://github.com/feast-dev/feast/commit/22c7b58f9590a357eaa57c77d5ed351f1fa07501))
* Add online/offline replica support ([#4812](https://github.com/feast-dev/feast/issues/4812)) ([b97da6c](https://github.com/feast-dev/feast/commit/b97da6ca3a08e3f0fc35552dd7f0bd3b59083f35))
* Added pvc accessModes support ([#4851](https://github.com/feast-dev/feast/issues/4851)) ([a73514c](https://github.com/feast-dev/feast/commit/a73514cd4f7fecbc89679566e0f8a0af16b6b06d))
* Adding EnvFrom support for the OptionalConfigs type to the Go Operator ([#4909](https://github.com/feast-dev/feast/issues/4909)) ([e01e510](https://github.com/feast-dev/feast/commit/e01e51076f5d8fe5be459037bd254e6f94e0cb0f))
* Adding Feature Server to components docs ([#4868](https://github.com/feast-dev/feast/issues/4868)) ([f95e54b](https://github.com/feast-dev/feast/commit/f95e54bdbee80be6b0e290a02e56f92daac2cf64))
* Adding features field to retrieve_online_features to return mor… ([#4869](https://github.com/feast-dev/feast/issues/4869)) ([7df287e](https://github.com/feast-dev/feast/commit/7df287e8c0f5ec3ab3fa88fd5576f636053a3769))
* Adding packages for Milvus Online Store ([#4854](https://github.com/feast-dev/feast/issues/4854)) ([49171bd](https://github.com/feast-dev/feast/commit/49171bd53fb8bfc325eb7167cac8cae18a28bd63))
* Adding vector_search parameter to fields ([#4855](https://github.com/feast-dev/feast/issues/4855)) ([739eaa7](https://github.com/feast-dev/feast/commit/739eaa78e6d995ee0750292d2f8d81886a3f9829))
* Feast Operator support log level configuration for services ([#4808](https://github.com/feast-dev/feast/issues/4808)) ([19424bc](https://github.com/feast-dev/feast/commit/19424bcc975d90d922791b5bd0da6ac13955c0c5))
* Go Operator - Parsing the output to go structs ([#4832](https://github.com/feast-dev/feast/issues/4832)) ([732865f](https://github.com/feast-dev/feast/commit/732865f20e7fae7a46f54be7bc469ce2b3bc44e2))
* Implement `date_partition_column` for `SparkSource` ([#4844](https://github.com/feast-dev/feast/issues/4844)) ([c5ffa03](https://github.com/feast-dev/feast/commit/c5ffa037cb030c64d6e25995199cf762cc0e9b2a))
* Loading the CA trusted store certificate into Feast to verify the public certificate. ([#4852](https://github.com/feast-dev/feast/issues/4852)) ([132ce2a](https://github.com/feast-dev/feast/commit/132ce2a6c9e3ff8544680d5237e9e1523d988d7e))
* Operator E2E test to validate FeatureStore custom resource using remote registry ([#4822](https://github.com/feast-dev/feast/issues/4822)) ([d558ef7](https://github.com/feast-dev/feast/commit/d558ef7e19aa561c37c38d4d0da2b8c1467414f5))
* Operator improvements ([#4928](https://github.com/feast-dev/feast/issues/4928)) ([7a1f4dd](https://github.com/feast-dev/feast/commit/7a1f4dd8b96a40d055467e1e5f72c91167e40484))
* Removing the tls_verify_client flag from feast cli for offline server. ([#4842](https://github.com/feast-dev/feast/issues/4842)) ([8320e23](https://github.com/feast-dev/feast/commit/8320e23eb85cc419ef8aa0fdc07efa81857e0345))
* Separating the RBAC and Remote related integration tests. ([#4905](https://github.com/feast-dev/feast/issues/4905)) ([76e1e21](https://github.com/feast-dev/feast/commit/76e1e2178c285886136e8f2fc4436302e4291715))
* Snyk vulnerability issues fix. ([#4867](https://github.com/feast-dev/feast/issues/4867)) ([dbc9207](https://github.com/feast-dev/feast/commit/dbc92070c8ef6b9e4e53d89ec03090bf30bd0f60)), closes [#6](https://github.com/feast-dev/feast/issues/6) [#3](https://github.com/feast-dev/feast/issues/3) [#4](https://github.com/feast-dev/feast/issues/4)
* Use ASOF JOIN in Snowflake offline store query ([#4850](https://github.com/feast-dev/feast/issues/4850)) ([8f591a2](https://github.com/feast-dev/feast/commit/8f591a235ba5bd9d1bc598195f46c7e12e437a2c))


### Reverts

* Revert "chore: Add Milvus to pr_integration_tests.yml" ([#4900](https://github.com/feast-dev/feast/issues/4900)) ([07958f7](https://github.com/feast-dev/feast/commit/07958f71cd89984325ec3ca2006b17fe5d333d02)), closes [#4891](https://github.com/feast-dev/feast/issues/4891)

# [0.42.0](https://github.com/feast-dev/feast/compare/v0.41.0...v0.42.0) (2024-12-05)


### Bug Fixes

* Add adapters for sqlite datetime conversion ([#4797](https://github.com/feast-dev/feast/issues/4797)) ([e198b17](https://github.com/feast-dev/feast/commit/e198b173be6355c1f169aeaae2b503f2273f23f1))
* Added grpcio extras to default feature-server image ([#4737](https://github.com/feast-dev/feast/issues/4737)) ([e9cd373](https://github.com/feast-dev/feast/commit/e9cd3733f041da99bb1e84843ffe5af697085c34))
* Changing node version in release ([7089918](https://github.com/feast-dev/feast/commit/7089918509404b3d217e7a2a0161293a8d6cb8aa))
* Feast create empty online table when FeatureView attribute online=False ([#4666](https://github.com/feast-dev/feast/issues/4666)) ([237c453](https://github.com/feast-dev/feast/commit/237c453c2da7d549b9bdb2c044ba284fbb9d9ba7))
* Fix db store types in Operator CRD ([#4798](https://github.com/feast-dev/feast/issues/4798)) ([f09339e](https://github.com/feast-dev/feast/commit/f09339eda24785d0a57feb4cf785f297d1a02ccb))
* Fix the config issue for postgres ([#4776](https://github.com/feast-dev/feast/issues/4776)) ([a36f7e5](https://github.com/feast-dev/feast/commit/a36f7e50d97c85595cbaa14165901924efa61cbb))
* Fixed example materialize-incremental and improved explanation ([#4734](https://github.com/feast-dev/feast/issues/4734)) ([ca8a7ab](https://github.com/feast-dev/feast/commit/ca8a7ab888b53fe43db6e6437e7070c83e00c10d))
* Fixed SparkSource docstrings so it wouldn't used inhereted class docstrings ([#4722](https://github.com/feast-dev/feast/issues/4722)) ([32e6aa1](https://github.com/feast-dev/feast/commit/32e6aa1e7c752551d455c5efd0974a938d756210))
* Fixing PGVector integration tests ([#4778](https://github.com/feast-dev/feast/issues/4778)) ([88a0320](https://github.com/feast-dev/feast/commit/88a03205a4ecbd875e808f6e8f86fef4f93e6da6))
* Incorrect type passed to assert_permissions in materialize endpoints ([#4727](https://github.com/feast-dev/feast/issues/4727)) ([b72c2da](https://github.com/feast-dev/feast/commit/b72c2daac80ac22d1d8160f155bb55a1bdbf16f7))
* Issue of DataSource subclasses using parent abstract class docstrings ([#4730](https://github.com/feast-dev/feast/issues/4730)) ([b24acd5](https://github.com/feast-dev/feast/commit/b24acd50149cb4737d5c27aa3236881f8ad26fea))
* Operator envVar positioning & tls.SecretRef.Name ([#4806](https://github.com/feast-dev/feast/issues/4806)) ([1115d96](https://github.com/feast-dev/feast/commit/1115d966df8ecff5553ae0c0879559f9ad735245))
* Populates project created_time correctly according to created ti… ([#4686](https://github.com/feast-dev/feast/issues/4686)) ([a61b93c](https://github.com/feast-dev/feast/commit/a61b93c666a79ec72b48d0927b2a4e1598f6650b))
* Reduce feast-server container image size & fix dev image build ([#4781](https://github.com/feast-dev/feast/issues/4781)) ([ccc9aea](https://github.com/feast-dev/feast/commit/ccc9aea6ee0a720c6dfddf9eaa6805e7b63fa7f1))
* Removed version func from feature_store.py ([#4748](https://github.com/feast-dev/feast/issues/4748)) ([f902bb9](https://github.com/feast-dev/feast/commit/f902bb9765a2efd4b1325de80e3b4f2101bb3911))
* Support registry instantiation for read-only users ([#4719](https://github.com/feast-dev/feast/issues/4719)) ([ca3d3c8](https://github.com/feast-dev/feast/commit/ca3d3c8f474ff6bf9f716c37df236bbc41bbd0d2))
* Syntax Error in BigQuery While Retrieving Columns that Start wit… ([#4713](https://github.com/feast-dev/feast/issues/4713)) ([60fbc62](https://github.com/feast-dev/feast/commit/60fbc62080950549f28b9411e00926be168bea56))
* Update release version in a pertinent Operator file ([#4708](https://github.com/feast-dev/feast/issues/4708)) ([764a8a6](https://github.com/feast-dev/feast/commit/764a8a657c045e99575bb8cfdc51afd9c61fa8e2))


### Features

* Add api contract to fastapi docs ([#4721](https://github.com/feast-dev/feast/issues/4721)) ([1a165c7](https://github.com/feast-dev/feast/commit/1a165c734ad8ee3923c786d80a00e4040cb1b1c8))
* Add Couchbase as an online store ([#4637](https://github.com/feast-dev/feast/issues/4637)) ([824859b](https://github.com/feast-dev/feast/commit/824859b813a1d756887f1006fb25914a2018d097))
* Add Operator support for spec.feastProject & status.applied fields ([#4656](https://github.com/feast-dev/feast/issues/4656)) ([430ac53](https://github.com/feast-dev/feast/commit/430ac535a5bd8311a485e51011a9602ca441d2d3))
* Add services functionality to Operator ([#4723](https://github.com/feast-dev/feast/issues/4723)) ([d1d80c0](https://github.com/feast-dev/feast/commit/d1d80c0d208e25b92047fe5f162c67c00c69bb43))
* Add TLS support to the Operator ([#4796](https://github.com/feast-dev/feast/issues/4796)) ([a617a6c](https://github.com/feast-dev/feast/commit/a617a6c8d67c6baaa6f9c1cc78b7799d72de48a3))
* Added feast Go operator db stores support ([#4771](https://github.com/feast-dev/feast/issues/4771)) ([3302363](https://github.com/feast-dev/feast/commit/3302363e2f149715e1c0fb5597d0b91a97756db2))
* Added support for setting env vars in feast services in feast controller  ([#4739](https://github.com/feast-dev/feast/issues/4739)) ([84b24b5](https://github.com/feast-dev/feast/commit/84b24b547e40bab4fad664bb77cd864613267aad))
* Adding docs outlining native Python transformations on singletons ([#4741](https://github.com/feast-dev/feast/issues/4741)) ([0150278](https://github.com/feast-dev/feast/commit/01502785109dfd64e3db03c855a34d9cab1a9073))
* Adding first feast operator e2e test. ([#4791](https://github.com/feast-dev/feast/issues/4791)) ([8339f8d](https://github.com/feast-dev/feast/commit/8339f8d55c7263becda42ab41961224091dee727))
* Adding github action to run the operator end-to-end tests. ([#4762](https://github.com/feast-dev/feast/issues/4762)) ([d8ccb00](https://github.com/feast-dev/feast/commit/d8ccb005ab8db0e79885b43aa430b78d1fbba379))
* Adding ssl support for registry server. ([#4718](https://github.com/feast-dev/feast/issues/4718)) ([ccf7a55](https://github.com/feast-dev/feast/commit/ccf7a55e11165f4663384c580003cb809b5e0f83))
* Adding SSL support for the React UI server and feast UI command. ([#4736](https://github.com/feast-dev/feast/issues/4736)) ([4a89252](https://github.com/feast-dev/feast/commit/4a89252cb18715458d724e5b54c77ed0de27cf3f))
* Adding support for native Python transformations on a single dictionary  ([#4724](https://github.com/feast-dev/feast/issues/4724)) ([9bbc1c6](https://github.com/feast-dev/feast/commit/9bbc1c61c7bbc38fce5568e6427257cf4d683fb2))
* Adding TLS support for offline server. ([#4744](https://github.com/feast-dev/feast/issues/4744)) ([5d8d03f](https://github.com/feast-dev/feast/commit/5d8d03ff2086256aa2977e5ec2ecdc048154dc1f))
* Building the feast image ([#4775](https://github.com/feast-dev/feast/issues/4775)) ([6635dde](https://github.com/feast-dev/feast/commit/6635dde9618d000d0567791018779fc188c893d8))
* File persistence definition and implementation ([#4742](https://github.com/feast-dev/feast/issues/4742)) ([3bad4a1](https://github.com/feast-dev/feast/commit/3bad4a135cdd9184f1b8e3c9c52470552cf2799d))
* Object store persistence in operator ([#4758](https://github.com/feast-dev/feast/issues/4758)) ([0ae86da](https://github.com/feast-dev/feast/commit/0ae86da3ab931832b0dfe357c0be82997d37430d))
* OIDC authorization in Feast Operator ([#4801](https://github.com/feast-dev/feast/issues/4801)) ([eb111d6](https://github.com/feast-dev/feast/commit/eb111d673ee5cea2cfadda55d0917a591cd6c377))
* Operator will create k8s serviceaccount for each feast service ([#4767](https://github.com/feast-dev/feast/issues/4767)) ([cde5760](https://github.com/feast-dev/feast/commit/cde5760cc94cccd4cbeed918acca09d1b106d7e5))
* Printing more verbose logs when we start the offline server  ([#4660](https://github.com/feast-dev/feast/issues/4660)) ([9d8d3d8](https://github.com/feast-dev/feast/commit/9d8d3d88a0ecccef4d610baf84f1b409276044dd))
* PVC configuration and impl ([#4750](https://github.com/feast-dev/feast/issues/4750)) ([785a190](https://github.com/feast-dev/feast/commit/785a190b50873bca2704c835027290787fe56656))
* Qdrant vectorstore support ([#4689](https://github.com/feast-dev/feast/issues/4689)) ([86573d2](https://github.com/feast-dev/feast/commit/86573d2778cb064fb7a930dfe08e84465084523f))
* RBAC Authorization in Feast Operator ([#4786](https://github.com/feast-dev/feast/issues/4786)) ([0ef5acc](https://github.com/feast-dev/feast/commit/0ef5acccc09a4a4a379a84cdacb0f5b7d9e8df70))
* Support for nested timestamp fields in Spark Offline store ([#4740](https://github.com/feast-dev/feast/issues/4740)) ([d4d94f8](https://github.com/feast-dev/feast/commit/d4d94f8ed76f72625305ad6e80337670664ba9b0))
* Update the go feature server from Expedia code repo. ([#4665](https://github.com/feast-dev/feast/issues/4665)) ([6406625](https://github.com/feast-dev/feast/commit/6406625ff8895fa65b11d587246f7d1f5feaecba))
* Updated feast Go operator db stores ([#4809](https://github.com/feast-dev/feast/issues/4809)) ([2c5a6b5](https://github.com/feast-dev/feast/commit/2c5a6b554cf6170b2590f32124cd7b84121cb864))
* Updated sample secret following review ([#4811](https://github.com/feast-dev/feast/issues/4811)) ([dc9f825](https://github.com/feast-dev/feast/commit/dc9f8259ee6a2043a6fce88ea0d0a5e59494ef76))

# [0.41.0](https://github.com/feast-dev/feast/compare/v0.40.0...v0.41.0) (2024-10-26)


* chore!: Update @elastic/eui and @emotion/react in Feast UI ([#4597](https://github.com/feast-dev/feast/issues/4597)) ([b9ddbf9](https://github.com/feast-dev/feast/commit/b9ddbf9351a55b1a5c102102b06ad3b2f28b3d1f))


### Bug Fixes

* Add --chdir to test_workflow.py ([#4453](https://github.com/feast-dev/feast/issues/4453)) ([6b2f026](https://github.com/feast-dev/feast/commit/6b2f026747b8adebe659aed3d4d2f95d551d5d1e))
* Add feast-operator files to semantic-release script ([#4382](https://github.com/feast-dev/feast/issues/4382)) ([8eceff2](https://github.com/feast-dev/feast/commit/8eceff26ba00fd446d27ad5ce2ee9d039c57fd9a))
* Add feast-operator Makefile to semantic-release script ([#4424](https://github.com/feast-dev/feast/issues/4424)) ([d18d01d](https://github.com/feast-dev/feast/commit/d18d01de8356ffdc29b35c93978f36e7541d415e))
* Added Offline Store Arrow client errors handler ([#4524](https://github.com/feast-dev/feast/issues/4524)) ([7535b40](https://github.com/feast-dev/feast/commit/7535b4036ce980c9a05bc33a9e61a7938ea1303e))
* Added Online Store REST client errors handler ([#4488](https://github.com/feast-dev/feast/issues/4488)) ([2118719](https://github.com/feast-dev/feast/commit/21187199173f4c4f5417205d99535af6be492a9a))
* Added Permission API docs ([#4485](https://github.com/feast-dev/feast/issues/4485)) ([2bd03fa](https://github.com/feast-dev/feast/commit/2bd03fa4da5e76f6b29b0b54b455d5552d256838))
* Added support for multiple name patterns to Permissions ([#4633](https://github.com/feast-dev/feast/issues/4633)) ([f05e928](https://github.com/feast-dev/feast/commit/f05e92861da64d1c5e9cfe3c6307b3422d0d83b8))
* Adding protobuf<5 as a required dependency due to snowflake limitations ([#4537](https://github.com/feast-dev/feast/issues/4537)) ([cecca83](https://github.com/feast-dev/feast/commit/cecca8360bed62ab2f4fddc5d3a888247ea0a87a))
* Avoid the python 3.9+ threadpool cleanup bug ([#4627](https://github.com/feast-dev/feast/issues/4627)) ([ba05893](https://github.com/feast-dev/feast/commit/ba05893ba6db2d8d1e7bcc8cf8162f4fb72c9563))
* Bigquery dataset create table disposition ([#4649](https://github.com/feast-dev/feast/issues/4649)) ([58e03d1](https://github.com/feast-dev/feast/commit/58e03d17448a883ef57bcd6d8926d1e54ddcdece))
* Changes template file path to relative path ([#4624](https://github.com/feast-dev/feast/issues/4624)) ([3e313b1](https://github.com/feast-dev/feast/commit/3e313b15efc7fc72d35d70315fc8b7c172fc7993))
* Check for snowflake functions when setting up materialization engine ([#4456](https://github.com/feast-dev/feast/issues/4456)) ([c365b4e](https://github.com/feast-dev/feast/commit/c365b4e71a16fb69883608c5f781c6d55502bb8e))
* Correctly handle list values in _python_value_to_proto_value ([#4608](https://github.com/feast-dev/feast/issues/4608)) ([c0a1026](https://github.com/feast-dev/feast/commit/c0a10269914c2ca01fe1cf6b24b120bfa58d04e7))
* Default to pandas mode if not specified in ODFV proto in database ([#4420](https://github.com/feast-dev/feast/issues/4420)) ([d235832](https://github.com/feast-dev/feast/commit/d235832b78027b98df8e8a9e434a51a0c78b3092))
* Deleting data from feast_metadata when we delete project ([#4550](https://github.com/feast-dev/feast/issues/4550)) ([351a2d0](https://github.com/feast-dev/feast/commit/351a2d0a7f9808178ab9d201083eb2894ce7384f))
* Disable active_timer When registry_ttl_sec is 0 ([#4499](https://github.com/feast-dev/feast/issues/4499)) ([c94f32f](https://github.com/feast-dev/feast/commit/c94f32f2b637c7b7d917d2456432180af7569cf5))
* Escape special characters in the Postgres password ([#4394](https://github.com/feast-dev/feast/issues/4394)) ([419ca5e](https://github.com/feast-dev/feast/commit/419ca5e9523ff38f27141b79ae12ebb0646c6617))
* FeastExtrasDependencyImportError when using SparkOfflineStore without S3 ([#4594](https://github.com/feast-dev/feast/issues/4594)) ([1ba94f7](https://github.com/feast-dev/feast/commit/1ba94f7e2018fea0114f1703dd3942d589071825))
* Fix Feast project name test ([#4685](https://github.com/feast-dev/feast/issues/4685)) ([9f41fd6](https://github.com/feast-dev/feast/commit/9f41fd6673b9576c802c2378b56e04b9a090d99d))
* Fix for SQL registry initialization fails [#4543](https://github.com/feast-dev/feast/issues/4543) ([#4544](https://github.com/feast-dev/feast/issues/4544)) ([4e2eacc](https://github.com/feast-dev/feast/commit/4e2eacc1beea8f8866b78968abadfd42eee63d6a))
* Fix gitignore issue ([#4674](https://github.com/feast-dev/feast/issues/4674)) ([2807dfa](https://github.com/feast-dev/feast/commit/2807dfaf46d3d9e79f84b0ff22dbaeede377c89b))
* Fix online pg import ([#4581](https://github.com/feast-dev/feast/issues/4581)) ([1f17caa](https://github.com/feast-dev/feast/commit/1f17caacdaa573d08dbf8dc68b20e73a187ed8a4))
* Fix the mypy type check issue. ([#4498](https://github.com/feast-dev/feast/issues/4498)) ([7ecc615](https://github.com/feast-dev/feast/commit/7ecc615945b7bb48e103ca6eb278b39759d71c5a))
* Fix vector store config ([#4583](https://github.com/feast-dev/feast/issues/4583)) ([11c00d4](https://github.com/feast-dev/feast/commit/11c00d43fd1b2c5caf4d49f705bd55c704edae8a))
* Fixes validator field access for 'project_id' in BigQuery offline Store ([#4509](https://github.com/feast-dev/feast/issues/4509)) ([9a0398e](https://github.com/feast-dev/feast/commit/9a0398e2e18585172d857cf3202a81551d31609b))
* Fixing failure of protos during ODFV transformations for missing entities ([#4667](https://github.com/feast-dev/feast/issues/4667)) ([41aaeeb](https://github.com/feast-dev/feast/commit/41aaeebaa5908f44cda28a4410e2fca412f53e92))
* Fixing the master branch build failure. ([#4563](https://github.com/feast-dev/feast/issues/4563)) ([0192b2e](https://github.com/feast-dev/feast/commit/0192b2eb245c8e0ea9a913195ddf28382dc23982))
* Hao xu request source timestamp_field ([#4495](https://github.com/feast-dev/feast/issues/4495)) ([96344b2](https://github.com/feast-dev/feast/commit/96344b2b6830dcc280567542d111d1b0f39879e0))
* Ignore the type check as both functions calls are not belonging to Feast code. ([#4500](https://github.com/feast-dev/feast/issues/4500)) ([867f532](https://github.com/feast-dev/feast/commit/867f532154977790e3bb11f2a94baa4f2289de99))
* Import grpc only for type checking in errors.py ([#4533](https://github.com/feast-dev/feast/issues/4533)) ([f308572](https://github.com/feast-dev/feast/commit/f308572715d0593951f71bb3da5c5be6de29a2f9))
* Initial commit targetting grpc registry server ([#4458](https://github.com/feast-dev/feast/issues/4458)) ([484240c](https://github.com/feast-dev/feast/commit/484240c4e783d68bc521b62b723c2dcbd00fab5e)), closes [#4465](https://github.com/feast-dev/feast/issues/4465)
* Links to the RBAC documentation under Concepts and Components ([#4430](https://github.com/feast-dev/feast/issues/4430)) ([0a48f7b](https://github.com/feast-dev/feast/commit/0a48f7bb436febb0171c78a559a577eedeff421f))
* Locate feature_store.yaml from __file__ ([#4443](https://github.com/feast-dev/feast/issues/4443)) ([20290ce](https://github.com/feast-dev/feast/commit/20290ce28c513f705db1dbb6b0f719ba1846217f))
* Logger settings for feature servers and updated logger for permission flow ([#4531](https://github.com/feast-dev/feast/issues/4531)) ([50b8f23](https://github.com/feast-dev/feast/commit/50b8f238b6f9adbc9ff0b20e18b78b2948c2f440))
* Move tslib from devDependencies to dependencies in Feast UI ([#4525](https://github.com/feast-dev/feast/issues/4525)) ([c5a4d90](https://github.com/feast-dev/feast/commit/c5a4d907bf34f4cf7477b212cd2820b0e7d24b7b))
* Null value compatibility for unit timestamp list value type ([#4378](https://github.com/feast-dev/feast/issues/4378)) ([8f264b6](https://github.com/feast-dev/feast/commit/8f264b6807a07874dc01207c655baeef7dfaa7b2))
* Patch FAISS online return signature ([#4671](https://github.com/feast-dev/feast/issues/4671)) ([0d45e95](https://github.com/feast-dev/feast/commit/0d45e95767773846861d53feb4af4c6bc1451b5e))
* Quickstart documentation changes ([#4618](https://github.com/feast-dev/feast/issues/4618)) ([7ac0908](https://github.com/feast-dev/feast/commit/7ac0908f3846cc0ab05082f748506814c84b2e9c))
* Refactor auth_client_manager_factory.py in function get_auth_client_m… ([#4505](https://github.com/feast-dev/feast/issues/4505)) ([def8633](https://github.com/feast-dev/feast/commit/def863360bf0e553d242900ee915e953c6c3f9b6))
* Remote apply using offline store ([#4559](https://github.com/feast-dev/feast/issues/4559)) ([ac62a32](https://github.com/feast-dev/feast/commit/ac62a323c86fba8096eefde85775dd7a857e9e25))
* Remove Feast UI TypeScript dependencies from `peerDependencies` and `dependencies` ([#4554](https://github.com/feast-dev/feast/issues/4554)) ([e781e16](https://github.com/feast-dev/feast/commit/e781e1652cadc6576dbab369248d6e4afdb5f158))
* Remove unnecessary peer dependencies from Feast UI ([#4577](https://github.com/feast-dev/feast/issues/4577)) ([9ac7f4e](https://github.com/feast-dev/feast/commit/9ac7f4ea77357219fc8420f05e493767bc5357c2))
* Removed protobuf as a required dependency ([#4535](https://github.com/feast-dev/feast/issues/4535)) ([0fb76e9](https://github.com/feast-dev/feast/commit/0fb76e9041885659c68e294b0c033c62050bd374))
* Removed the k8s dependency from required dependencies ([#4519](https://github.com/feast-dev/feast/issues/4519)) ([3073ea5](https://github.com/feast-dev/feast/commit/3073ea5911339a5744be45512a9a2ee8b250292b))
* Removed usage of pull_request_target as much as possible to prevent security concerns ([#4549](https://github.com/feast-dev/feast/issues/4549)) ([3198371](https://github.com/feast-dev/feast/commit/3198371fc0e07f6b51b62c7e3abbc48729078bb9))
* Replaced ClusterRoles with local RoleBindings ([#4625](https://github.com/feast-dev/feast/issues/4625)) ([ca9fb9b](https://github.com/feast-dev/feast/commit/ca9fb9bc8c3f3b06b4ba5fce362e26633144715c))
* Retire pytz library ([#4406](https://github.com/feast-dev/feast/issues/4406)) ([23c6c86](https://github.com/feast-dev/feast/commit/23c6c862e1da4e9523530eb48c7ce79319dc442d))
* Typos related to k8s ([#4442](https://github.com/feast-dev/feast/issues/4442)) ([dda0088](https://github.com/feast-dev/feast/commit/dda0088f25eab5828613bd6d080aeddf681641f0))
* Update java testcontainers to use Compose V2 ([#4381](https://github.com/feast-dev/feast/issues/4381)) ([9a33fce](https://github.com/feast-dev/feast/commit/9a33fce695c54226b3afd7b998e284f358bab141))
* Update min versions for pyarrow and protobuf ([#4646](https://github.com/feast-dev/feast/issues/4646)) ([c7ddd4b](https://github.com/feast-dev/feast/commit/c7ddd4bb48290d6d9327fcb6349b84b4d14334af))
* Update react-router-dom to 6.3.0 and restrict its version in Feast UI ([#4556](https://github.com/feast-dev/feast/issues/4556)) ([4293608](https://github.com/feast-dev/feast/commit/42936084a7d214d65faea5359ae70eefda8d23ad)), closes [#3794](https://github.com/feast-dev/feast/issues/3794) [/github.com/remix-run/react-router/blob/main/CHANGELOG.md#v630](https://github.com//github.com/remix-run/react-router/blob/main/CHANGELOG.md/issues/v630)
* Update the base image for feature-server. ([#4576](https://github.com/feast-dev/feast/issues/4576)) ([0390d8a](https://github.com/feast-dev/feast/commit/0390d8a86f50360a4df89165db62972328d22ca4))
* Update the base image of materilization engine. ([#4580](https://github.com/feast-dev/feast/issues/4580)) ([f8592d8](https://github.com/feast-dev/feast/commit/f8592d86b2903ebfebc505bbf9392927aae5609c))
* Updated README link ([#4669](https://github.com/feast-dev/feast/issues/4669)) ([35fbdc9](https://github.com/feast-dev/feast/commit/35fbdc91cc8a327be2a8cd162f7ccff983b16932))
* Updating the documentation and adding tests for project length ([#4628](https://github.com/feast-dev/feast/issues/4628)) ([945b0fa](https://github.com/feast-dev/feast/commit/945b0faadd40c8dc76d104bce14ee902bd513127))
* Using get_type_hints instead of inspect signature for udf return annotation  ([#4391](https://github.com/feast-dev/feast/issues/4391)) ([3a32e8a](https://github.com/feast-dev/feast/commit/3a32e8ae28110db0934fc26ec6992eb606fed012))
* Using repo_config parameter in teardown to allow for feature-store-yaml overrides ([#4413](https://github.com/feast-dev/feast/issues/4413)) ([0baeeb5](https://github.com/feast-dev/feast/commit/0baeeb5ec524c1e6209edab9605ca8a098a2ec88))
* Validating permission to update an existing request on both the new and the old instance ([#4449](https://github.com/feast-dev/feast/issues/4449)) ([635a01b](https://github.com/feast-dev/feast/commit/635a01b4c77db781d67f9f5ebb1067806b1e2a13))


### Features

* Add boto3 session based auth for dynamodb online store for cross account access ([#4606](https://github.com/feast-dev/feast/issues/4606)) ([00eaf74](https://github.com/feast-dev/feast/commit/00eaf744f50f56f449c7ad8f6ceb92a09304ee0b))
* Add cli list/describe for SavedDatasets, StreamFeatureViews, & … ([#4487](https://github.com/feast-dev/feast/issues/4487)) ([7b250e5](https://github.com/feast-dev/feast/commit/7b250e5eff5de56f5c5da103e91051276940298a))
* Add connection_name field to Snowflake config ([#4600](https://github.com/feast-dev/feast/issues/4600)) ([10ce2aa](https://github.com/feast-dev/feast/commit/10ce2aa0050e419acfae27971a6fff87bade3ba4))
* Add health check service to registry server ([#4421](https://github.com/feast-dev/feast/issues/4421)) ([46655f0](https://github.com/feast-dev/feast/commit/46655f06de339ee09245de3e1a648eb4e3bcd729))
* Add more __repr__ methods ([#4676](https://github.com/feast-dev/feast/issues/4676)) ([e726c09](https://github.com/feast-dev/feast/commit/e726c096f2de93d6dc0a807c97c47476cc79dd61))
* Add registry methods for dealing with all FV types ([#4435](https://github.com/feast-dev/feast/issues/4435)) ([ac381b2](https://github.com/feast-dev/feast/commit/ac381b292cfa29804ee5f0822f876d227a0989d9))
* Added Project object to Feast Objects ([#4475](https://github.com/feast-dev/feast/issues/4475)) ([4a6b663](https://github.com/feast-dev/feast/commit/4a6b663f80bc91d6de35ed2ec428d34811d17a18))
* Added support for reading from Reader  Endpoints for AWS Aurora use cases ([#4494](https://github.com/feast-dev/feast/issues/4494)) ([d793c77](https://github.com/feast-dev/feast/commit/d793c77d923df95a186b9d4829b167f1a5a304e6))
* Adding documentation for On Demand Feature Transformations with writes ([#4607](https://github.com/feast-dev/feast/issues/4607)) ([8e0c1b5](https://github.com/feast-dev/feast/commit/8e0c1b51665357d44229239401a04b76396b9047))
* Adding mode='python' for get_historical_features on ODFVs ([#4653](https://github.com/feast-dev/feast/issues/4653)) ([c40d539](https://github.com/feast-dev/feast/commit/c40d539b85fe537077dd26904d78624da3d33951))
* Adding registry cache support for get_on_demand_feature_view ([#4572](https://github.com/feast-dev/feast/issues/4572)) ([354c059](https://github.com/feast-dev/feast/commit/354c059e5475f9c3927d9180a421118507a22cf0))
* Adding SSL support for online server ([#4677](https://github.com/feast-dev/feast/issues/4677)) ([80a5b3c](https://github.com/feast-dev/feast/commit/80a5b3c499faca8625d60267a34dcbddfe0c042a))
* Adding write capability to online store to on demand feature views ([#4585](https://github.com/feast-dev/feast/issues/4585)) ([ef9e0bb](https://github.com/feast-dev/feast/commit/ef9e0bbdb2a80250786b87972a53c4cf5890bb76)), closes [#4603](https://github.com/feast-dev/feast/issues/4603)
* Allow feast snowflake to read in byte string for private-key authentication ([#4384](https://github.com/feast-dev/feast/issues/4384)) ([5215a21](https://github.com/feast-dev/feast/commit/5215a2139a9d824dc2d8f45181bd177a1e8e9561))
* An action to test operator at PR time ([#4635](https://github.com/feast-dev/feast/issues/4635)) ([14c1000](https://github.com/feast-dev/feast/commit/14c1000554c590cb89ecb5ef44c57aa1b5dd1387))
* Create ADOPTERS.md ([#4410](https://github.com/feast-dev/feast/issues/4410)) ([721ec74](https://github.com/feast-dev/feast/commit/721ec74f17ee95e375054f21135e54e0687104a7))
* Create initial structure of Feast Go Operator ([#4596](https://github.com/feast-dev/feast/issues/4596)) ([b5ab6c7](https://github.com/feast-dev/feast/commit/b5ab6c799d529aaea19a196cedca7bb2c93cbbe9))
* Faiss and In memory store ([#4464](https://github.com/feast-dev/feast/issues/4464)) ([a1ff129](https://github.com/feast-dev/feast/commit/a1ff1290002376b4c9ac8ad14e60df4622bb47a4))
* Feast Security Model (aka RBAC) ([#4380](https://github.com/feast-dev/feast/issues/4380)) ([1771f66](https://github.com/feast-dev/feast/commit/1771f668247ef3b46ea7dac634e557e249bc1ba9)), closes [#36](https://github.com/feast-dev/feast/issues/36)
* Instrument Feast using Prometheus and OpenTelemetry ([#4366](https://github.com/feast-dev/feast/issues/4366)) ([a571e08](https://github.com/feast-dev/feast/commit/a571e08b97a95f8543d7dea27902c135ab3a4378))
* Intra server to server communication ([#4433](https://github.com/feast-dev/feast/issues/4433)) ([729c874](https://github.com/feast-dev/feast/commit/729c874e8c30719f23ad287d3cb84f1d654274ec))
* Publish TypeScript types in Feast UI package ([#4551](https://github.com/feast-dev/feast/issues/4551)) ([334e5d7](https://github.com/feast-dev/feast/commit/334e5d78855709d4ca56619f16eecb414f88ce2d))
* Refactoring code to get oidc end points from discovery URL. ([#4429](https://github.com/feast-dev/feast/issues/4429)) ([896360a](https://github.com/feast-dev/feast/commit/896360af19a37c9a2a4634ec88021c4f69bdb141))
* Return entity key in the retrieval document api ([#4511](https://github.com/feast-dev/feast/issues/4511)) ([5f5caf0](https://github.com/feast-dev/feast/commit/5f5caf0cac539ed779692e0ec819659cf5a33a0d))
* Update roadmap.md ([#4445](https://github.com/feast-dev/feast/issues/4445)) ([34238d2](https://github.com/feast-dev/feast/commit/34238d2a0bfe9dbad753fec9613c83d848b1a520))
* Update sqlite-vec package ([#4389](https://github.com/feast-dev/feast/issues/4389)) ([b734cb1](https://github.com/feast-dev/feast/commit/b734cb147a4afd28407ec57d95f70ff604f82954))
* Updated Feast model Inference Architecture ([#4570](https://github.com/feast-dev/feast/issues/4570)) ([8cd0dcf](https://github.com/feast-dev/feast/commit/8cd0dcff0cc3b1387c7ca65018ebb09e03538242))
* Updating docs to include model inference guidelines ([#4416](https://github.com/feast-dev/feast/issues/4416)) ([cebbe04](https://github.com/feast-dev/feast/commit/cebbe045597b85e1ae4394a8c14741e88347a6b8))
* Updating FeatureViewProjection and OnDemandFeatureView to add batch_source and entities ([#4530](https://github.com/feast-dev/feast/issues/4530)) ([0795496](https://github.com/feast-dev/feast/commit/07954960c5501e2ecc1f1285ddf4aa68f9ac880b))
* Upgrade React from 17.0.2 to 18.3.1 in Feast UI ([#4620](https://github.com/feast-dev/feast/issues/4620)) ([d6f3cb8](https://github.com/feast-dev/feast/commit/d6f3cb81a4c2a51bae2d29a185753cf6b1d2b16d))


### Performance Improvements

* Add init and cleanup of long lived resources ([#4642](https://github.com/feast-dev/feast/issues/4642)) ([47dc04d](https://github.com/feast-dev/feast/commit/47dc04d43f483944f97248abaccd15dba319281f))
* Added indexes to sql tables to optimize query execution ([#4538](https://github.com/feast-dev/feast/issues/4538)) ([9688790](https://github.com/feast-dev/feast/commit/9688790a5e7a70f628a46021bde0201922c7e04d))
* Default to async endpoints, use threadpool for sync ([#4647](https://github.com/feast-dev/feast/issues/4647)) ([c1f1912](https://github.com/feast-dev/feast/commit/c1f19127ab5c22dfe67869990480e7e9d8183ab1))
* Implement dynamo write_batch_async ([#4675](https://github.com/feast-dev/feast/issues/4675)) ([ba4404c](https://github.com/feast-dev/feast/commit/ba4404cecfb196c4084f9bf892cd3528184d42c1))
* Make /push async ([#4650](https://github.com/feast-dev/feast/issues/4650)) ([61abf89](https://github.com/feast-dev/feast/commit/61abf894aca7aa52042c40e77f64b49835f4324e))
* Parallelize read calls by table and batch ([#4619](https://github.com/feast-dev/feast/issues/4619)) ([043eff1](https://github.com/feast-dev/feast/commit/043eff1a87bdf775a437503395acda87cbecf875))


### BREAKING CHANGES

* Consuming apps that use @elastic/eui should update it
to a compatible version. If you use @elastic/eui components that have
been renamed or replaced with others, you'll need to update your code
accordingly.

Signed-off-by: Harri Lehtola <peruukki@hotmail.com>

* chore: Update Node version from 17 to 20 in UI unit tests

Node 17 is not an LTS (long-term support) version and apparently
rejected by the latest versions of Elastic UI:

> error @elastic/eui@95.12.0: The engine "node" is incompatible with
> this module. Expected version "16.x || 18.x || >=20.x". Got "17.9.1"

Let's try with the latest LTS version.

Signed-off-by: Harri Lehtola <peruukki@hotmail.com>

# [0.40.0](https://github.com/feast-dev/feast/compare/v0.39.0...v0.40.0) (2024-07-31)


### Bug Fixes

* Added missing type ([#4315](https://github.com/feast-dev/feast/issues/4315)) ([86af60a](https://github.com/feast-dev/feast/commit/86af60ad87d537b17e4ce6ec7a5eac0d637fb32d))
* Avoid XSS attack from Jinjin2's Environment(). ([#4355](https://github.com/feast-dev/feast/issues/4355)) ([40270e7](https://github.com/feast-dev/feast/commit/40270e754660d0a8f57cc8a3bbfb1e1e346c3d86))
* CGO Memory leak issue in GO Feature server ([#4291](https://github.com/feast-dev/feast/issues/4291)) ([43e198f](https://github.com/feast-dev/feast/commit/43e198f6945c5e868ade341309f2c5ca39ac563e))
* Deprecated the datetime.utcfromtimestamp(). ([#4306](https://github.com/feast-dev/feast/issues/4306)) ([21deec8](https://github.com/feast-dev/feast/commit/21deec8495a101442e78cabc9a30cb5fbee5382f))
* Fix SQLite import issue ([#4294](https://github.com/feast-dev/feast/issues/4294)) ([398ea3b](https://github.com/feast-dev/feast/commit/398ea3b86c83605963124404ff4baa95162dc1f4))
* Increment operator to v0.39.0 ([#4368](https://github.com/feast-dev/feast/issues/4368)) ([3ddb4fb](https://github.com/feast-dev/feast/commit/3ddb4fb90d845bb3113cc51c484938579668d2c5))
* Minor typo in the unit test. ([#4296](https://github.com/feast-dev/feast/issues/4296)) ([6c75e84](https://github.com/feast-dev/feast/commit/6c75e84b036f84910dcbd7f1733ebd0d8839ab6c))
* OnDemandFeatureView type inference for array types ([#4310](https://github.com/feast-dev/feast/issues/4310)) ([c45ff72](https://github.com/feast-dev/feast/commit/c45ff72f821404c595477e696ab4be1b888090cc))
* Remove redundant batching in PostgreSQLOnlineStore.online_write_batch and fix progress bar ([#4331](https://github.com/feast-dev/feast/issues/4331)) ([0d89d15](https://github.com/feast-dev/feast/commit/0d89d1519fc6b8ddd05a2588138e2e85f5a921b1))
* Remove typo. ([#4351](https://github.com/feast-dev/feast/issues/4351)) ([92d17de](https://github.com/feast-dev/feast/commit/92d17def8cdff2bebfa622a4b3846d5bdc3e58d8))
* Retire the datetime.utcnow(). ([#4352](https://github.com/feast-dev/feast/issues/4352)) ([a8bc696](https://github.com/feast-dev/feast/commit/a8bc696010fa94fa0be44fba2570bee0eab83ba2))
* Update dask version to support pandas 1.x ([#4326](https://github.com/feast-dev/feast/issues/4326)) ([a639d61](https://github.com/feast-dev/feast/commit/a639d617c047030f75c6950e9bfa6e5cfe63daaa))
* Update Feast object metadata in the registry ([#4257](https://github.com/feast-dev/feast/issues/4257)) ([8028ae0](https://github.com/feast-dev/feast/commit/8028ae0f39e706637bc2781850a3b7d8925a87f7))
* Using one single function call for utcnow(). ([#4307](https://github.com/feast-dev/feast/issues/4307)) ([98ff63c](https://github.com/feast-dev/feast/commit/98ff63cd389207998b3452ec46e5a2f0fc70485c))


### Features

* Add async feature retrieval for Postgres Online Store ([#4327](https://github.com/feast-dev/feast/issues/4327)) ([cea52e9](https://github.com/feast-dev/feast/commit/cea52e9fb02cb9e0b8f48206278474f5a5fa167e))
* Add Async refresh to Sql Registry ([#4251](https://github.com/feast-dev/feast/issues/4251)) ([f569786](https://github.com/feast-dev/feast/commit/f5697863669a6bb9dbd491f79192e8ddd0073388))
* Add SingleStore as an OnlineStore ([#4285](https://github.com/feast-dev/feast/issues/4285)) ([2c38946](https://github.com/feast-dev/feast/commit/2c3894693e9079b8ad7873b139b30440c919e913))
* Add Tornike to maintainers.md ([#4339](https://github.com/feast-dev/feast/issues/4339)) ([8e8c1f2](https://github.com/feast-dev/feast/commit/8e8c1f2ff9a77738e71542cbaab9531f321842a4))
* Bump psycopg2 to psycopg3 for all Postgres components ([#4303](https://github.com/feast-dev/feast/issues/4303)) ([9451d9c](https://github.com/feast-dev/feast/commit/9451d9ca15f234e8e16e81351294fd63b33c1af2))
* Entity key deserialization ([#4284](https://github.com/feast-dev/feast/issues/4284)) ([83fad15](https://github.com/feast-dev/feast/commit/83fad152ffe01a3b2691095a45b90eb30044c859))
* Ignore paths feast apply ([#4276](https://github.com/feast-dev/feast/issues/4276)) ([b4d54af](https://github.com/feast-dev/feast/commit/b4d54afaa83cb3e1391d62f4243e7d63a698064c))
* Move get_online_features to OnlineStore interface ([#4319](https://github.com/feast-dev/feast/issues/4319)) ([7072fd0](https://github.com/feast-dev/feast/commit/7072fd0e2e1d2f4d9a3e8f02d04ae042b3d9c0d4))
* Port mssql contrib offline store to ibis ([#4360](https://github.com/feast-dev/feast/issues/4360)) ([7914cbd](https://github.com/feast-dev/feast/commit/7914cbdaffeade727cf3cee538cf128cbfd86e06))


### Reverts

* Revert "fix: Avoid XSS attack from Jinjin2's Environment()." ([#4357](https://github.com/feast-dev/feast/issues/4357)) ([cdeab48](https://github.com/feast-dev/feast/commit/cdeab486970ccb8c716499610f927a6e8eb14457)), closes [#4355](https://github.com/feast-dev/feast/issues/4355)

# [0.39.0](https://github.com/feast-dev/feast/compare/v0.38.0...v0.39.0) (2024-06-18)


### Bug Fixes

* Feast UI importlib change ([#4248](https://github.com/feast-dev/feast/issues/4248)) ([5d486b8](https://github.com/feast-dev/feast/commit/5d486b8a53d799a49cc93e0f5a41aede3bc753ef))
* Feature server no_feature_log argument error ([#4255](https://github.com/feast-dev/feast/issues/4255)) ([15524ce](https://github.com/feast-dev/feast/commit/15524cec6ba81ab6aae24b390ee63868c60c49e5))
* Feature UI Server image won't start in an OpenShift cluster ([#4250](https://github.com/feast-dev/feast/issues/4250)) ([4891f76](https://github.com/feast-dev/feast/commit/4891f766f79a1863293412664ff8592a9e446785))
* Handles null values in data during GO Feature retrieval ([#4274](https://github.com/feast-dev/feast/issues/4274)) ([c491e57](https://github.com/feast-dev/feast/commit/c491e5726d792f731f961b66fdf6c0b772165e86))
* Make Java gRPC client use timeouts as expected ([#4237](https://github.com/feast-dev/feast/issues/4237)) ([f5a37c1](https://github.com/feast-dev/feast/commit/f5a37c1ce129620a4e3ee4fbe88425017f5a5ec2))
* Remove self assignment code line. ([#4238](https://github.com/feast-dev/feast/issues/4238)) ([e514f66](https://github.com/feast-dev/feast/commit/e514f66a4c39f31bed969721bfe5c9c563786693))
* Set default values for feature_store.serve() function ([#4225](https://github.com/feast-dev/feast/issues/4225)) ([fa74438](https://github.com/feast-dev/feast/commit/fa744380ad48ee394a05b2f600db5fb0a99c03aa))


### Features

* Add online_read_async for dynamodb ([#4244](https://github.com/feast-dev/feast/issues/4244)) ([b5ef384](https://github.com/feast-dev/feast/commit/b5ef3843499f575d4cacb9988b98b9778f67ee3b))
* Add the ability to list objects by `tags` ([#4246](https://github.com/feast-dev/feast/issues/4246)) ([fbf92da](https://github.com/feast-dev/feast/commit/fbf92da6a4468759bfc9738f9ca581f047efb2b0))
* Added deadline to gRPC Java client ([#4217](https://github.com/feast-dev/feast/issues/4217)) ([ff429c9](https://github.com/feast-dev/feast/commit/ff429c9f85c99478d9814e698522905d23e8d384))
* Adding vector search for sqlite ([#4176](https://github.com/feast-dev/feast/issues/4176)) ([2478831](https://github.com/feast-dev/feast/commit/2478831e8204bc8b9204ba048a74179ac3193367))
* Change get_online_features signature, move online retrieval functions to utils ([#4278](https://github.com/feast-dev/feast/issues/4278)) ([7287662](https://github.com/feast-dev/feast/commit/7287662f25117660160441bd61c9109b63a20d0d))
* Feature/adding remote online store ([#4226](https://github.com/feast-dev/feast/issues/4226)) ([9454d7c](https://github.com/feast-dev/feast/commit/9454d7cb8901c59f5e7c95096cd0078cbbe953fd))
* List all feature views ([#4256](https://github.com/feast-dev/feast/issues/4256)) ([36a574d](https://github.com/feast-dev/feast/commit/36a574d6788afca5fe2fb8776386c9462cb2ff24))
* Make RegistryServer writable ([#4231](https://github.com/feast-dev/feast/issues/4231)) ([79e1143](https://github.com/feast-dev/feast/commit/79e11439688650bc5dc62a6fa9a9a6f54c214a50))
* Remote offline Store  ([#4262](https://github.com/feast-dev/feast/issues/4262)) ([28a3d24](https://github.com/feast-dev/feast/commit/28a3d24b12b35e4154df2bfd66dedb80bcfa3292))
* Set optional full-scan for deletion ([#4189](https://github.com/feast-dev/feast/issues/4189)) ([b9cadd5](https://github.com/feast-dev/feast/commit/b9cadd53250f619f5ffd39232efef5461f156fde))

# [0.38.0](https://github.com/feast-dev/feast/compare/v0.37.0...v0.38.0) (2024-05-24)


### Bug Fixes

* Add vector database doc ([#4165](https://github.com/feast-dev/feast/issues/4165)) ([37f36b6](https://github.com/feast-dev/feast/commit/37f36b681bde0c1ae83303803c89d3ed0b2ac8a9))
* Change checkout action back to v3 from v5 which isn't released yet ([#4147](https://github.com/feast-dev/feast/issues/4147)) ([9523fff](https://github.com/feast-dev/feast/commit/9523fff2dda2e0d53bffa7f5c0d6f2f69f6b8c02))
* Change numpy version <1.25 dependency to <2 in setup.py ([#4085](https://github.com/feast-dev/feast/issues/4085)) ([2ba71ff](https://github.com/feast-dev/feast/commit/2ba71fff5f76ed05066e94f3b11d08bc30b54b39)), closes [#4084](https://github.com/feast-dev/feast/issues/4084)
* Changed the code the way mysql container is initialized.  ([#4140](https://github.com/feast-dev/feast/issues/4140)) ([8b5698f](https://github.com/feast-dev/feast/commit/8b5698fefa965fc08fdb5e07d739d0ca276a3522)), closes [#4126](https://github.com/feast-dev/feast/issues/4126)
* Correct nightly install command, move all installs to uv ([#4164](https://github.com/feast-dev/feast/issues/4164)) ([c86d594](https://github.com/feast-dev/feast/commit/c86d594613b0fb1425451def4fc1d7a7496eea92))
* Default value is not set in Redis connection string using environment variable ([#4136](https://github.com/feast-dev/feast/issues/4136)) ([95acfb4](https://github.com/feast-dev/feast/commit/95acfb4cefc10f96f8ed61f148e24b238d400a68)), closes [#3669](https://github.com/feast-dev/feast/issues/3669)
* Get container host addresses from testcontainers (java) ([#4125](https://github.com/feast-dev/feast/issues/4125)) ([9184dde](https://github.com/feast-dev/feast/commit/9184dde1fcd57de5765c850615eb5e70cbafe70f))
* Get rid of empty string `name_alias` during feature view projection deserialization  ([#4116](https://github.com/feast-dev/feast/issues/4116)) ([65056ce](https://github.com/feast-dev/feast/commit/65056cea6c4537834a1c40be2ad37e1659310a47))
* Helm chart `feast-feature-server`, improve Service template name ([#4161](https://github.com/feast-dev/feast/issues/4161)) ([dedc164](https://github.com/feast-dev/feast/commit/dedc1645ef1f38aa9b50a0cf55e4bc23ec60d5ad))
* Improve the code related to on-demand-featureview. ([#4203](https://github.com/feast-dev/feast/issues/4203)) ([d91d7e0](https://github.com/feast-dev/feast/commit/d91d7e0da69d15c7aa14e736b608ed9f5ece3504))
* Integration tests for async sdk method ([#4201](https://github.com/feast-dev/feast/issues/4201)) ([08c44ae](https://github.com/feast-dev/feast/commit/08c44ae35a4a91228f9f78c7323b4b7a73ef33aa))
* Make sure schema is used when calling `get_table_query_string` method for Snowflake datasource ([#4131](https://github.com/feast-dev/feast/issues/4131)) ([c1579c7](https://github.com/feast-dev/feast/commit/c1579c77324cebb0514422235956812403316c80))
* Make sure schema is used when generating `from_expression` for Snowflake ([#4177](https://github.com/feast-dev/feast/issues/4177)) ([5051da7](https://github.com/feast-dev/feast/commit/5051da75de81deed19b25fbc2826d504a8ebdc8b))
* Pass native input values to `get_online_features` from feature server ([#4117](https://github.com/feast-dev/feast/issues/4117)) ([60756cb](https://github.com/feast-dev/feast/commit/60756cb4637a7961b6caffef3242e2886e77f78a))
* Pass region to S3 client only if set (Java) ([#4151](https://github.com/feast-dev/feast/issues/4151)) ([b8087f7](https://github.com/feast-dev/feast/commit/b8087f7a181977e0e4d3bd29c857d8e137af1de2))
* Pgvector patch ([#4108](https://github.com/feast-dev/feast/issues/4108)) ([ad45bb4](https://github.com/feast-dev/feast/commit/ad45bb4ac2dd83b530adda6196f85d46decaf98e))
* Update doc ([#4153](https://github.com/feast-dev/feast/issues/4153)) ([e873636](https://github.com/feast-dev/feast/commit/e873636b4a5f3a05666f9284c31e488f27257ed0))
* Update master-only benchmark bucket name due to credential update ([#4183](https://github.com/feast-dev/feast/issues/4183)) ([e88f1e3](https://github.com/feast-dev/feast/commit/e88f1e39778300fb443f1db230fe9589b74d9ed6))
* Updating the instructions for quickstart guide. ([#4120](https://github.com/feast-dev/feast/issues/4120)) ([0c30e96](https://github.com/feast-dev/feast/commit/0c30e96da144babe725a3f168c05d2fbeca65507))
* Upgrading the test container so that local tests works with updated d… ([#4155](https://github.com/feast-dev/feast/issues/4155)) ([93ddb11](https://github.com/feast-dev/feast/commit/93ddb11bf5a182cea44435147e39f40b30a69db7))


### Features

* Add a Kubernetes Operator for the Feast Feature Server ([#4145](https://github.com/feast-dev/feast/issues/4145)) ([4a696dc](https://github.com/feast-dev/feast/commit/4a696dc4b0fd96d51872a5e629ab5f3ca785d708))
* Add delta format to `FileSource`, add support for it in ibis/duckdb ([#4123](https://github.com/feast-dev/feast/issues/4123)) ([2b6f1d0](https://github.com/feast-dev/feast/commit/2b6f1d0945e8dbf13d01e045f87c5e58546b4af6))
* Add materialization support to ibis/duckdb ([#4173](https://github.com/feast-dev/feast/issues/4173)) ([369ca98](https://github.com/feast-dev/feast/commit/369ca98d88a5cb3c67b2363232b7c2eddfc4f333))
* Add optional private key params to Snowflake config ([#4205](https://github.com/feast-dev/feast/issues/4205)) ([20f5419](https://github.com/feast-dev/feast/commit/20f5419d30c32b533e91043a9690007a84000512))
* Add s3 remote storage export for duckdb ([#4195](https://github.com/feast-dev/feast/issues/4195)) ([6a04c48](https://github.com/feast-dev/feast/commit/6a04c48b4b84fb9905df638e5c4041c12532b053))
* Adding DatastoreOnlineStore 'database' argument. ([#4180](https://github.com/feast-dev/feast/issues/4180)) ([e739745](https://github.com/feast-dev/feast/commit/e739745482fed1b9c2d7b788ebb088041118c642))
* Adding get_online_features_async to feature store sdk ([#4172](https://github.com/feast-dev/feast/issues/4172)) ([311efc5](https://github.com/feast-dev/feast/commit/311efc5005b24d1fc9bc389ee7579e102e2cd4ea))
* Adding support for dictionary writes to online store  ([#4156](https://github.com/feast-dev/feast/issues/4156)) ([abfac01](https://github.com/feast-dev/feast/commit/abfac011ad1f94caef001539591d03b1552f65e5))
* Elasticsearch vector database ([#4188](https://github.com/feast-dev/feast/issues/4188)) ([bf99640](https://github.com/feast-dev/feast/commit/bf99640c0bcfd9ee7c1e66d24cb791bfa0e5ac4a))
* Enable other distance metrics for Vector DB and Update docs ([#4170](https://github.com/feast-dev/feast/issues/4170)) ([ba9f4ef](https://github.com/feast-dev/feast/commit/ba9f4efd5eccd0548a39521a145c6573ac90c221))
* Feast/IKV datetime edgecase errors ([#4211](https://github.com/feast-dev/feast/issues/4211)) ([bdae562](https://github.com/feast-dev/feast/commit/bdae562ea4582d8e47763736b639c70e56d79b2d))
* Feast/IKV documenation language changes ([#4149](https://github.com/feast-dev/feast/issues/4149)) ([690a621](https://github.com/feast-dev/feast/commit/690a6212e9f2b14fc4bf65513e5d30e70e229d0a))
* Feast/IKV online store contrib plugin integration ([#4068](https://github.com/feast-dev/feast/issues/4068)) ([f2b4eb9](https://github.com/feast-dev/feast/commit/f2b4eb94add8f86afa4e168236e8fcd11968510e))
* Feast/IKV online store documentation ([#4146](https://github.com/feast-dev/feast/issues/4146)) ([73601e4](https://github.com/feast-dev/feast/commit/73601e45e2fc57dc889644b1d28115b3c94bd8ea))
* Feast/IKV upgrade client version ([#4200](https://github.com/feast-dev/feast/issues/4200)) ([0e42150](https://github.com/feast-dev/feast/commit/0e4215060f97b7629015ab65ac526dfef0a1f7d4))
* Incorporate substrait ODFVs into ibis-based offline store queries ([#4102](https://github.com/feast-dev/feast/issues/4102)) ([c3a102f](https://github.com/feast-dev/feast/commit/c3a102f1b1941c8681ec876b54d7d16a32862925))
* Isolate input-dependent calculations in `get_online_features` ([#4041](https://github.com/feast-dev/feast/issues/4041)) ([2a6edea](https://github.com/feast-dev/feast/commit/2a6edeae42a2ebba7d9fc69af917bdc41ae6ecb0))
* Make arrow primary interchange for online ODFV execution ([#4143](https://github.com/feast-dev/feast/issues/4143)) ([3fdb716](https://github.com/feast-dev/feast/commit/3fdb71631fbb1b9cfb8d1cad69dbc2d2d50cea0d))
* Move data source validation entrypoint to offline store ([#4197](https://github.com/feast-dev/feast/issues/4197)) ([a17725d](https://github.com/feast-dev/feast/commit/a17725daec9e7355591e7ff2bc57202d5fa3f0c1))
* Upgrading python version to 3.11, adding support for 3.11 as well. ([#4159](https://github.com/feast-dev/feast/issues/4159)) ([4b1634f](https://github.com/feast-dev/feast/commit/4b1634f4da7ba47a29dfd4a0d573dfe515a8863d)), closes [#4152](https://github.com/feast-dev/feast/issues/4152) [#4114](https://github.com/feast-dev/feast/issues/4114)


### Reverts

* Reverts "fix: Using version args to install the correct feast version" ([#4112](https://github.com/feast-dev/feast/issues/4112)) ([b66baa4](https://github.com/feast-dev/feast/commit/b66baa46f48c72f4704bfe3980a8df49e1a06507)), closes [#3953](https://github.com/feast-dev/feast/issues/3953)

## [0.37.1](https://github.com/feast-dev/feast/compare/v0.37.0...v0.37.1) (2024-04-17)


### Bug Fixes

* Pgvector patch ([#4108](https://github.com/feast-dev/feast/issues/4108)) ([1a1f0b1](https://github.com/feast-dev/feast/commit/1a1f0b1c56aa2ac00b1e1aa1e21cc200ea659334))


### Reverts

* Reverts "fix: Using version args to install the correct feast version" ([#4112](https://github.com/feast-dev/feast/issues/4112)) ([d5ded69](https://github.com/feast-dev/feast/commit/d5ded69dea9af3a363feaa948cd3d2dcf10fb80c)), closes [#3953](https://github.com/feast-dev/feast/issues/3953)

# [0.37.0](https://github.com/feast-dev/feast/compare/v0.36.0...v0.37.0) (2024-04-17)


### Bug Fixes

* Pgvector patch ([#4103](https://github.com/feast-dev/feast/issues/4103)) ([5c4a9c5](https://github.com/feast-dev/feast/commit/5c4a9c57fa42ee5688fb6b428cedb416a7dbf185))
* Remove top-level grpc import in cli ([#4107](https://github.com/feast-dev/feast/issues/4107)) ([4362b6c](https://github.com/feast-dev/feast/commit/4362b6cc857ceafe60d58a14f2dfe006a83effb8))


### Features

* Add tags to dynamodb config ([#4100](https://github.com/feast-dev/feast/issues/4100)) ([b08b8d5](https://github.com/feast-dev/feast/commit/b08b8d5ce226cceae5e874a287db300f6fb9d41b))

# [0.36.0](https://github.com/feast-dev/feast/compare/v0.35.0...v0.36.0) (2024-04-16)


### Bug Fixes

* Add __eq__, __hash__ to SparkSource for correct comparison ([#4028](https://github.com/feast-dev/feast/issues/4028)) ([e703b40](https://github.com/feast-dev/feast/commit/e703b40582e676d4ec92551e79a444a9c0949f66))
* Add conn.commit() to Postgresonline_write_batch.online_write_batch ([#3904](https://github.com/feast-dev/feast/issues/3904)) ([7d75fc5](https://github.com/feast-dev/feast/commit/7d75fc525a7f2f46811d168ce71f91b5736ad788))
* Add missing __init__.py to embedded_go ([#4051](https://github.com/feast-dev/feast/issues/4051)) ([6bb4c73](https://github.com/feast-dev/feast/commit/6bb4c73b49934706002f9346c2260ab4261e4638))
* Add missing init files in infra utils ([#4067](https://github.com/feast-dev/feast/issues/4067)) ([54910a1](https://github.com/feast-dev/feast/commit/54910a16253c3f901d3bd5399bc2ba9703a7254d))
* Added registryPath parameter documentation in WebUI reference ([#3983](https://github.com/feast-dev/feast/issues/3983)) ([5e0af8f](https://github.com/feast-dev/feast/commit/5e0af8f52832daec34edd19cbad5e20ac3fd74d0)), closes [#3974](https://github.com/feast-dev/feast/issues/3974) [#3974](https://github.com/feast-dev/feast/issues/3974)
* Adding missing init files in materialization modules ([#4052](https://github.com/feast-dev/feast/issues/4052)) ([df05253](https://github.com/feast-dev/feast/commit/df0525355c32bbc40f890213edfa36512dd5bf55))
* Allow trancated timestamps when converting ([#3861](https://github.com/feast-dev/feast/issues/3861)) ([bdd7dfb](https://github.com/feast-dev/feast/commit/bdd7dfb6128dfc1f314a61a266da91c611ce7892))
* Azure blob storage support in Java feature server ([#2319](https://github.com/feast-dev/feast/issues/2319)) ([#4014](https://github.com/feast-dev/feast/issues/4014)) ([b9aabbd](https://github.com/feast-dev/feast/commit/b9aabbd35e27b26fb3af414da604062d6c8d17d0))
* Bugfix for grabbing historical data from Snowflake with array type features. ([#3964](https://github.com/feast-dev/feast/issues/3964)) ([1cc94f2](https://github.com/feast-dev/feast/commit/1cc94f2d23f88e0d9412b2fab8761abc81f5d35c))
* Bytewax materialization engine fails when loading feature_store.yaml ([#3912](https://github.com/feast-dev/feast/issues/3912)) ([987f0fd](https://github.com/feast-dev/feast/commit/987f0fdc99df1ef4507baff75e3df0e02bf42034))
* CI unittest warnings ([#4006](https://github.com/feast-dev/feast/issues/4006)) ([0441b8b](https://github.com/feast-dev/feast/commit/0441b8b9a7eae2eb478d12a8de911c1bd39ced37))
* Correct the returning class proto type of StreamFeatureView to StreamFeatureViewProto instead of FeatureViewProto. ([#3843](https://github.com/feast-dev/feast/issues/3843)) ([86d6221](https://github.com/feast-dev/feast/commit/86d62215f2338ea9d48c6e723e907c82cbe5500b))
* Create index only if not exists during MySQL online store update ([#3905](https://github.com/feast-dev/feast/issues/3905)) ([2f99a61](https://github.com/feast-dev/feast/commit/2f99a617b6a5d8eae1e27c780bbfa94594f54441))
* Disable minio tests in workflows on master and nightly ([#4072](https://github.com/feast-dev/feast/issues/4072)) ([c06dda8](https://github.com/feast-dev/feast/commit/c06dda84a26c5df3e761a18adaa81f87b1bcc0de))
* Disable the Feast Usage feature by default. ([#4090](https://github.com/feast-dev/feast/issues/4090)) ([b5a7013](https://github.com/feast-dev/feast/commit/b5a701359543e9e0f4088db54beb939e57131faa))
* Dump repo_config by alias ([#4063](https://github.com/feast-dev/feast/issues/4063)) ([e4bef67](https://github.com/feast-dev/feast/commit/e4bef6769265a9b5d87486e34ac00f022ca9ce28))
* Extend SQL registry config with a sqlalchemy_config_kwargs key ([#3997](https://github.com/feast-dev/feast/issues/3997)) ([21931d5](https://github.com/feast-dev/feast/commit/21931d59f8a2f8b69383de0dd371a780149ccda8))
* Feature Server image startup in OpenShift clusters ([#4096](https://github.com/feast-dev/feast/issues/4096)) ([9efb243](https://github.com/feast-dev/feast/commit/9efb243c548b075ca8288e04b09b84a9fa49dc7c))
* Fix copy method for StreamFeatureView ([#3951](https://github.com/feast-dev/feast/issues/3951)) ([cf06704](https://github.com/feast-dev/feast/commit/cf06704bd58c77931679f1c0c7e44de7042f931f))
* Fix for materializing entityless feature views in Snowflake ([#3961](https://github.com/feast-dev/feast/issues/3961)) ([1e64c77](https://github.com/feast-dev/feast/commit/1e64c77e1e146f952f450db9370e2da5c85a8500))
* Fix type mapping spark ([#4071](https://github.com/feast-dev/feast/issues/4071)) ([3afa78e](https://github.com/feast-dev/feast/commit/3afa78e454b5478b041f1182edcebace916ef67b))
* Fix typo as the cli does not support shortcut-f option. ([#3954](https://github.com/feast-dev/feast/issues/3954)) ([dd79dbb](https://github.com/feast-dev/feast/commit/dd79dbbac90caaf0617a5046c84a2618e532980b))
* Get container host addresses from testcontainers ([#3946](https://github.com/feast-dev/feast/issues/3946)) ([2cf1a0f](https://github.com/feast-dev/feast/commit/2cf1a0fa9efbceca2e79c5e375796696e248e3d9))
* Handle ComplexFeastType to None comparison ([#3876](https://github.com/feast-dev/feast/issues/3876)) ([fa8492d](https://github.com/feast-dev/feast/commit/fa8492dfe7f38ab493a8d35a412ec9334a0ff6b9))
* Hashlib md5 errors in FIPS for python 3.9+ ([#4019](https://github.com/feast-dev/feast/issues/4019)) ([6d9156b](https://github.com/feast-dev/feast/commit/6d9156b3d6372d654048ea2bfb7eec3f3908d038))
* Making the query_timeout variable as optional int because upstream is considered to be optional ([#4092](https://github.com/feast-dev/feast/issues/4092)) ([fd5b620](https://github.com/feast-dev/feast/commit/fd5b620b2c56c56286a5899b271da426c1a4ef67))
* Move gRPC dependencies to an extra ([#3900](https://github.com/feast-dev/feast/issues/3900)) ([f93c5fd](https://github.com/feast-dev/feast/commit/f93c5fd4b8bd0031942c4f6ba4e84ebc54be8522))
* Prevent spamming pull busybox from dockerhub ([#3923](https://github.com/feast-dev/feast/issues/3923)) ([7153cad](https://github.com/feast-dev/feast/commit/7153cad6082edfded96999c49ee1bdc9329e11c3))
* Quickstart notebook example ([#3976](https://github.com/feast-dev/feast/issues/3976)) ([b023aa5](https://github.com/feast-dev/feast/commit/b023aa5817bffe235f460c5df879141bb5945edb))
* Raise error when not able read of file source spark source ([#4005](https://github.com/feast-dev/feast/issues/4005)) ([34cabfb](https://github.com/feast-dev/feast/commit/34cabfb29a2692180dc6b6dda8bba9062beca4d2))
* remove not use input parameter in spark source ([#3980](https://github.com/feast-dev/feast/issues/3980)) ([7c90882](https://github.com/feast-dev/feast/commit/7c908822f8d9f5e32ab17d96e6b5dd79e5b59b3e))
* Remove parentheses in pull_latest_from_table_or_query ([#4026](https://github.com/feast-dev/feast/issues/4026)) ([dc4671e](https://github.com/feast-dev/feast/commit/dc4671ed7e28b4157112a81ee0a70925d02db8e8))
* Remove proto-plus imports ([#4044](https://github.com/feast-dev/feast/issues/4044)) ([ad8f572](https://github.com/feast-dev/feast/commit/ad8f5721af6d8ad8b7539b91e0616ebf6e47f47b))
* Remove unnecessary dependency on mysqlclient ([#3925](https://github.com/feast-dev/feast/issues/3925)) ([f494f02](https://github.com/feast-dev/feast/commit/f494f02e1254b91b56b0b69f4a15edafe8d7291a))
* Restore label check for all actions using pull_request_target ([#3978](https://github.com/feast-dev/feast/issues/3978)) ([591ba4e](https://github.com/feast-dev/feast/commit/591ba4e39842b5fbb49db32be4fce28e6d520d93))
* Revert mypy config ([#3952](https://github.com/feast-dev/feast/issues/3952)) ([6b8e96c](https://github.com/feast-dev/feast/commit/6b8e96c982a50587a13216666085fc61494cdfc9))
* Rewrite Spark materialization engine to use mapInPandas ([#3936](https://github.com/feast-dev/feast/issues/3936)) ([dbb59ba](https://github.com/feast-dev/feast/commit/dbb59ba0932e5962b34b14e7218a1ddae86a9686))
* Run feature server w/o gunicorn on windows ([#4024](https://github.com/feast-dev/feast/issues/4024)) ([584e9b1](https://github.com/feast-dev/feast/commit/584e9b1be9452158d9104133a24ff29d3976f9ed))
* SqlRegistry _apply_object update statement ([#4042](https://github.com/feast-dev/feast/issues/4042)) ([ef62def](https://github.com/feast-dev/feast/commit/ef62defbd80172ba3c536c413388234707278be1))
* Substrait ODFVs for online ([#4064](https://github.com/feast-dev/feast/issues/4064)) ([26391b0](https://github.com/feast-dev/feast/commit/26391b07605794bcb0eb6cdec6d59bd94720bba6))
* Swap security label check on the PR title validation job to explicit permissions instead ([#3987](https://github.com/feast-dev/feast/issues/3987)) ([f604af9](https://github.com/feast-dev/feast/commit/f604af9ebf56ebd88b4e6ef541fdc20de2cc5b8c))
* Transformation server doesn't generate files from proto ([#3902](https://github.com/feast-dev/feast/issues/3902)) ([d3a2a45](https://github.com/feast-dev/feast/commit/d3a2a45d9bc2b690a7aa784ec7b0411e91244dab))
* Trino as an OfflineStore Access Denied when BasicAuthenticaion ([#3898](https://github.com/feast-dev/feast/issues/3898)) ([49d2988](https://github.com/feast-dev/feast/commit/49d2988a562c66b3949cf2368fe44ed41e767eab))
* Trying to import pyspark lazily to avoid the dependency on the library ([#4091](https://github.com/feast-dev/feast/issues/4091)) ([a05cdbc](https://github.com/feast-dev/feast/commit/a05cdbcd38d80ce1abfff7d93bef9df589dbd61c))
* Typo Correction in Feast UI Readme ([#3939](https://github.com/feast-dev/feast/issues/3939)) ([c16e5af](https://github.com/feast-dev/feast/commit/c16e5afcc5273b0c26b79dd4e233a28618ac490a))
* Update actions/setup-python from v3 to v4 ([#4003](https://github.com/feast-dev/feast/issues/4003)) ([ee4c4f1](https://github.com/feast-dev/feast/commit/ee4c4f1ca486facc14e13ad0dbe7c9cc7c82d832))
* Update typeguard version to >=4.0.0 ([#3837](https://github.com/feast-dev/feast/issues/3837)) ([dd96150](https://github.com/feast-dev/feast/commit/dd96150e2a5829401f793a51da4b3594677e570d))
* Upgrade sqlalchemy from 1.x to 2.x regarding PVE-2022-51668. ([#4065](https://github.com/feast-dev/feast/issues/4065)) ([ec4c15c](https://github.com/feast-dev/feast/commit/ec4c15c0104fa8f4cebdbf29f9e067baab07b09b))
* Use CopyFrom() instead of __deepycopy__() for creating a copy of protobuf object. ([#3999](https://github.com/feast-dev/feast/issues/3999)) ([5561b30](https://github.com/feast-dev/feast/commit/5561b306d8c7b43851f5f411e1c4f4f34d99933f))
* Using version args to install the correct feast version ([#3953](https://github.com/feast-dev/feast/issues/3953)) ([b83a702](https://github.com/feast-dev/feast/commit/b83a70227c6afe7258328ff5847a26b526d0b5df))
* Verify the existence of Registry tables in snowflake before calling CREATE sql command. Allow read-only user to call feast apply. ([#3851](https://github.com/feast-dev/feast/issues/3851)) ([9a3590e](https://github.com/feast-dev/feast/commit/9a3590ea771ca3c3224f5e1a833453144e54284e))


### Features

* Add duckdb offline store ([#3981](https://github.com/feast-dev/feast/issues/3981)) ([161547b](https://github.com/feast-dev/feast/commit/161547b167c7a9b2d53517d498acbe50d9298a40))
* Add Entity df in format of a Spark Dataframe instead of just pd.DataFrame or string for SparkOfflineStore ([#3988](https://github.com/feast-dev/feast/issues/3988)) ([43b2c28](https://github.com/feast-dev/feast/commit/43b2c287705c2a3e882517524229f155c9ce0a01))
* Add gRPC Registry Server ([#3924](https://github.com/feast-dev/feast/issues/3924)) ([373e624](https://github.com/feast-dev/feast/commit/373e624abb8779b8a60d30aa08d25414d987bb1b))
* Add local tests for s3 registry using minio ([#4029](https://github.com/feast-dev/feast/issues/4029)) ([d82d1ec](https://github.com/feast-dev/feast/commit/d82d1ecb534ab35b901c36e920666196eae0ac79))
* Add python bytes to array type conversion support proto ([#3874](https://github.com/feast-dev/feast/issues/3874)) ([8688acd](https://github.com/feast-dev/feast/commit/8688acd1731aa04b041090c7b1c049bfba1717ed))
* Add python client for remote registry server ([#3941](https://github.com/feast-dev/feast/issues/3941)) ([42a7b81](https://github.com/feast-dev/feast/commit/42a7b8170d6dc994055c67989046d11c238af40f))
* Add Substrait-based ODFV transformation ([#3969](https://github.com/feast-dev/feast/issues/3969)) ([9e58bd4](https://github.com/feast-dev/feast/commit/9e58bd463f7ca2b4982708cb1e1250f587ecfb68))
* Add support for arrays in snowflake ([#3769](https://github.com/feast-dev/feast/issues/3769)) ([8d6bec8](https://github.com/feast-dev/feast/commit/8d6bec8fc47986c84f366ce3edfe7d03fa6b2e9f))
* Added delete_table to redis online store ([#3857](https://github.com/feast-dev/feast/issues/3857)) ([03dae13](https://github.com/feast-dev/feast/commit/03dae13aa60c072b171c7f21d4e795eaaad18e55))
* Adding support for Native Python feature transformations for ODFVs ([#4045](https://github.com/feast-dev/feast/issues/4045)) ([73bc853](https://github.com/feast-dev/feast/commit/73bc85351a9202d3db93907e8206d68123ee5baa))
* Bumping requirements ([#4079](https://github.com/feast-dev/feast/issues/4079)) ([1943056](https://github.com/feast-dev/feast/commit/194305631bbb6cca251dbb46df5b5575ffb2391b))
* Decouple transformation types from ODFVs ([#3949](https://github.com/feast-dev/feast/issues/3949)) ([0a9fae8](https://github.com/feast-dev/feast/commit/0a9fae8fd42e7348365ef902038f3f71f977ef3e))
* Dropping Python 3.8 from local integration tests and integration tests ([#3994](https://github.com/feast-dev/feast/issues/3994)) ([817995c](https://github.com/feast-dev/feast/commit/817995c12588cc35c53d1ad487efaaf53da287be))
* Dropping python 3.8 requirements files from the project. ([#4021](https://github.com/feast-dev/feast/issues/4021)) ([f09c612](https://github.com/feast-dev/feast/commit/f09c612d046dfa56e9c616ff68c05823ce0f3bb6))
* Dropping the support for python 3.8 version from feast ([#4010](https://github.com/feast-dev/feast/issues/4010)) ([a0f7472](https://github.com/feast-dev/feast/commit/a0f7472f200300f3a45aa404922dd67bb4ad237f))
* Dropping unit tests for Python 3.8 ([#3989](https://github.com/feast-dev/feast/issues/3989)) ([60f24f9](https://github.com/feast-dev/feast/commit/60f24f9ed16a216acb0f3642892dea73690ca29f))
* Enable Arrow-based columnar data transfers  ([#3996](https://github.com/feast-dev/feast/issues/3996)) ([d8d7567](https://github.com/feast-dev/feast/commit/d8d75676cbaf565b6a6a097f33c49f56b852dcd7))
* Enable Vector database and retrieve_online_documents API ([#4061](https://github.com/feast-dev/feast/issues/4061)) ([ec19036](https://github.com/feast-dev/feast/commit/ec19036fcc4c77084a2dd5aae5576f8f43393eba))
* Kubernetes materialization engine written based on bytewax ([#4087](https://github.com/feast-dev/feast/issues/4087)) ([7617bdb](https://github.com/feast-dev/feast/commit/7617bdb7f4222edb69893c37621bd87b940b3227))
* Lint with ruff ([#4043](https://github.com/feast-dev/feast/issues/4043)) ([7f1557b](https://github.com/feast-dev/feast/commit/7f1557b348b7935e3586c90c8dec15fdf6cd8665))
* Make arrow primary interchange for offline ODFV execution ([#4083](https://github.com/feast-dev/feast/issues/4083)) ([9ed0a09](https://github.com/feast-dev/feast/commit/9ed0a09746aca0eb73c6e214f082e0e3887ff836))
* Pandas v2 compatibility ([#3957](https://github.com/feast-dev/feast/issues/3957)) ([64459ad](https://github.com/feast-dev/feast/commit/64459ad1b5ed4a782b7ce87fcec3012e00408c74))
* Pull duckdb from contribs, add to CI ([#4059](https://github.com/feast-dev/feast/issues/4059)) ([318a2b8](https://github.com/feast-dev/feast/commit/318a2b8bfc94f10c81206071fcb1d41f19683288))
* Refactor ODFV schema inference ([#4076](https://github.com/feast-dev/feast/issues/4076)) ([c50a9ff](https://github.com/feast-dev/feast/commit/c50a9ff783fa400542422990ff835da930bcb6bf))
* Refactor registry caching logic into a separate class ([#3943](https://github.com/feast-dev/feast/issues/3943)) ([924f944](https://github.com/feast-dev/feast/commit/924f9441107b8e36a3d5c6f8b16ed24f9a03b867))
* Rename OnDemandTransformations to Transformations ([#4038](https://github.com/feast-dev/feast/issues/4038)) ([9b98eaf](https://github.com/feast-dev/feast/commit/9b98eafccbf39b41186bfb3ebd36af20d57bd509))
* Revert updating dependencies so that feast can be run on 3.11. ([#3968](https://github.com/feast-dev/feast/issues/3968)) ([d3c68fb](https://github.com/feast-dev/feast/commit/d3c68fb8646b29032cb67b8c8e6a8c0aa7a821c7)), closes [#3958](https://github.com/feast-dev/feast/issues/3958)
* Rewrite ibis point-in-time-join w/o feast abstractions ([#4023](https://github.com/feast-dev/feast/issues/4023)) ([3980e0c](https://github.com/feast-dev/feast/commit/3980e0c9a762a6ec3bcee5a0e9cdf532994bb1c9))
* Support s3gov schema by snowflake offline store during materialization ([#3891](https://github.com/feast-dev/feast/issues/3891)) ([ea8ad17](https://github.com/feast-dev/feast/commit/ea8ad1731a5ebe798b11181fc0027f7cac0e1526))
* Update odfv test ([#4054](https://github.com/feast-dev/feast/issues/4054)) ([afd52b8](https://github.com/feast-dev/feast/commit/afd52b8803d7660a90f382d2c1ad7705608c861b))
* Update pyproject.toml to use Python 3.9 as default ([#4011](https://github.com/feast-dev/feast/issues/4011)) ([277b891](https://github.com/feast-dev/feast/commit/277b891ffa1193914b123672010e588573dcaa98))
* Update the Pydantic from v1 to v2 ([#3948](https://github.com/feast-dev/feast/issues/3948)) ([ec11a7c](https://github.com/feast-dev/feast/commit/ec11a7cb8d56d8e2e5cda07e06b4c98dcc9d2ba3))
* Updating dependencies so that feast can be run on 3.11. ([#3958](https://github.com/feast-dev/feast/issues/3958)) ([59639db](https://github.com/feast-dev/feast/commit/59639dbb0272aacd2201cb5f65b01445013db6e6))
* Updating protos to separate transformation ([#4018](https://github.com/feast-dev/feast/issues/4018)) ([c58ef74](https://github.com/feast-dev/feast/commit/c58ef74c18554d823f7957bf602184c744bb7ed7))


### Reverts

* Reverting bumping requirements ([#4081](https://github.com/feast-dev/feast/issues/4081)) ([1ba65b4](https://github.com/feast-dev/feast/commit/1ba65b4e13a2af3e9cea879d1c1e48891a0f0610)), closes [#4079](https://github.com/feast-dev/feast/issues/4079)
* Verify the existence of Registry tables in snowflake… ([#3907](https://github.com/feast-dev/feast/issues/3907)) ([c0d358a](https://github.com/feast-dev/feast/commit/c0d358a49d5f576bb9f1017d1ee0db2d6cd5f1a5)), closes [#3851](https://github.com/feast-dev/feast/issues/3851)

# [0.35.0](https://github.com/feast-dev/feast/compare/v0.34.0...v0.35.0) (2024-01-13)


### Bug Fixes

* Add async refresh to prevent synchronous refresh in main thread ([#3812](https://github.com/feast-dev/feast/issues/3812)) ([9583ed6](https://github.com/feast-dev/feast/commit/9583ed6b4ae8d3b97934bf0c80ecb236ed1e2895))
* Adopt connection pooling for HBase ([#3793](https://github.com/feast-dev/feast/issues/3793)) ([b3852bf](https://github.com/feast-dev/feast/commit/b3852bfb8b27bf07736935f465da3067fcbac0ae))
* Bytewax engine create configmap from object ([#3821](https://github.com/feast-dev/feast/issues/3821)) ([25e9775](https://github.com/feast-dev/feast/commit/25e97756adedfd1227d591ae74bdf60655f9067e))
* Fix warnings from deprecated paths and update default log level ([#3757](https://github.com/feast-dev/feast/issues/3757)) ([68a8737](https://github.com/feast-dev/feast/commit/68a87379c42567f338d86cb2be90520cc6d4bfb6))
* improve parsing bytewax job status ([5983f40](https://github.com/feast-dev/feast/commit/5983f40f8f5df5dbbcd2640f83ef82c19cdb4d19))
* make bytewax settings unexposed ([ae1bb8b](https://github.com/feast-dev/feast/commit/ae1bb8bdd1b9e293809519971935c93c2214d791))
* Make generated temp table name escaped ([#3797](https://github.com/feast-dev/feast/issues/3797)) ([175d796](https://github.com/feast-dev/feast/commit/175d7969b1f75ab797aff9c92a70d845297444ad))
* Pin numpy version to avoid spammy deprecation messages ([774ed33](https://github.com/feast-dev/feast/commit/774ed33a067bf9bf087520325b72f4f4d194106a))
* Redundant feature materialization and premature incremental materialization timestamp updates ([#3789](https://github.com/feast-dev/feast/issues/3789)) ([417b16b](https://github.com/feast-dev/feast/commit/417b16b57af7b38fbd0708b9a0c5d5035ed021fd)), closes [#6](https://github.com/feast-dev/feast/issues/6) [#7](https://github.com/feast-dev/feast/issues/7)
* Resolve hbase hotspot issue when materializing ([#3790](https://github.com/feast-dev/feast/issues/3790)) ([7376db8](https://github.com/feast-dev/feast/commit/7376db8dbd1d3168a1262fbbc0ce3899be8d0c34))
* Set keepalives_idle None by default ([#3756](https://github.com/feast-dev/feast/issues/3756)) ([8717e9b](https://github.com/feast-dev/feast/commit/8717e9bf0fd253454982b9c9e9527c4d41906e9c))
* Set upper bound for bigquery client due to its breaking changes ([2151c39](https://github.com/feast-dev/feast/commit/2151c39d1a8d8eba114306411dd4bd91ac0ce3f6))
* UI project cannot handle fallback routes ([#3766](https://github.com/feast-dev/feast/issues/3766)) ([96ece0f](https://github.com/feast-dev/feast/commit/96ece0fe94a07cc6f1dabf5d6c9b061b48b06d67))
* update dependencies versions due to conflicts ([5dc0b24](https://github.com/feast-dev/feast/commit/5dc0b241ec68aa10fd783569bf0ae12c5752f20f))
* Update jackson and remove unnecessary logging ([#3809](https://github.com/feast-dev/feast/issues/3809)) ([018d0ea](https://github.com/feast-dev/feast/commit/018d0eab69dde63266f2c56813045ea5c5523f76))
* upgrade the pyarrow to latest v14.0.1 for CVE-2023-47248. ([052182b](https://github.com/feast-dev/feast/commit/052182bcca046e35456674fc7d524825882f4b35))


### Features

* Add get online feature rpc to gprc server ([#3815](https://github.com/feast-dev/feast/issues/3815)) ([01db8cc](https://github.com/feast-dev/feast/commit/01db8cce6f82d4c6e496041351fb6b56eb2645b0))
* Add materialize and materialize-incremental rest endpoints ([#3761](https://github.com/feast-dev/feast/issues/3761)) ([fa600fe](https://github.com/feast-dev/feast/commit/fa600fe3c4b1d5fdd383a9367511ac5616ee7a32)), closes [#3760](https://github.com/feast-dev/feast/issues/3760)
* add redis sentinel support ([3387a15](https://github.com/feast-dev/feast/commit/3387a15d2b7e8dea430a271570be5a19b32bd3fe))
* add redis sentinel support ([4337c89](https://github.com/feast-dev/feast/commit/4337c89083a3cfca21ee1beef473fda13b0e9014))
* add redis sentinel support format lint ([aad8718](https://github.com/feast-dev/feast/commit/aad8718d24d893b3ff8c5864c5b8d210cfcdb22f))
* Add support for `table_create_disposition` in bigquery job for offline store ([#3762](https://github.com/feast-dev/feast/issues/3762)) ([6a728fe](https://github.com/feast-dev/feast/commit/6a728fe66db0286ea10301d1fe693d6dcba4e4f4))
* Add support for in_cluster config and additional labels for bytewax materialization ([#3754](https://github.com/feast-dev/feast/issues/3754)) ([2192e65](https://github.com/feast-dev/feast/commit/2192e6527fa10f1580e4dd8f350e05e45af981b7))
* Apply cache to load proto registry for performance ([#3702](https://github.com/feast-dev/feast/issues/3702)) ([709c709](https://github.com/feast-dev/feast/commit/709c7098dc28a35dd488f5079d3787cf1f74ec03))
* Make bytewax job write as mini-batches ([#3777](https://github.com/feast-dev/feast/issues/3777)) ([9b0e5ce](https://github.com/feast-dev/feast/commit/9b0e5ce2d1b617fcdcf0699c8b0cf8549a5e5ac5))
* Optimize bytewax pod resource with zero-copy ([9cf9d96](https://github.com/feast-dev/feast/commit/9cf9d965a5566a87bb7419f2e8509666076f035f))
* Support GCS filesystem for bytewax engine ([#3774](https://github.com/feast-dev/feast/issues/3774)) ([fb6b807](https://github.com/feast-dev/feast/commit/fb6b807f8b32776d388757ca431d290c03170c66))

# [0.34.0](https://github.com/feast-dev/feast/compare/v0.33.0...v0.34.0) (2023-09-07)


### Bug Fixes

* Add NUMERIC to bq_to_feast type map ([#3719](https://github.com/feast-dev/feast/issues/3719)) ([6474b4b](https://github.com/feast-dev/feast/commit/6474b4b0169dc9b3df8e8daecded2b1fad5ead58))
* Fix python unit tests ([#3734](https://github.com/feast-dev/feast/issues/3734)) ([e81684d](https://github.com/feast-dev/feast/commit/e81684d4f7916c986fa8e6cf06c2918951469799))
* Handle unknown postgres source types gracefully ([#3634](https://github.com/feast-dev/feast/issues/3634)) ([d7041f4](https://github.com/feast-dev/feast/commit/d7041f4cce813d349e9016da55d65a65c1ec2355))
* Pin protobuf version to avoid seg fault on some machines ([028cc20](https://github.com/feast-dev/feast/commit/028cc20a28118bd31deca8965782d5ad25f74300))
* Remove unwanted excessive splitting of gcs path, so expected gcs parquet paths are returned from BigQueryRetrievalJob.to_remote_storage() ([#3730](https://github.com/feast-dev/feast/issues/3730)) ([f2c5988](https://github.com/feast-dev/feast/commit/f2c59885e31f3f238dbd9c13cd1ba168e3233a9d))
* Run store.plan() only when need it. ([#3708](https://github.com/feast-dev/feast/issues/3708)) ([7bc7c47](https://github.com/feast-dev/feast/commit/7bc7c47b4507310850474290131c03fb6d480834))
* Saved datasets no longer break CLI registry-dump command ([#3717](https://github.com/feast-dev/feast/issues/3717)) ([f28ccc2](https://github.com/feast-dev/feast/commit/f28ccc2b8f42bcca943d498ad583337d4cd70383))
* Update py3.8 ci requirements for cython 3.0 release ([#3735](https://github.com/feast-dev/feast/issues/3735)) ([1695c13](https://github.com/feast-dev/feast/commit/1695c13fa8f48fdc2b5e627837043c5eea0914a9))


### Features

* Enhance customization of Trino connections when using Trino-based Offline Stores ([#3699](https://github.com/feast-dev/feast/issues/3699)) ([ed7535e](https://github.com/feast-dev/feast/commit/ed7535e23d490249ca7d224fb88e53b98d496ec0))
* Implement gRPC server to ingest streaming features ([#3687](https://github.com/feast-dev/feast/issues/3687)) ([a3fcd1f](https://github.com/feast-dev/feast/commit/a3fcd1f369bdf07174b5ecf2a49ca9864cf145d4))

# [0.33.0](https://github.com/feast-dev/feast/compare/v0.32.0...v0.33.0) (2023-08-14)


### Bug Fixes

* Add aws-sts dependency in java sdk so that S3 client acquires IRSA role ([#3696](https://github.com/feast-dev/feast/issues/3696)) ([c75a01f](https://github.com/feast-dev/feast/commit/c75a01fce2d52cd18479ace748b8eb2e6c81c988))
* Redshift push ignores schema ([#3671](https://github.com/feast-dev/feast/issues/3671)) ([76270f6](https://github.com/feast-dev/feast/commit/76270f66b3d98b0119b70927c06908f9834b6120))


### Features

* Add possibility to save dataset as table, when spark config has remote warehouse info ([#3645](https://github.com/feast-dev/feast/issues/3645)) ([22c109b](https://github.com/feast-dev/feast/commit/22c109bc088d093a7c81c59e11490a9a21f82309))

# [0.32.0](https://github.com/feast-dev/feast/compare/v0.31.0...v0.32.0) (2023-07-17)


### Bug Fixes

* Added generic Feature store Creation for CLI ([#3618](https://github.com/feast-dev/feast/issues/3618)) ([bf740d2](https://github.com/feast-dev/feast/commit/bf740d2d0ef3b62cf27f25083b5b4d49955b56fb))
* Broken non-root path with projects-list.json ([#3665](https://github.com/feast-dev/feast/issues/3665)) ([4861af0](https://github.com/feast-dev/feast/commit/4861af0f3206f965ad2f7a4eddf2ebec2df149f8))
* Clean up snowflake to_spark_df() ([#3607](https://github.com/feast-dev/feast/issues/3607)) ([e8e643e](https://github.com/feast-dev/feast/commit/e8e643e3555127daf37fa4961fdadb0508a041db))
* Entityless fv breaks with `KeyError: __dummy` applying feature_store.plan() on python ([#3640](https://github.com/feast-dev/feast/issues/3640)) ([ef4ef32](https://github.com/feast-dev/feast/commit/ef4ef32f7466b5d874d537abe8def4731b15dc85))
* Fix scan datasize to 0 for inference schema ([#3628](https://github.com/feast-dev/feast/issues/3628)) ([c3dd74e](https://github.com/feast-dev/feast/commit/c3dd74e238b5b9e784e3dbe102941c66a63f6686))
* Fix timestamp consistency in push api ([#3614](https://github.com/feast-dev/feast/issues/3614)) ([9b227d7](https://github.com/feast-dev/feast/commit/9b227d7d44f30d28d1faadc8015f25dc4a6f56b5))
* For SQL registry, increase max data_source_name length to 255 ([#3630](https://github.com/feast-dev/feast/issues/3630)) ([478caec](https://github.com/feast-dev/feast/commit/478caecc8d61b6070ec03bc13688a83b8c5f5936))
* Implements connection pool for postgres online store ([#3633](https://github.com/feast-dev/feast/issues/3633)) ([059509a](https://github.com/feast-dev/feast/commit/059509a492d180effb1786713738665e293838e7))
* Manage redis pipe's context ([#3655](https://github.com/feast-dev/feast/issues/3655)) ([48e0971](https://github.com/feast-dev/feast/commit/48e097130e68241e751bd4be5af7427fffad47cf))
* Missing Catalog argument in athena connector ([#3661](https://github.com/feast-dev/feast/issues/3661)) ([f6d3caf](https://github.com/feast-dev/feast/commit/f6d3caf8affc74aef7ac489d3e6816d45b30d820))
* Optimize bytes processed when retrieving entity df schema to 0 ([#3680](https://github.com/feast-dev/feast/issues/3680)) ([1c01035](https://github.com/feast-dev/feast/commit/1c010357affd48616d39f1ad01b872fac946269d))


### Features

* Add gunicorn for serve with multiprocess ([#3636](https://github.com/feast-dev/feast/issues/3636)) ([4de7faf](https://github.com/feast-dev/feast/commit/4de7faf7b262d30a9f6795911d8fa97df775fa8d))
* Use string as a substitute for unregistered types during schema inference ([#3646](https://github.com/feast-dev/feast/issues/3646)) ([c474ccd](https://github.com/feast-dev/feast/commit/c474ccdd23ca8161de5e2958f0a12826c020dc44))

# [0.31.0](https://github.com/feast-dev/feast/compare/v0.30.0...v0.31.0) (2023-04-21)


### Bug Fixes

* Add Stream Feature Views to helper that collect Feature View names ([#3582](https://github.com/feast-dev/feast/issues/3582)) ([7854f63](https://github.com/feast-dev/feast/commit/7854f637160d4d1f4758b83e6c396fe49447e7b7))
* Add StreamFeatureViewSpec to FeastObjectSpecProto convenience type ([#3550](https://github.com/feast-dev/feast/issues/3550)) ([3cefd6c](https://github.com/feast-dev/feast/commit/3cefd6cf806997be4ea8427bcf4aa9852d6ce038))
* Batch Snowflake materialization queries to obey Snowpark 100 fea… ([#3406](https://github.com/feast-dev/feast/issues/3406)) ([f9862b5](https://github.com/feast-dev/feast/commit/f9862b565b6c9019ec146871d2fb45590eb31576))
* Bytewax materializer security context ([#3573](https://github.com/feast-dev/feast/issues/3573)) ([6794338](https://github.com/feast-dev/feast/commit/6794338d0c9405a5a9ba7ef2b47de98cd905474e))
* **cI:** Install coreutils in mac github workers for smoke test ([#3563](https://github.com/feast-dev/feast/issues/3563)) ([e7421c1](https://github.com/feast-dev/feast/commit/e7421c11172aaafff34da98fc14cf763c2d70002))
* Fix bug with no SqlRegistryConfig class ([#3586](https://github.com/feast-dev/feast/issues/3586)) ([6dc1368](https://github.com/feast-dev/feast/commit/6dc1368afb66a4231b7513939a7cbf204ab4d46f))
* Fix Snowflake template ([#3584](https://github.com/feast-dev/feast/issues/3584)) ([6c09c39](https://github.com/feast-dev/feast/commit/6c09c39b64e31dc6e84be566524d6126683f3013))
* Make snowflake to remote tables temporary ([#3588](https://github.com/feast-dev/feast/issues/3588)) ([ad48146](https://github.com/feast-dev/feast/commit/ad4814643abd28d5b2e119b8ef46ddfdce77424a))
* Remove snowflake source warehouse tech debt ([#3422](https://github.com/feast-dev/feast/issues/3422)) ([7da0580](https://github.com/feast-dev/feast/commit/7da058085cd1211fb383ff0a6c5ae8f59999c5f0))
* Snowflake remote storage ([#3574](https://github.com/feast-dev/feast/issues/3574)) ([f8d3890](https://github.com/feast-dev/feast/commit/f8d3890f9f049c4b9190456b071e0fdb29aae69e))
* Support param timeout when persisting ([#3593](https://github.com/feast-dev/feast/issues/3593)) ([01a98f0](https://github.com/feast-dev/feast/commit/01a98f08e9e6d0aebf41188f2644f49111ea4ca9))
* Use pyarrow in a way that works across versions ([#3562](https://github.com/feast-dev/feast/issues/3562)) ([1289f3f](https://github.com/feast-dev/feast/commit/1289f3f7eea6bd3b08617606862a75f0224f9f18))
* Wrap the bigquery table name with backtick. ([#3577](https://github.com/feast-dev/feast/issues/3577)) ([09f0e7e](https://github.com/feast-dev/feast/commit/09f0e7e1011fc451b3bfb94c4b7764007fc69836))


### Features

* Add AWS Redshift Serverless support ([#3595](https://github.com/feast-dev/feast/issues/3595)) ([58ce148](https://github.com/feast-dev/feast/commit/58ce148401fe578b1727bc42ee6b4b9a558660c7))
* Add Hazelcast as an online store ([#3523](https://github.com/feast-dev/feast/issues/3523)) ([b05d50b](https://github.com/feast-dev/feast/commit/b05d50bcfeb179c2596f96f0d0a714754c516361))
* Cache Bigtable client ([#3602](https://github.com/feast-dev/feast/issues/3602)) ([b27472f](https://github.com/feast-dev/feast/commit/b27472fc1fb42368ffe1556c848dc3b21b2fca0c))
* Relax aws extras requirements ([#3585](https://github.com/feast-dev/feast/issues/3585)) ([7e77382](https://github.com/feast-dev/feast/commit/7e77382c6b75f514e18b683fef1495fa1fa87308))
* Show bigquery datasource table and query on UI ([#3600](https://github.com/feast-dev/feast/issues/3600)) ([58d63f7](https://github.com/feast-dev/feast/commit/58d63f7e6b1dde3dcd8893e4448940ea34e671cf))
* Update snowflake offline store job output formats -- added arrow ([#3589](https://github.com/feast-dev/feast/issues/3589)) ([be3e349](https://github.com/feast-dev/feast/commit/be3e3491d83e337af42e06f75226919904cb5d86))

# [0.30.0](https://github.com/feast-dev/feast/compare/v0.29.0...v0.30.0) (2023-03-24)


### Bug Fixes

* Add description attribute to the Field.from_proto method ([#3469](https://github.com/feast-dev/feast/issues/3469)) ([473f8d9](https://github.com/feast-dev/feast/commit/473f8d93fa8d565e53fc59b3c444a1b8ed061c51))
* Add filesystem kwargs when read prev_table on FileRetrievalJob (… ([#3491](https://github.com/feast-dev/feast/issues/3491)) ([dca4745](https://github.com/feast-dev/feast/commit/dca47458c81c211fee485a502feebe28426848f0)), closes [#3490](https://github.com/feast-dev/feast/issues/3490)
* Bytewax image pull secret config ([#3547](https://github.com/feast-dev/feast/issues/3547)) ([d2d13b1](https://github.com/feast-dev/feast/commit/d2d13b1762ba67c386fcd48351f3872b92671450))
* Clean up Rockset Online Store for use ([#3549](https://github.com/feast-dev/feast/issues/3549)) ([a76c6d0](https://github.com/feast-dev/feast/commit/a76c6d0f9e3aa28d03d430f6f85ce4e91870c844))
* Feature view `entities` from_proto type ([#3524](https://github.com/feast-dev/feast/issues/3524)) ([57bbb61](https://github.com/feast-dev/feast/commit/57bbb61829fffe08ff3e09aceb1e82ea862b55a2))
* Fix missing requests requirement after GCP requirement removed. Make BigQuerySource not require gcp extra ([2c85421](https://github.com/feast-dev/feast/commit/2c85421fef02dc85854960b4616f00e613934c01))
* Fix SQL Registry cache miss ([#3482](https://github.com/feast-dev/feast/issues/3482)) ([3249b97](https://github.com/feast-dev/feast/commit/3249b97b5471322e068f81fc65d9072f2eed1ba3))
* Fixed path inside quickstart notebook ([#3456](https://github.com/feast-dev/feast/issues/3456)) ([66edc32](https://github.com/feast-dev/feast/commit/66edc32b97f51049dd0fc97765c714c0c6e374e5))
* Improve BQ point-in-time joining scalability ([#3429](https://github.com/feast-dev/feast/issues/3429)) ([ff66784](https://github.com/feast-dev/feast/commit/ff66784ddb1e63e68b88c47132996eccb13891ae))
* Pin typeguard to 2.13.3 which is what we are currently using. ([#3542](https://github.com/feast-dev/feast/issues/3542)) ([61f6fb0](https://github.com/feast-dev/feast/commit/61f6fb03b27cfa92672beb054ee8aba41145645c))
* Protobuf lower bound to 3.20 to alert that Feast is incompatible with tensorflow ([#3476](https://github.com/feast-dev/feast/issues/3476)) ([9ca59e3](https://github.com/feast-dev/feast/commit/9ca59e32d2f1c01cec3b5afaff6802e6036dcad8))
* Spark kafka processor sorting ([#3479](https://github.com/feast-dev/feast/issues/3479)) ([f2cbf43](https://github.com/feast-dev/feast/commit/f2cbf43d4be6829ce3affb72b8a5416d8e084ba9))
* UI working behind base url ([#3514](https://github.com/feast-dev/feast/issues/3514)) ([9a3fd98](https://github.com/feast-dev/feast/commit/9a3fd98468edc6e5fd185d05b8dd1cabac73845c))
* Update go dependencies ([#3512](https://github.com/feast-dev/feast/issues/3512)) ([bada97c](https://github.com/feast-dev/feast/commit/bada97c9dadf05bb369e6f820290b0411bc7412d))


### Features

* Add Rockset as an OnlineStore ([#3405](https://github.com/feast-dev/feast/issues/3405)) ([fd91cda](https://github.com/feast-dev/feast/commit/fd91cda1af47bde948ef6a2f4688785358ae38ec))
* Add Snowflake Registry ([#3363](https://github.com/feast-dev/feast/issues/3363)) ([ec1e61d](https://github.com/feast-dev/feast/commit/ec1e61d11c24247996b0f8508f44ec7f31b9145c))
* Added SnowflakeConnection caching ([#3531](https://github.com/feast-dev/feast/issues/3531)) ([f9f8df2](https://github.com/feast-dev/feast/commit/f9f8df2802b980a3ee161222b174d397764f755a))
* Adding query timeout to `to_df` and `to_arrow` retrieval methods ([#3505](https://github.com/feast-dev/feast/issues/3505)) ([bab6644](https://github.com/feast-dev/feast/commit/bab6644308efb1aa8bce52f2f47df9de87492fc9))
* adds k8s config options to Bytewax materialization engine ([#3518](https://github.com/feast-dev/feast/issues/3518)) ([1883f55](https://github.com/feast-dev/feast/commit/1883f55729ffddaef8d730ba3ffb76eb50cc7201))

# [0.29.0](https://github.com/feast-dev/feast/compare/v0.28.0...v0.29.0) (2023-01-31)


### Bug Fixes

* Add check for bool type in addition to sample ([#3452](https://github.com/feast-dev/feast/issues/3452)) ([1c7c491](https://github.com/feast-dev/feast/commit/1c7c491378c9a5dc892ec58f2d81d4e95b800580))
* Buggy SQL for postgres source ([#3424](https://github.com/feast-dev/feast/issues/3424)) ([1ea100e](https://github.com/feast-dev/feast/commit/1ea100ef472a7cc5b750d4b84992a254b4582de6))
* Ensure no duplicates in `fv.schema` ([#3460](https://github.com/feast-dev/feast/issues/3460)) ([08ffa8d](https://github.com/feast-dev/feast/commit/08ffa8dff61acd7047d205083b78efa98e2dccb8))
* Fix delete sfv twice issue ([#3466](https://github.com/feast-dev/feast/issues/3466)) ([dfd5eae](https://github.com/feast-dev/feast/commit/dfd5eaec6bab4961a7981e4f6a70b45e4d72bce4))
* Stream feature view UI shows transformation issue ([#3464](https://github.com/feast-dev/feast/issues/3464)) ([1ef5137](https://github.com/feast-dev/feast/commit/1ef51376a67347c31ee2e7a037be844526ecc48d))
* Update registry.refresh to have a default arg ([#3450](https://github.com/feast-dev/feast/issues/3450)) ([2f7c4ed](https://github.com/feast-dev/feast/commit/2f7c4ede8f9e66703714261f1152f78526d4bf43))
* Updating the batch field so that you can query create and event date. ([#3411](https://github.com/feast-dev/feast/issues/3411)) ([01ab462](https://github.com/feast-dev/feast/commit/01ab462d49442d8c7f4de418132665e48552c22d)), closes [#3401](https://github.com/feast-dev/feast/issues/3401)


### Features

* Add data source search ([#3449](https://github.com/feast-dev/feast/issues/3449)) ([fbbb293](https://github.com/feast-dev/feast/commit/fbbb2935fd7c722dbe85f19a8ddf788765116360))
* Adding list_validation_references for default and sql registry ([#3436](https://github.com/feast-dev/feast/issues/3436)) ([21dd253](https://github.com/feast-dev/feast/commit/21dd253adda26c18366cf4338512bdc2c00882cf))
* Make UI accessible behind proxy ([#3428](https://github.com/feast-dev/feast/issues/3428)) ([753d8db](https://github.com/feast-dev/feast/commit/753d8dbb5e34c24cf065f599a2cd370b3723de9c))

# [0.28.0](https://github.com/feast-dev/feast/compare/v0.27.0...v0.28.0) (2023-01-03)


### Bug Fixes

* Apply billing project when infer schema ([#3417](https://github.com/feast-dev/feast/issues/3417)) ([4f9ad7e](https://github.com/feast-dev/feast/commit/4f9ad7e0bdc9d87e9fcd64d0268feee86e59d9c0))
* Assertion condition when value is 0 ([#3401](https://github.com/feast-dev/feast/issues/3401)) ([98a24a3](https://github.com/feast-dev/feast/commit/98a24a34c4274464f43c49b33b2b0baa88221cbd))
* Enable registry caching in SQL Registry ([#3395](https://github.com/feast-dev/feast/issues/3395)) ([2e57376](https://github.com/feast-dev/feast/commit/2e573769a24e2429233afe34424af0433b2dc7ec))
* Fix bug where SQL registry was incorrectly writing infra config around online stores ([#3394](https://github.com/feast-dev/feast/issues/3394)) ([6bcf77c](https://github.com/feast-dev/feast/commit/6bcf77c19f84188586ee7dcc57920a43062ee3be))
* Get all columns with describe table method from RedshiftData-api ([#3377](https://github.com/feast-dev/feast/issues/3377)) ([fd97254](https://github.com/feast-dev/feast/commit/fd97254b18605fff7414845d94725a606112b874))
* ODFV able to handle boolean pandas type ([#3384](https://github.com/feast-dev/feast/issues/3384)) ([8f242e6](https://github.com/feast-dev/feast/commit/8f242e640881113f1c6c5137dece8bf01504c61d))
* Remove PySpark dependency from Snowflake Offline Store ([#3388](https://github.com/feast-dev/feast/issues/3388)) ([7b160c7](https://github.com/feast-dev/feast/commit/7b160c74685848a10965d5ab82721eddfb8c3766))
* Specifies timeout in exception polling ([#3398](https://github.com/feast-dev/feast/issues/3398)) ([c0ca7e4](https://github.com/feast-dev/feast/commit/c0ca7e4638d5bcbc9a40778eaf6da0b721da6a6c))
* Update import logic to remove `pyspark` dependency from Snowflake Offline Store ([#3397](https://github.com/feast-dev/feast/issues/3397)) ([cf073e6](https://github.com/feast-dev/feast/commit/cf073e68b0b3953ad901523ef7200e399eb3adfb))


### Features

* Add template for Github Codespaces ([#3421](https://github.com/feast-dev/feast/issues/3421)) ([41c0537](https://github.com/feast-dev/feast/commit/41c0537b0335863f40207ba608348bd315c20148))
* Adds description attribute for features/fields ([#3425](https://github.com/feast-dev/feast/issues/3425)) ([26f4881](https://github.com/feast-dev/feast/commit/26f4881b51d6027fcce24a75add496f90fc36c5d))
* Snowflake skip materialization if no table change ([#3404](https://github.com/feast-dev/feast/issues/3404)) ([0ab3942](https://github.com/feast-dev/feast/commit/0ab3942e5fa0f731e226f8823f62d127a39b5f93))

# [0.27.0](https://github.com/feast-dev/feast/compare/v0.26.0...v0.27.0) (2022-12-05)


### Bug Fixes

* Changing Snowflake template code to avoid query not implemented … ([#3319](https://github.com/feast-dev/feast/issues/3319)) ([1590d6b](https://github.com/feast-dev/feast/commit/1590d6be14b9a9cea6fbde60f2373cb3cd68acb9))
* Dask zero division error if parquet dataset has only one partition ([#3236](https://github.com/feast-dev/feast/issues/3236)) ([69e4a7d](https://github.com/feast-dev/feast/commit/69e4a7d10d912cd89f1e0c2a024d07cf2f44211e))
* Enable Spark materialization on Yarn ([#3370](https://github.com/feast-dev/feast/issues/3370)) ([0c20a4e](https://github.com/feast-dev/feast/commit/0c20a4e6ac95d7c28299a95d91024be4370ef7ed))
* Ensure that Snowflake accounts for number columns that overspecify precision ([#3306](https://github.com/feast-dev/feast/issues/3306)) ([0ad0ace](https://github.com/feast-dev/feast/commit/0ad0ace893edbd0680ee100726ad38bec1436974))
* Fix memory leak from usage.py not properly cleaning up call stack ([#3371](https://github.com/feast-dev/feast/issues/3371)) ([a0c6fde](https://github.com/feast-dev/feast/commit/a0c6fde93bc8088d2bb34d1dd366a44b5d2657ee))
* Fix workflow to contain env vars ([#3379](https://github.com/feast-dev/feast/issues/3379)) ([548bed9](https://github.com/feast-dev/feast/commit/548bed988687dc94147f494d4dbb4147fa5bea8e))
* Update bytewax materialization ([#3368](https://github.com/feast-dev/feast/issues/3368)) ([4ebe00f](https://github.com/feast-dev/feast/commit/4ebe00fefa337cbe96ad3c712f44b9c9d7a46ef2))
* Update the version counts ([#3378](https://github.com/feast-dev/feast/issues/3378)) ([8112db5](https://github.com/feast-dev/feast/commit/8112db5b5a18123da567c310fe385ae907edb56d))
* Updated AWS Athena template ([#3322](https://github.com/feast-dev/feast/issues/3322)) ([5956981](https://github.com/feast-dev/feast/commit/595698105637aaeb952fddc2957c83e501964d2a))
* Wrong UI data source type display ([#3276](https://github.com/feast-dev/feast/issues/3276)) ([8f28062](https://github.com/feast-dev/feast/commit/8f280620bceb3a6e42ffffd0571eeb353b0feff2))


### Features

* Cassandra online store, concurrency in bulk write operations ([#3367](https://github.com/feast-dev/feast/issues/3367)) ([eaf354c](https://github.com/feast-dev/feast/commit/eaf354cde27c8f3a46ac256a71ad5ec06dfd70b3))
* Cassandra online store, concurrent fetching for multiple entities ([#3356](https://github.com/feast-dev/feast/issues/3356)) ([00fa21f](https://github.com/feast-dev/feast/commit/00fa21f9ebef496a6801974cfd3a0a0b4861a11e))
* Get Snowflake Query Output As Pyspark Dataframe ([#2504](https://github.com/feast-dev/feast/issues/2504)) ([#3358](https://github.com/feast-dev/feast/issues/3358)) ([2f18957](https://github.com/feast-dev/feast/commit/2f189572493cb498243bf7d6ec12d768f4b90ee7))

# [0.26.0](https://github.com/feast-dev/feast/compare/v0.25.0...v0.26.0) (2022-10-06)


### Bug Fixes

* Add `X-Trino-Extra-Credential` header and remove user override ([#3246](https://github.com/feast-dev/feast/issues/3246)) ([164e666](https://github.com/feast-dev/feast/commit/164e666ee8c425150903819a0035d6f6e48bcdd0))
* Add postgres to the feature server Dockerfile to fix helm chart flow ([#3261](https://github.com/feast-dev/feast/issues/3261)) ([6f6cbb7](https://github.com/feast-dev/feast/commit/6f6cbb76a216965666719e492c5ba854ada32605))
* Add stream feature view in the Web UI ([#3257](https://github.com/feast-dev/feast/issues/3257)) ([1f70b3a](https://github.com/feast-dev/feast/commit/1f70b3a9be738ab54f0cb59046ed1e299f92e3a2))
* Build dockerfile correctly ([#3239](https://github.com/feast-dev/feast/issues/3239)) ([a2dc0d0](https://github.com/feast-dev/feast/commit/a2dc0d0410eb297afddfb1dd4f1f899ab70fa14f))
* Configuration to stop coercion of tz for entity_df ([#3255](https://github.com/feast-dev/feast/issues/3255)) ([97b7ab9](https://github.com/feast-dev/feast/commit/97b7ab906081d1afe034fc017a2ec61f20818771))
* Enable users to upgrade a batch source into a push source ([#3213](https://github.com/feast-dev/feast/issues/3213)) ([1b312fb](https://github.com/feast-dev/feast/commit/1b312fbb96a0629c0bd465ee09a01a43130c99c7))
* Fix docker image for feature-server ([#3272](https://github.com/feast-dev/feast/issues/3272)) ([eff01d1](https://github.com/feast-dev/feast/commit/eff01d1ed3a76ff539595455f69ff1c954ad46cd))
* Fix Feast UI release process to update the feast-ui package  ([#3267](https://github.com/feast-dev/feast/issues/3267)) ([a9d48d0](https://github.com/feast-dev/feast/commit/a9d48d07f634deb0b992e36d2ae1577d9b627940))
* Return 422 on bad push source name ([#3214](https://github.com/feast-dev/feast/issues/3214)) ([b851e01](https://github.com/feast-dev/feast/commit/b851e01ab4ae78e6714793c7efddb86b39d2a36e))
* Stream feature view meta undefined created_timestamp issue ([#3266](https://github.com/feast-dev/feast/issues/3266)) ([12e1a8f](https://github.com/feast-dev/feast/commit/12e1a8f0e9f098cfc3ed66db6665cb5fa26240f4))
* Stream feature view not shown in the UI ([#3251](https://github.com/feast-dev/feast/issues/3251)) ([e713dda](https://github.com/feast-dev/feast/commit/e713dda7c5a4433f5d13b4685d36978645e0cd02))
* Udf in stream feature view UI shows pickled data ([#3268](https://github.com/feast-dev/feast/issues/3268)) ([0728117](https://github.com/feast-dev/feast/commit/07281178f8be78178b739366c90fd2074db0981b))
* Update snowflake materialization messages ([#3230](https://github.com/feast-dev/feast/issues/3230)) ([a63d440](https://github.com/feast-dev/feast/commit/a63d440e4207c1e360630423bcda2c329673fddd))
* Updated quickstart notebook to patch an incorrect reference to an outdated featureview name ([#3271](https://github.com/feast-dev/feast/issues/3271)) ([b9b9c54](https://github.com/feast-dev/feast/commit/b9b9c542bc0860cfb351be7d84d6461b70ad2c81))
* Use configured user in env var instead of "user" for Trino ([#3254](https://github.com/feast-dev/feast/issues/3254)) ([532d8a1](https://github.com/feast-dev/feast/commit/532d8a1ec5dd50a959b512427fdbc3e5bb986eb4))


### Features

* Add mysql as online store ([#3190](https://github.com/feast-dev/feast/issues/3190)) ([cb8db84](https://github.com/feast-dev/feast/commit/cb8db84a1b200c7a716d23e17f150b013d5689ff))
* Add possibility to define feature_store.yaml path with env variable ([#3231](https://github.com/feast-dev/feast/issues/3231)) ([95fdb19](https://github.com/feast-dev/feast/commit/95fdb19ed3f4b4af7fe1fb732a1fd63182cf1072))
* Add request_timeout setting for cassandra online store adapter ([#3256](https://github.com/feast-dev/feast/issues/3256)) ([da20757](https://github.com/feast-dev/feast/commit/da20757aa1d5c5f898651295781ef61b52d7d712))
* Add tag description in Field in the Feast UI ([#3258](https://github.com/feast-dev/feast/issues/3258)) ([086f279](https://github.com/feast-dev/feast/commit/086f2790236b776f5c2397365266d9d4f4959142))
* Adding billing_project_id in BigQueryOfflineStoreConfig ([#3253](https://github.com/feast-dev/feast/issues/3253)) ([f80f05f](https://github.com/feast-dev/feast/commit/f80f05f5d2042c6fff533b0dc85612acda5a7417))
* BigTable online store ([#3140](https://github.com/feast-dev/feast/issues/3140)) ([6bc91c2](https://github.com/feast-dev/feast/commit/6bc91c23545a6cf765c0e70887afb22eada2c8e1)), closes [/github.com/spulec/moto/blob/master/CHANGELOG.md#400](https://github.com//github.com/spulec/moto/blob/master/CHANGELOG.md/issues/400)
* Filter subset features in postgres [#3174](https://github.com/feast-dev/feast/issues/3174) ([#3203](https://github.com/feast-dev/feast/issues/3203)) ([b48d36b](https://github.com/feast-dev/feast/commit/b48d36bee074788626ed5c2d5cf130bd2fc8bda5))

# [0.25.0](https://github.com/feast-dev/feast/compare/v0.24.0...v0.25.0) (2022-09-20)


### Bug Fixes

* Broken Feature Service Link ([#3227](https://github.com/feast-dev/feast/issues/3227)) ([e117082](https://github.com/feast-dev/feast/commit/e1170822bd3de8e1bfe803d9e2757c760fa5df2f))
* Feature-server image is missing mysql dependency for mysql registry ([#3223](https://github.com/feast-dev/feast/issues/3223)) ([ae37b20](https://github.com/feast-dev/feast/commit/ae37b2058e59a722c45324f5b43668ae4e74657d))
* Fix handling of TTL in Go server ([#3232](https://github.com/feast-dev/feast/issues/3232)) ([f020630](https://github.com/feast-dev/feast/commit/f020630c0144ab366f50c29dc3c97b8501687d3b))
* Fix materialization when running on Spark cluster. ([#3166](https://github.com/feast-dev/feast/issues/3166)) ([175fd25](https://github.com/feast-dev/feast/commit/175fd256e0d21f6539f68708705bddf1caa3d975))
* Fix push API to respect feature view's already inferred entity types ([#3172](https://github.com/feast-dev/feast/issues/3172)) ([7c50ab5](https://github.com/feast-dev/feast/commit/7c50ab510633c11646b6ff04853f3f26942ad646))
* Fix release workflow ([#3144](https://github.com/feast-dev/feast/issues/3144)) ([20a9dd9](https://github.com/feast-dev/feast/commit/20a9dd98550ad8daf291381a771b3da798e4c1a4))
* Fix Shopify timestamp bug and add warnings to help with debugging entity registration ([#3191](https://github.com/feast-dev/feast/issues/3191)) ([de75971](https://github.com/feast-dev/feast/commit/de75971e27357a8fb4a882bd7ec4212148256616))
* Handle complex Spark data types in SparkSource ([#3154](https://github.com/feast-dev/feast/issues/3154)) ([5ddb83b](https://github.com/feast-dev/feast/commit/5ddb83b14817f55e51e5c89014a3439ec3ef5a59))
* Local staging location provision ([#3195](https://github.com/feast-dev/feast/issues/3195)) ([cdf0faf](https://github.com/feast-dev/feast/commit/cdf0fafa6939f67cfb13ee3e28ff16a46c2147ae))
* Remove bad snowflake offline store method ([#3204](https://github.com/feast-dev/feast/issues/3204)) ([dfdd0ca](https://github.com/feast-dev/feast/commit/dfdd0ca5fe54b638ac5a268501d67e5621ca8d89))
* Remove opening file object when validating S3 parquet source ([#3217](https://github.com/feast-dev/feast/issues/3217)) ([a906018](https://github.com/feast-dev/feast/commit/a9060188713e34d07fd82cf3469061fdd2220956))
* Snowflake config file search error ([#3193](https://github.com/feast-dev/feast/issues/3193)) ([189afb9](https://github.com/feast-dev/feast/commit/189afb9313d071c7b6492e0e8a996e6d073a2c6c))
* Update Snowflake Online docs ([#3206](https://github.com/feast-dev/feast/issues/3206)) ([7bc1dff](https://github.com/feast-dev/feast/commit/7bc1dff5882c53c7e25f51ddb0b730bd81091a03))


### Features

* Add `to_remote_storage` functionality to `SparkOfflineStore` ([#3175](https://github.com/feast-dev/feast/issues/3175)) ([2107ce2](https://github.com/feast-dev/feast/commit/2107ce295f191eb1339c8670f963d39e66c4ccf6))
* Add ability to give boto extra args for registry config ([#3219](https://github.com/feast-dev/feast/issues/3219)) ([fbc6a2c](https://github.com/feast-dev/feast/commit/fbc6a2c48303424ef08f9b206e406fc0448e5c6f))
* Add health endpoint to py server ([#3202](https://github.com/feast-dev/feast/issues/3202)) ([43222f2](https://github.com/feast-dev/feast/commit/43222f21046c54a68250350c49b4cdf819d41591))
* Add snowflake support for date & number with scale ([#3148](https://github.com/feast-dev/feast/issues/3148)) ([50e8755](https://github.com/feast-dev/feast/commit/50e8755d41ca2eacd41e31fc0be1202c69b61fdd))
* Add tag kwarg to set Snowflake online store table path ([#3176](https://github.com/feast-dev/feast/issues/3176)) ([39aeea3](https://github.com/feast-dev/feast/commit/39aeea3fa77c3b3a789556a1e0fa22ecedcae4ea))
* Add workgroup to athena offline store config ([#3139](https://github.com/feast-dev/feast/issues/3139)) ([a752211](https://github.com/feast-dev/feast/commit/a752211e1d0d6b44901d88f435328fc355d16c20))
* Implement spark materialization engine ([#3184](https://github.com/feast-dev/feast/issues/3184)) ([a59c33a](https://github.com/feast-dev/feast/commit/a59c33ac10760b4029fadd8e377eb36a2c458583))

# [0.24.0](https://github.com/feast-dev/feast/compare/v0.23.0...v0.24.0) (2022-08-25)


### Bug Fixes

* Check if on_demand_feature_views is an empty list rather than None for snowflake provider ([#3046](https://github.com/feast-dev/feast/issues/3046)) ([9b05e65](https://github.com/feast-dev/feast/commit/9b05e651546d1526aa166854e425285c8ec3a6d5))
* FeatureStore.apply applies BatchFeatureView correctly ([#3098](https://github.com/feast-dev/feast/issues/3098)) ([41be511](https://github.com/feast-dev/feast/commit/41be51170db1f9d9439ac801625458d471827cee))
* Fix Feast Java inconsistency with int64 serialization vs python ([#3031](https://github.com/feast-dev/feast/issues/3031)) ([4bba787](https://github.com/feast-dev/feast/commit/4bba78709e4e5884ce9aad75e84f9b4449665b81))
* Fix feature service inference logic ([#3089](https://github.com/feast-dev/feast/issues/3089)) ([4310ed7](https://github.com/feast-dev/feast/commit/4310ed7e687da0e80a18c6d8cb95cfb15bbd1eae))
* Fix field mapping logic during feature inference ([#3067](https://github.com/feast-dev/feast/issues/3067)) ([cdfa761](https://github.com/feast-dev/feast/commit/cdfa761a16206afcdb64959c25ee3b5f2b312566))
* Fix incorrect on demand feature view diffing and improve Java tests ([#3074](https://github.com/feast-dev/feast/issues/3074)) ([0702310](https://github.com/feast-dev/feast/commit/0702310366882a388af4f299a69467841c132259))
* Fix Java helm charts to work with refactored logic. Fix FTS image ([#3105](https://github.com/feast-dev/feast/issues/3105)) ([2b493e0](https://github.com/feast-dev/feast/commit/2b493e0457cea19a9b3faa163f099d6b32fde30d))
* Fix on demand feature view output in feast plan + Web UI crash ([#3057](https://github.com/feast-dev/feast/issues/3057)) ([bfae6ac](https://github.com/feast-dev/feast/commit/bfae6ac5a42fcdeebfaed2d1473c546da23c3bdc))
* Fix release workflow to release 0.24.0 ([#3138](https://github.com/feast-dev/feast/issues/3138)) ([a69aaae](https://github.com/feast-dev/feast/commit/a69aaae4c5595e87501e0b5d58533360306fb831))
* Fix Spark offline store type conversion to arrow ([#3071](https://github.com/feast-dev/feast/issues/3071)) ([b26566d](https://github.com/feast-dev/feast/commit/b26566d92573164d9968fb356fd68446725f70f5))
* Fixing Web UI, which fails for the SQL registry ([#3028](https://github.com/feast-dev/feast/issues/3028)) ([64603b6](https://github.com/feast-dev/feast/commit/64603b677421b21f04bd72238e358dac43122b29))
* Force Snowflake Session to Timezone UTC ([#3083](https://github.com/feast-dev/feast/issues/3083)) ([9f221e6](https://github.com/feast-dev/feast/commit/9f221e66eb2dd83b0e6beb528a694f4933953571))
* Make infer dummy entity join key idempotent ([#3115](https://github.com/feast-dev/feast/issues/3115)) ([1f5b1e0](https://github.com/feast-dev/feast/commit/1f5b1e078b41729938ab6b4d9a35c2fcb2be39cd))
* More explicit error messages ([#2708](https://github.com/feast-dev/feast/issues/2708)) ([e4d7afd](https://github.com/feast-dev/feast/commit/e4d7afdce8fc2596d1a27f2d85f259f2fa35bafa))
* Parse inline data sources ([#3036](https://github.com/feast-dev/feast/issues/3036)) ([c7ba370](https://github.com/feast-dev/feast/commit/c7ba370aa14ef3216c84aaa8852fd519931840bf))
* Prevent overwriting existing file during `persist` ([#3088](https://github.com/feast-dev/feast/issues/3088)) ([69af21f](https://github.com/feast-dev/feast/commit/69af21f4c487506417d7bbb3ad32f1be2246a654))
* Register BatchFeatureView in feature repos correctly ([#3092](https://github.com/feast-dev/feast/issues/3092)) ([b8e39ea](https://github.com/feast-dev/feast/commit/b8e39ea4cd2d990f2422c60bf39d8d940ecc9522))
* Return an empty infra object from sql registry when it doesn't exist ([#3022](https://github.com/feast-dev/feast/issues/3022)) ([8ba87d1](https://github.com/feast-dev/feast/commit/8ba87d1b550526c24bb5f6b3ce63c6435676a5cb))
* Teardown tables for Snowflake Materialization testing ([#3106](https://github.com/feast-dev/feast/issues/3106)) ([0a0c974](https://github.com/feast-dev/feast/commit/0a0c974f38e7ef41ecf0af5c6a3eacac7369aa38))
* UI error when saved dataset is present in registry. ([#3124](https://github.com/feast-dev/feast/issues/3124)) ([83cf753](https://github.com/feast-dev/feast/commit/83cf7533fa757af7eb3ab1c4f540aca8edd134af))
* Update sql.py ([#3096](https://github.com/feast-dev/feast/issues/3096)) ([2646a86](https://github.com/feast-dev/feast/commit/2646a864b0031617b26577926ade2341f998557b))
* Updated snowflake template ([#3130](https://github.com/feast-dev/feast/issues/3130)) ([f0594e1](https://github.com/feast-dev/feast/commit/f0594e160a1f11d896e884b40f7e1110d2df6aa9))


### Features

* Add authentication option for snowflake connector ([#3039](https://github.com/feast-dev/feast/issues/3039)) ([74c75f1](https://github.com/feast-dev/feast/commit/74c75f1f4c91f0097f9a1085a4e68a07c524037d))
* Add Cassandra/AstraDB online store contribution ([#2873](https://github.com/feast-dev/feast/issues/2873)) ([feb6cb8](https://github.com/feast-dev/feast/commit/feb6cb8518889288d6ddd97e4482db2f6b86eabd))
* Add Snowflake materialization engine ([#2948](https://github.com/feast-dev/feast/issues/2948)) ([f3b522b](https://github.com/feast-dev/feast/commit/f3b522b007cc5e5ccd32dbe04e47d30136810f6c))
* Adding saved dataset capabilities for Postgres  ([#3070](https://github.com/feast-dev/feast/issues/3070)) ([d3253c3](https://github.com/feast-dev/feast/commit/d3253c362deb775a8f1f5cd325e44d3e598d0bdf))
* Allow passing repo config path via flag ([#3077](https://github.com/feast-dev/feast/issues/3077)) ([0d2d951](https://github.com/feast-dev/feast/commit/0d2d951d565daac1a4f01fab988d44010b6856bb))
* Contrib azure provider with synapse/mssql offline store and Azure registry store ([#3072](https://github.com/feast-dev/feast/issues/3072)) ([9f7e557](https://github.com/feast-dev/feast/commit/9f7e5573e764466590badab4250b69aef6f256b0))
* Custom Docker image for Bytewax batch materialization ([#3099](https://github.com/feast-dev/feast/issues/3099)) ([cdd1b07](https://github.com/feast-dev/feast/commit/cdd1b0734fcb20c258cf6ee4c067f23d2fff81e0))
* Feast AWS Athena offline store (again) ([#3044](https://github.com/feast-dev/feast/issues/3044)) ([989ce08](https://github.com/feast-dev/feast/commit/989ce085c0949564af61afb73401c27669cfdaba))
* Implement spark offline store `offline_write_batch` method ([#3076](https://github.com/feast-dev/feast/issues/3076)) ([5b0cc87](https://github.com/feast-dev/feast/commit/5b0cc8798616455d955b543cb8012ad88927aea2))
* Initial Bytewax materialization engine ([#2974](https://github.com/feast-dev/feast/issues/2974)) ([55c61f9](https://github.com/feast-dev/feast/commit/55c61f9c4584bf040a7dc1719200704402811d6d))
* Refactor feature server helm charts to allow passing feature_store.yaml in environment variables ([#3113](https://github.com/feast-dev/feast/issues/3113)) ([85ee789](https://github.com/feast-dev/feast/commit/85ee78947bc9793b17348c08325844a2ee4ad0ff))

# [0.23.0](https://github.com/feast-dev/feast/compare/v0.22.0...v0.23.0) (2022-08-02)


### Bug Fixes

* Add dummy alias to pull_all_from_table_or_query ([#2956](https://github.com/feast-dev/feast/issues/2956)) ([5e45228](https://github.com/feast-dev/feast/commit/5e45228a406e6ee7f82e41cab7f734730ff2e73f))
* Bump version of Guava to mitigate cve ([#2896](https://github.com/feast-dev/feast/issues/2896)) ([51df8be](https://github.com/feast-dev/feast/commit/51df8be5d3b9bc702393d00e9a6370c703510358))
* Change numpy version on setup.py and upgrade it to resolve dependabot warning ([#2887](https://github.com/feast-dev/feast/issues/2887)) ([80ea7a9](https://github.com/feast-dev/feast/commit/80ea7a93a9d7ea19f9a1218430e008a33eb6d788))
* Change the feature store plan method to public modifier ([#2904](https://github.com/feast-dev/feast/issues/2904)) ([0ec7d1a](https://github.com/feast-dev/feast/commit/0ec7d1abd3f509e17870ca168ece356382fb7fe9))
* Deprecate 3.7 wheels and fix verification workflow ([#2934](https://github.com/feast-dev/feast/issues/2934)) ([040c910](https://github.com/feast-dev/feast/commit/040c9107b719a7b3f3c70ab743f148e47b0a0982))
* Do not allow same column to be reused in data sources ([#2965](https://github.com/feast-dev/feast/issues/2965)) ([661c053](https://github.com/feast-dev/feast/commit/661c0535f34b042846562a3fb4cdab4ab4403459))
* Fix build wheels workflow to install apache-arrow correctly ([#2932](https://github.com/feast-dev/feast/issues/2932)) ([bdeb4ae](https://github.com/feast-dev/feast/commit/bdeb4aeaf2a5cfa144a65cc84f7bfb26e3077e7a))
* Fix file offline store logic for feature views without ttl ([#2971](https://github.com/feast-dev/feast/issues/2971)) ([26f6b69](https://github.com/feast-dev/feast/commit/26f6b69b0e2c8a4ea37b43e3d1eaa4cdb8c085a9))
* Fix grpc and update protobuf ([#2894](https://github.com/feast-dev/feast/issues/2894)) ([86e9efd](https://github.com/feast-dev/feast/commit/86e9efdc893de817a359feb939f06717716c0b17))
* Fix night ci syntax error and update readme ([#2935](https://github.com/feast-dev/feast/issues/2935)) ([b917540](https://github.com/feast-dev/feast/commit/b917540c27052c01f872a2de686a6dd3b7a16e9c))
* Fix nightly ci again ([#2939](https://github.com/feast-dev/feast/issues/2939)) ([1603c9e](https://github.com/feast-dev/feast/commit/1603c9e7765e08bb1832c03b66b754afbf8a9b4d))
* Fix the go build and use CgoArrowAllocator to prevent incorrect garbage collection ([#2919](https://github.com/feast-dev/feast/issues/2919)) ([130746e](https://github.com/feast-dev/feast/commit/130746ea5cfadad6ef467c0cb0490d4745fdad70))
* Fix typo in CONTRIBUTING.md ([#2955](https://github.com/feast-dev/feast/issues/2955)) ([8534f69](https://github.com/feast-dev/feast/commit/8534f69026d03e6e5964ef3e9bc69cc18397a879))
* Fixing broken links to feast documentation on java readme and contribution ([#2892](https://github.com/feast-dev/feast/issues/2892)) ([d044588](https://github.com/feast-dev/feast/commit/d044588d702b3dc2dd6b9a9e28056df19d942a09))
* Fixing Spark min / max entity df event timestamps range return order ([#2735](https://github.com/feast-dev/feast/issues/2735)) ([ac55ce2](https://github.com/feast-dev/feast/commit/ac55ce25388abfa35e93097bd14190eeba08a165))
* Move gcp back to 1.47.0 since grpcio-tools 1.48.0 got yanked from pypi ([#2990](https://github.com/feast-dev/feast/issues/2990)) ([fc447eb](https://github.com/feast-dev/feast/commit/fc447eb3d0345dba6a45cdf5b1c1c2e982766cb9))
* Refactor testing and sort out unit and integration tests ([#2975](https://github.com/feast-dev/feast/issues/2975)) ([2680f7b](https://github.com/feast-dev/feast/commit/2680f7b031717b64e6ea3addf150369dccebdbc1))
* Remove hard-coded integration test setup for AWS & GCP ([#2970](https://github.com/feast-dev/feast/issues/2970)) ([e4507ac](https://github.com/feast-dev/feast/commit/e4507ac16540cb3a7e29c31121963a0fe8f79fe4))
* Resolve small typo in README file ([#2930](https://github.com/feast-dev/feast/issues/2930)) ([16ae902](https://github.com/feast-dev/feast/commit/16ae902909911bbf45d0e430895b3bc20bba01e9))
* Revert "feat: Add snowflake online store ([#2902](https://github.com/feast-dev/feast/issues/2902))" ([#2909](https://github.com/feast-dev/feast/issues/2909)) ([38fd001](https://github.com/feast-dev/feast/commit/38fd00195f8ed309b2e7bae06d48cb10ab82f5aa))
* Snowflake_online_read fix ([#2988](https://github.com/feast-dev/feast/issues/2988)) ([651ce34](https://github.com/feast-dev/feast/commit/651ce341687034ce07ca959f805f3c90dccfd4cc))
* Spark source support table with pattern "db.table" ([#2606](https://github.com/feast-dev/feast/issues/2606)) ([3ce5139](https://github.com/feast-dev/feast/commit/3ce51391e0b2ebdec68c81d93b54f5d06bb427a6)), closes [#2605](https://github.com/feast-dev/feast/issues/2605)
* Switch mysql log string to use regex ([#2976](https://github.com/feast-dev/feast/issues/2976)) ([5edf4b0](https://github.com/feast-dev/feast/commit/5edf4b0332a298a0e172dd58e0a627efe5705eec))
* Update gopy to point to fork to resolve github annotation errors. ([#2940](https://github.com/feast-dev/feast/issues/2940)) ([ba2dcf1](https://github.com/feast-dev/feast/commit/ba2dcf13fe9dc4c082816a737100e00e3e9a8ad2))
* Version entity serialization mechanism and fix issue with int64 vals ([#2944](https://github.com/feast-dev/feast/issues/2944)) ([d0d27a3](https://github.com/feast-dev/feast/commit/d0d27a35a0d63a139970cb17542764ff2aaf6aaf))


### Features

* Add an experimental lambda-based materialization engine ([#2923](https://github.com/feast-dev/feast/issues/2923)) ([6f79069](https://github.com/feast-dev/feast/commit/6f79069c561eba888d070c46aae920f7ad0c2319))
* Add column reordering to `write_to_offline_store` ([#2876](https://github.com/feast-dev/feast/issues/2876)) ([8abc2ef](https://github.com/feast-dev/feast/commit/8abc2ef76d461b6b4bbd97e2dfdf29c1c335cb80))
* Add custom JSON table tab w/ formatting ([#2851](https://github.com/feast-dev/feast/issues/2851)) ([0159f38](https://github.com/feast-dev/feast/commit/0159f3875de7c8509c465346bd13dd11fba0d467))
* Add CustomSourceOptions to SavedDatasetStorage ([#2958](https://github.com/feast-dev/feast/issues/2958)) ([23c09c8](https://github.com/feast-dev/feast/commit/23c09c83bc530de830ba867b10ceb02f113db5d6))
* Add Go option to `feast serve` command ([#2966](https://github.com/feast-dev/feast/issues/2966)) ([a36a695](https://github.com/feast-dev/feast/commit/a36a6950b34d718ad328b4faca0c178fb23a3100))
* Add interfaces for batch materialization engine ([#2901](https://github.com/feast-dev/feast/issues/2901)) ([38b28ca](https://github.com/feast-dev/feast/commit/38b28ca0181610c65d966a2f09456dbb102fbced))
* Add pages for individual Features to the Feast UI ([#2850](https://github.com/feast-dev/feast/issues/2850)) ([9b97fca](https://github.com/feast-dev/feast/commit/9b97fca876d9520d6e1f9025562036330cc0aabd))
* Add snowflake online store ([#2902](https://github.com/feast-dev/feast/issues/2902)) ([f758f9e](https://github.com/feast-dev/feast/commit/f758f9e148212d08f63df155e864940c27d92155)), closes [#2903](https://github.com/feast-dev/feast/issues/2903)
* Add Snowflake online store (again) ([#2922](https://github.com/feast-dev/feast/issues/2922)) ([2ef71fc](https://github.com/feast-dev/feast/commit/2ef71fc6b3ec4fca3b543f2f64bed765b09c3af4)), closes [#2903](https://github.com/feast-dev/feast/issues/2903)
* Add to_remote_storage method to RetrievalJob ([#2916](https://github.com/feast-dev/feast/issues/2916)) ([109ee9c](https://github.com/feast-dev/feast/commit/109ee9cff5bcda46889583f2968003f6a3e375b3))
* Support retrieval from multiple feature views with different join keys ([#2835](https://github.com/feast-dev/feast/issues/2835)) ([056cfa1](https://github.com/feast-dev/feast/commit/056cfa1b21db4ff092b9d1f9c06f7300a4c9f4b7))

# [0.22.0](https://github.com/feast-dev/feast/compare/v0.21.0...v0.22.0) (2022-06-29)


### Bug Fixes

* Add columns for user metadata in the tables ([#2760](https://github.com/feast-dev/feast/issues/2760)) ([269055e](https://github.com/feast-dev/feast/commit/269055e25178956715c163547c9f0a33a5892a75))
* Add project columns in the SQL Registry ([#2784](https://github.com/feast-dev/feast/issues/2784)) ([336fdd1](https://github.com/feast-dev/feast/commit/336fdd104d2669d19ca56b0d17eadb78fc95a3cd))
* Add S3FS dependency (which Dask depends on for S3 files) ([#2701](https://github.com/feast-dev/feast/issues/2701)) ([5d6fa94](https://github.com/feast-dev/feast/commit/5d6fa9408052695dfce179ea619d8362898d6329))
* Bugfixes for how registry is loaded ([#2768](https://github.com/feast-dev/feast/issues/2768)) ([ecb8b2a](https://github.com/feast-dev/feast/commit/ecb8b2af4ba0f9d52be8ac560ac2a9da0f46f38e))
* Conversion of null timestamp from proto to python ([#2814](https://github.com/feast-dev/feast/issues/2814)) ([cb23648](https://github.com/feast-dev/feast/commit/cb23648da71cbf63e382ec75a8afe7228ff8cbc2))
* Correct feature statuses during feature logging test ([#2709](https://github.com/feast-dev/feast/issues/2709)) ([cebf609](https://github.com/feast-dev/feast/commit/cebf609309f81a0f4ccded3448cefac5923be525))
* Correctly generate projects-list.json when calling feast ui and using postgres as a source ([#2845](https://github.com/feast-dev/feast/issues/2845)) ([bee8076](https://github.com/feast-dev/feast/commit/bee8076085e9f42e783fb9ce5ad07b618d913f0d))
* Dynamodb drops missing entities when batching ([#2802](https://github.com/feast-dev/feast/issues/2802)) ([a2e9209](https://github.com/feast-dev/feast/commit/a2e9209f7a7283925b56b5877e8fdc2e2d863798))
* Enable faulthandler and disable flaky tests ([#2815](https://github.com/feast-dev/feast/issues/2815)) ([4934d84](https://github.com/feast-dev/feast/commit/4934d843fc65dd62ba1c1302243c1b2c151c78d2))
* Explicitly translate errors when instantiating the go fs  ([#2842](https://github.com/feast-dev/feast/issues/2842)) ([7a2c4cd](https://github.com/feast-dev/feast/commit/7a2c4cd8bf9b16331fad1d2e7d0ea3f85589f96b))
* Fix broken roadmap links ([#2690](https://github.com/feast-dev/feast/issues/2690)) ([b3ba8aa](https://github.com/feast-dev/feast/commit/b3ba8aaf3a87343d756a2996376865096d543515))
* Fix bugs in applying stream feature view and retrieving online features  ([#2754](https://github.com/feast-dev/feast/issues/2754)) ([d024e5e](https://github.com/feast-dev/feast/commit/d024e5efac085ec12a17005389229bfd93cf466e))
* Fix Feast UI failure with new way of specifying entities ([#2773](https://github.com/feast-dev/feast/issues/2773)) ([0d1ac01](https://github.com/feast-dev/feast/commit/0d1ac01ef45a1ee78799c7c5ebf30b2476bfc30e))
* Fix feature view __getitem__ for feature services ([#2769](https://github.com/feast-dev/feast/issues/2769)) ([88cc47d](https://github.com/feast-dev/feast/commit/88cc47dea936f34c0798b6c6c31fda0d1b3ecbd7))
* Fix issue when user specifies a port for feast ui  ([#2692](https://github.com/feast-dev/feast/issues/2692)) ([1c621fe](https://github.com/feast-dev/feast/commit/1c621fe3649900a59e85fe9c4f3840dd09bc88d0))
* Fix macos wheel version for 310 and also checkout edited go files ([#2890](https://github.com/feast-dev/feast/issues/2890)) ([bdf170f](https://github.com/feast-dev/feast/commit/bdf170f37971abc34930d62559374882139d69b6))
* Fix on demand feature view crash from inference when it uses df.apply ([#2713](https://github.com/feast-dev/feast/issues/2713)) ([c5539fd](https://github.com/feast-dev/feast/commit/c5539fd9874fed3f69b0aaebc4d1d32e624bd041))
* Fix SparkKafkaProcessor `query_timeout` parameter ([#2789](https://github.com/feast-dev/feast/issues/2789)) ([a8d282d](https://github.com/feast-dev/feast/commit/a8d282d3e4f041824ef7479f22c306dbfb8ad569))
* Fix workflow syntax error ([#2869](https://github.com/feast-dev/feast/issues/2869)) ([fae45a1](https://github.com/feast-dev/feast/commit/fae45a11e78c23a58012d1d3cd2b9eb20a267794))
* Fixed custom S3 endpoint read fail ([#2786](https://github.com/feast-dev/feast/issues/2786)) ([6fec431](https://github.com/feast-dev/feast/commit/6fec431dd5c9d53a678f58c9b87e2b1cdb02b238))
* Go install gopy instead using go mod tidy ([#2863](https://github.com/feast-dev/feast/issues/2863)) ([2f2b519](https://github.com/feast-dev/feast/commit/2f2b5192f5f9c3e38183a2912f3bc7e754c9db8a))
* Hydrate infra object in the sql registry proto() method  ([#2782](https://github.com/feast-dev/feast/issues/2782)) ([452dcd3](https://github.com/feast-dev/feast/commit/452dcd31da195273ef80ee39db5a7893b7d48cc2))
* Implement apply_materialization and infra methods in sql registry ([#2775](https://github.com/feast-dev/feast/issues/2775)) ([4ed107c](https://github.com/feast-dev/feast/commit/4ed107cdf6476faf20a4e09716ade87cb99f1d14))
* Minor refactor to format exception message ([#2764](https://github.com/feast-dev/feast/issues/2764)) ([da763c6](https://github.com/feast-dev/feast/commit/da763c6766cd7bec801312849e884b9dc8f8fb51))
* Prefer installing gopy from feast's fork as opposed to upstream ([#2839](https://github.com/feast-dev/feast/issues/2839)) ([34c997d](https://github.com/feast-dev/feast/commit/34c997d81b0084d81fb6fb21d5d4374fc7760695))
* Python server is not correctly starting in integration tests ([#2706](https://github.com/feast-dev/feast/issues/2706)) ([7583a0b](https://github.com/feast-dev/feast/commit/7583a0b1840c663af50bf4382c3ff2368593bb9c))
* Random port allocation for python server in tests ([#2710](https://github.com/feast-dev/feast/issues/2710)) ([dee8090](https://github.com/feast-dev/feast/commit/dee80908ffb247367526561de3ad4841516a3196))
* Refactor test to reuse LocalRegistryFile ([#2763](https://github.com/feast-dev/feast/issues/2763)) ([4339c0a](https://github.com/feast-dev/feast/commit/4339c0acc990de2b00db90221f41ac0d33e68544))
* Revert "chore(release): release 0.22.0" ([#2852](https://github.com/feast-dev/feast/issues/2852)) ([e6a4636](https://github.com/feast-dev/feast/commit/e6a463666e53b87c9d41334f6151df5e2c09c805))
* Stop running go mod tidy in setup.py ([#2877](https://github.com/feast-dev/feast/issues/2877)) ([676ecbb](https://github.com/feast-dev/feast/commit/676ecbb31550275017d83a6bc8dbf1e03f0d24fa)), closes [/github.com/pypa/cibuildwheel/issues/189#issuecomment-549933912](https://github.com//github.com/pypa/cibuildwheel/issues/189/issues/issuecomment-549933912)
* Support push sources in stream feature views ([#2704](https://github.com/feast-dev/feast/issues/2704)) ([0d60eaa](https://github.com/feast-dev/feast/commit/0d60eaa0b7f32c84eb015c814a3c879e7d4f50fd))
* Sync publish and build_wheels workflow to fix verify wheel error. ([#2871](https://github.com/feast-dev/feast/issues/2871)) ([b0f050a](https://github.com/feast-dev/feast/commit/b0f050a31946f9ff43ac3a3523d4dbd2a9802cc3))
* Update roadmap with stream feature view rfc ([#2824](https://github.com/feast-dev/feast/issues/2824)) ([fc8f890](https://github.com/feast-dev/feast/commit/fc8f89059d1095e81e265f342ccaa26ff7f444f9))
* Update udf tests and add base functions to streaming fcos and fix some nonetype errors ([#2776](https://github.com/feast-dev/feast/issues/2776)) ([331a214](https://github.com/feast-dev/feast/commit/331a214b04dc8b3f9347e79819130fa7bdf9e4c3))


### Features

* Add feast repo-upgrade for automated repo upgrades ([#2733](https://github.com/feast-dev/feast/issues/2733)) ([a3304d4](https://github.com/feast-dev/feast/commit/a3304d4e2d6d803f2a0fe35ef74204bd5cef7517))
* Add file write_to_offline_store functionality ([#2808](https://github.com/feast-dev/feast/issues/2808)) ([c0e2ad7](https://github.com/feast-dev/feast/commit/c0e2ad7bf7310289ec6a7a6bd8cd2f766786b0cd))
* Add http endpoint to the Go feature server ([#2658](https://github.com/feast-dev/feast/issues/2658)) ([3347a57](https://github.com/feast-dev/feast/commit/3347a57240cd485b7572777d7b977869140ccb20))
* Add simple TLS support in Go RedisOnlineStore ([#2860](https://github.com/feast-dev/feast/issues/2860)) ([521488d](https://github.com/feast-dev/feast/commit/521488d71fa5050f64aa04a8ba7ef9891a57ca94))
* Add StreamProcessor and SparkKafkaProcessor as contrib ([#2777](https://github.com/feast-dev/feast/issues/2777)) ([83ab682](https://github.com/feast-dev/feast/commit/83ab682c14a11a92121866409bddc787021d52e5))
* Added Spark support for Delta and Avro ([#2757](https://github.com/feast-dev/feast/issues/2757)) ([7d16516](https://github.com/feast-dev/feast/commit/7d1651687a474850ebb16d4c9c0ff4a3daa6d486))
* CLI interface for validation of logged features ([#2718](https://github.com/feast-dev/feast/issues/2718)) ([c8b11b3](https://github.com/feast-dev/feast/commit/c8b11b3b790b60e916d3257a036b9cb6430f4685))
* Enable stream feature view materialization ([#2798](https://github.com/feast-dev/feast/issues/2798)) ([a06700d](https://github.com/feast-dev/feast/commit/a06700dd81c9893e98f6709b82a6faa32be49915))
* Enable stream feature view materialization ([#2807](https://github.com/feast-dev/feast/issues/2807)) ([7d57724](https://github.com/feast-dev/feast/commit/7d57724dd4ff7d5ca4549bac8c72dbd71c57fce7))
* Implement `offline_write_batch` for BigQuery and Snowflake ([#2840](https://github.com/feast-dev/feast/issues/2840)) ([97444e4](https://github.com/feast-dev/feast/commit/97444e439d0dc7a66b7121161c6f6560ae53d307))
* Offline push endpoint for pushing to offline stores ([#2837](https://github.com/feast-dev/feast/issues/2837)) ([a88cd30](https://github.com/feast-dev/feast/commit/a88cd30f7925005db6f7c400b391d5e73d1b00f6))
* Push to Redshift batch source offline store directly ([#2819](https://github.com/feast-dev/feast/issues/2819)) ([5748a8b](https://github.com/feast-dev/feast/commit/5748a8bbe338dfcb3fbbdc59fb1f57a99e1ea5eb))
* Scaffold for unified push api ([#2796](https://github.com/feast-dev/feast/issues/2796)) ([1bd0930](https://github.com/feast-dev/feast/commit/1bd093028785ac9349be56c9ea98a3bd94c47fbe))
* SQLAlchemy Registry Support ([#2734](https://github.com/feast-dev/feast/issues/2734)) ([b3fe39c](https://github.com/feast-dev/feast/commit/b3fe39c1600fa370f28c7b01e2b3f7da716449c1))
* Stream Feature View FCOS ([#2750](https://github.com/feast-dev/feast/issues/2750)) ([0cf3c92](https://github.com/feast-dev/feast/commit/0cf3c923717f561d5656c57eb0b61fcd569917bd))
* Update stream fcos to have watermark and sliding interval ([#2765](https://github.com/feast-dev/feast/issues/2765)) ([3256952](https://github.com/feast-dev/feast/commit/325695275da610cecf2b9e820fd71f7f04179ccf))
* Validating logged features via Python SDK ([#2640](https://github.com/feast-dev/feast/issues/2640)) ([2874fc5](https://github.com/feast-dev/feast/commit/2874fc5c85810a65f750377d34418c71e747110e))


### Reverts

* Revert "chore(release): release 0.22.0" (#2891) ([e5abf58](https://github.com/feast-dev/feast/commit/e5abf589020b3c261ac0ce38d295ba96daf317c2)), closes [#2891](https://github.com/feast-dev/feast/issues/2891)
* Revert "chore(release): release 0.22.0" (#2870) ([ffb0892](https://github.com/feast-dev/feast/commit/ffb089241d6521caa3be4034e6ae44af7dc4f8af)), closes [#2870](https://github.com/feast-dev/feast/issues/2870)
* Revert "Create main.yml" (#2867) ([47922a4](https://github.com/feast-dev/feast/commit/47922a4cda532871eecd5e17edef6d08a4a50110)), closes [#2867](https://github.com/feast-dev/feast/issues/2867)

# [0.21.0](https://github.com/feast-dev/feast/compare/v0.20.0...v0.21.0) (2022-05-13)


### Bug Fixes

* Addresses ZeroDivisionError when materializing file source with same timestamps ([#2551](https://github.com/feast-dev/feast/issues/2551)) ([1e398d9](https://github.com/feast-dev/feast/commit/1e398d9089dc4674be73ea0869efb0aede50cd93))
* Asynchronously refresh registry for the feast ui command ([#2672](https://github.com/feast-dev/feast/issues/2672)) ([1b09ca2](https://github.com/feast-dev/feast/commit/1b09ca26e0ab7ba5593278e039bc64d293634e67))
* Build platform specific python packages with ci-build-wheel ([#2555](https://github.com/feast-dev/feast/issues/2555)) ([b10a4cf](https://github.com/feast-dev/feast/commit/b10a4cfec40514d5c36cefdb4be24d39fdb1e74a))
* Delete data sources from registry when using the diffing logic ([#2669](https://github.com/feast-dev/feast/issues/2669)) ([fc00ca8](https://github.com/feast-dev/feast/commit/fc00ca8fc091ab2642121de69d4624783f11445c))
* Enforce kw args featureservice ([#2575](https://github.com/feast-dev/feast/issues/2575)) ([160d7b7](https://github.com/feast-dev/feast/commit/160d7b71a74531909fab575cb263d02b3996fac2))
* Enforce kw args in datasources ([#2567](https://github.com/feast-dev/feast/issues/2567)) ([0b7ec53](https://github.com/feast-dev/feast/commit/0b7ec5386e66d21ac9e6d728e14e3663f27cf146))
* Feature logging to Redshift is broken ([#2655](https://github.com/feast-dev/feast/issues/2655)) ([479cd51](https://github.com/feast-dev/feast/commit/479cd512213635551e70ae599564a898046461b2))
* Feature service to templates ([#2649](https://github.com/feast-dev/feast/issues/2649)) ([1e02066](https://github.com/feast-dev/feast/commit/1e0206627fef891aa3a3f0b0f46507449e3663e7))
* Feature with timestamp type is incorrectly interpreted by Go FS ([#2588](https://github.com/feast-dev/feast/issues/2588)) ([e3d9588](https://github.com/feast-dev/feast/commit/e3d958863b32df0158b278fb97d392397cdff672))
* Fix `__hash__` methods ([#2556](https://github.com/feast-dev/feast/issues/2556)) ([ebb7dfe](https://github.com/feast-dev/feast/commit/ebb7dfeaee9dc337a90f5d6b984ad07365018d42))
* Fix AWS bootstrap template ([#2604](https://github.com/feast-dev/feast/issues/2604)) ([c94a69c](https://github.com/feast-dev/feast/commit/c94a69cff541feb1557770bdaf77cd7cc4f2f919))
* Fix broken proto conversion methods for data sources ([#2603](https://github.com/feast-dev/feast/issues/2603)) ([00ed65a](https://github.com/feast-dev/feast/commit/00ed65a77177cfe04877e9550d1c8c1e903dadf8))
* Fix case where on demand feature view tab is broken if no custom tabs are passed.  ([#2682](https://github.com/feast-dev/feast/issues/2682)) ([01d3568](https://github.com/feast-dev/feast/commit/01d3568168bb9febb9fbda4988283b3886c32a31))
* Fix DynamoDB fetches when there are entities that are not found ([#2573](https://github.com/feast-dev/feast/issues/2573)) ([7076fe0](https://github.com/feast-dev/feast/commit/7076fe0483de50af21fe7d7e7da192823f66c3da))
* Fix Feast UI parser to work with new APIs ([#2668](https://github.com/feast-dev/feast/issues/2668)) ([8d76751](https://github.com/feast-dev/feast/commit/8d76751d1e702ee7059def7721b97cfcdf9f6300))
* Fix java server after odfv update ([#2602](https://github.com/feast-dev/feast/issues/2602)) ([0ca6297](https://github.com/feast-dev/feast/commit/0ca62970dd6bc33c00bd5d8b828752814d480588))
* Fix materialization with ttl=0 bug ([#2666](https://github.com/feast-dev/feast/issues/2666)) ([ab78702](https://github.com/feast-dev/feast/commit/ab78702e20b4aaff13497dc8d29495f58ae5ee36))
* Fix push sources and add docs / tests pushing via the python feature server ([#2561](https://github.com/feast-dev/feast/issues/2561)) ([e8e418e](https://github.com/feast-dev/feast/commit/e8e418ed87481638e098d52ee2ca2d0327b63cc9))
* Fixed data mapping errors for Snowflake ([#2558](https://github.com/feast-dev/feast/issues/2558)) ([53c2ce2](https://github.com/feast-dev/feast/commit/53c2ce2c2255791fdd85aa80d69ab6214106e169))
* Forcing ODFV udfs to be __main__ module and fixing false positive duplicate data source warning ([#2677](https://github.com/feast-dev/feast/issues/2677)) ([2ce33cd](https://github.com/feast-dev/feast/commit/2ce33cdd5e0a6a855e60c067aadafddf10ea2359))
* Include the ui/build directory, and remove package data ([#2681](https://github.com/feast-dev/feast/issues/2681)) ([0384f5f](https://github.com/feast-dev/feast/commit/0384f5f78765a3d43534919428c4cc4744a101f2))
* Infer features for feature services when they depend on feature views without schemas ([#2653](https://github.com/feast-dev/feast/issues/2653)) ([87c194c](https://github.com/feast-dev/feast/commit/87c194c22b2732c7060a130bb8861eff0d168c0b))
* Pin dependencies to nearest major version ([#2647](https://github.com/feast-dev/feast/issues/2647)) ([bb72b7c](https://github.com/feast-dev/feast/commit/bb72b7c9f13442d6350867359ab764fe67c74c4c))
* Pin pip<22.1 to get around breaking change in pip==22.1 ([#2678](https://github.com/feast-dev/feast/issues/2678)) ([d3e01bc](https://github.com/feast-dev/feast/commit/d3e01bc74da9f4678d3cf384afd4616a299f32fd))
* Punt deprecation warnings and clean up some warnings. ([#2670](https://github.com/feast-dev/feast/issues/2670)) ([f775d2e](https://github.com/feast-dev/feast/commit/f775d2e8a4a185e59031504e6e8eda8b330d066c))
* Reject undefined features when using `get_historical_features` or `get_online_features` ([#2665](https://github.com/feast-dev/feast/issues/2665)) ([36849fb](https://github.com/feast-dev/feast/commit/36849fb76716ba663d347369dfec3f3bbfcac3f8))
* Remove ci extra from the feature transformation server dockerfile ([#2618](https://github.com/feast-dev/feast/issues/2618)) ([25613b4](https://github.com/feast-dev/feast/commit/25613b414470e8559b9d7b1f9bb74e269fea08c8))
* Remove incorrect call to logging.basicConfig ([#2676](https://github.com/feast-dev/feast/issues/2676)) ([8cbf51c](https://github.com/feast-dev/feast/commit/8cbf51ca2ba3f672b6dc0e820ac18d4608220abf))
* Small typo in CLI ([#2578](https://github.com/feast-dev/feast/issues/2578)) ([f372981](https://github.com/feast-dev/feast/commit/f3729812d0666a10290d3a5614729845058f835d))
* Switch from `join_key` to `join_keys` in tests and docs ([#2580](https://github.com/feast-dev/feast/issues/2580)) ([d66c931](https://github.com/feast-dev/feast/commit/d66c931026823f77cbfd40bbe82035eb337e417d))
* Teardown trino container correctly after tests ([#2562](https://github.com/feast-dev/feast/issues/2562)) ([72f1558](https://github.com/feast-dev/feast/commit/72f155882c95f21573b31a613edf066bdb55f630))
* Update build_go_protos to use a consistent python path ([#2550](https://github.com/feast-dev/feast/issues/2550)) ([f136f8c](https://github.com/feast-dev/feast/commit/f136f8cc6c7feade73466aeb6267500377089485))
* Update data source timestamp inference error message to make sense ([#2636](https://github.com/feast-dev/feast/issues/2636)) ([3eaf6b7](https://github.com/feast-dev/feast/commit/3eaf6b79a420ced39f46ca25dddd14b2c69c8ae4))
* Update field api to add tag parameter corresponding to labels in Feature. ([#2610](https://github.com/feast-dev/feast/issues/2610)) ([689d20b](https://github.com/feast-dev/feast/commit/689d20bd0e4da91b9d24878d86787dede0c68f6d))
* Update java integration tests and add more logging ([#2637](https://github.com/feast-dev/feast/issues/2637)) ([10e23b4](https://github.com/feast-dev/feast/commit/10e23b437cd83fe62c8e10c0f577781322970947))
* Update on demand feature view api ([#2587](https://github.com/feast-dev/feast/issues/2587)) ([38cd7f9](https://github.com/feast-dev/feast/commit/38cd7f9ec3d4bf8d81f7f5d65110529b646ba09f))
* Update RedisCluster to use redis-py official implementation ([#2554](https://github.com/feast-dev/feast/issues/2554)) ([ce5606f](https://github.com/feast-dev/feast/commit/ce5606f335e11f6efa13d638072c71062e63420d))
* Use cwd when getting module path ([#2577](https://github.com/feast-dev/feast/issues/2577)) ([b550e59](https://github.com/feast-dev/feast/commit/b550e591f815aff53accdd064589ef06b6607d97))
* Use ParquetDataset for Schema Inference ([#2686](https://github.com/feast-dev/feast/issues/2686)) ([4f85e3e](https://github.com/feast-dev/feast/commit/4f85e3e6b3bb5ca92595aed28db68ada58abbb75))
* Use timestamp type when converting unixtimestamp feature type to arrow ([#2593](https://github.com/feast-dev/feast/issues/2593)) ([c439611](https://github.com/feast-dev/feast/commit/c4396118a99392a7b284b635b80486c94e0c86c8))


### Features

* Add hbase online store support in feast ([#2590](https://github.com/feast-dev/feast/issues/2590)) ([c9eda79](https://github.com/feast-dev/feast/commit/c9eda79c7b1169ef05a481a96f07960c014e88b9))
* Adding SSL options for Postgres ([#2644](https://github.com/feast-dev/feast/issues/2644)) ([0e809c2](https://github.com/feast-dev/feast/commit/0e809c2c86a8687397b54a9e073d6d2abcafd35f))
* Allow Feast UI to be spun up with CLI command: feast ui ([#2667](https://github.com/feast-dev/feast/issues/2667)) ([44ca9f5](https://github.com/feast-dev/feast/commit/44ca9f5f7eb495e23dd9a603b92ca6fd348e9ddb))
* Allow to pass secrets and environment variables to transformation service ([#2632](https://github.com/feast-dev/feast/issues/2632)) ([ffa33ad](https://github.com/feast-dev/feast/commit/ffa33ad9860e3220f8c2bb5374c1fb86f716a0c7))
* CLI command 'feast serve' should start go-based server if flag is enabled ([#2617](https://github.com/feast-dev/feast/issues/2617)) ([f3ff812](https://github.com/feast-dev/feast/commit/f3ff812a15d46287ed12918396167b6ad295cdc9))
* Create stream and batch feature view abstractions ([#2559](https://github.com/feast-dev/feast/issues/2559)) ([d1f76e5](https://github.com/feast-dev/feast/commit/d1f76e5aa5d2e2006296b3ca2cd9b08816ea0d9b))
* Postgres supported as Registry, Online store, and Offline store ([#2401](https://github.com/feast-dev/feast/issues/2401)) ([ed2f979](https://github.com/feast-dev/feast/commit/ed2f979fab0bb9ca4ea6f8e56cc55b0b9ad107a6))
* Support entity fields in feature view `schema` parameter by dropping them ([#2568](https://github.com/feast-dev/feast/issues/2568)) ([c8fcc35](https://github.com/feast-dev/feast/commit/c8fcc3504c7438961d0dc05869e3a2fca5b11f20))
* Write logged features to an offline store (Python API) ([#2574](https://github.com/feast-dev/feast/issues/2574)) ([134dc5f](https://github.com/feast-dev/feast/commit/134dc5f7e86e7c88df2b3b313c41eb8f89def6e2))
* Write logged features to Offline Store (Go - Python integration) ([#2621](https://github.com/feast-dev/feast/issues/2621)) ([ccad832](https://github.com/feast-dev/feast/commit/ccad832a4457f5e6cbc001cbadd91b43d98d282d))


### Reverts

* Revert "chore: Deprecate value type (#2611)" (#2643) ([4fbdfb1](https://github.com/feast-dev/feast/commit/4fbdfb1da90b83978c431ce140ce113bf8284b9a)), closes [#2611](https://github.com/feast-dev/feast/issues/2611) [#2643](https://github.com/feast-dev/feast/issues/2643)

# [0.20.0](https://github.com/feast-dev/feast/compare/v0.19.0...v0.20.0) (2022-04-14)


### Bug Fixes

* Add inlined data sources to the top level registry ([#2456](https://github.com/feast-dev/feast/issues/2456)) ([356788a](https://github.com/feast-dev/feast/commit/356788a4d3c3677f60512b3e841498245289d9a2))
* Add new value types to types.ts for web ui ([#2463](https://github.com/feast-dev/feast/issues/2463)) ([ad5694e](https://github.com/feast-dev/feast/commit/ad5694e601559ca1d8f8d3c91307c3c2cae1c69a))
* Add PushSource proto and Python class ([#2428](https://github.com/feast-dev/feast/issues/2428)) ([9a4bd63](https://github.com/feast-dev/feast/commit/9a4bd63f62b54b76148c069c7665a3067553c456))
* Add spark to lambda dockerfile ([#2480](https://github.com/feast-dev/feast/issues/2480)) ([514666f](https://github.com/feast-dev/feast/commit/514666f31cf6456dee59a5db750d8bd4ec63d8be))
* Added private_key auth for Snowflake ([#2508](https://github.com/feast-dev/feast/issues/2508)) ([c42c9b0](https://github.com/feast-dev/feast/commit/c42c9b0f2a962e428d9af0d3e7ba080a62d77008))
* Added Redshift and Spark typecheck to data_source event_timestamp_col inference ([#2389](https://github.com/feast-dev/feast/issues/2389)) ([04dea73](https://github.com/feast-dev/feast/commit/04dea732a495dfc8fa3dd006c89f73c6d08097c5))
* Building of go extension fails ([#2448](https://github.com/feast-dev/feast/issues/2448)) ([7d1efd5](https://github.com/feast-dev/feast/commit/7d1efd56387b4a8c373c69f062ea31214f33c5f1))
* Bump the number of versions bumps expected to 27 ([#2549](https://github.com/feast-dev/feast/issues/2549)) ([ecc9938](https://github.com/feast-dev/feast/commit/ecc9938774e9446f54484272bb3677e8d2fc1682))
* Create __init__ files for the proto-generated python dirs ([#2410](https://github.com/feast-dev/feast/issues/2410)) ([e17028d](https://github.com/feast-dev/feast/commit/e17028dbb90cb88d9df9a9769abbd90c5c301563))
* Don't prevent apply from running given duplicate empty names in data sources. Also fix repeated apply of Spark data source. ([#2415](https://github.com/feast-dev/feast/issues/2415)) ([b95f441](https://github.com/feast-dev/feast/commit/b95f4410ee91069ff84e81d2d5f3e9329edc8626))
* Dynamodb deduplicate batch write request by partition keys ([#2515](https://github.com/feast-dev/feast/issues/2515)) ([70d4a13](https://github.com/feast-dev/feast/commit/70d4a1335312747521fc57e7742abf7eb85c12a6))
* Ensure that __init__ files exist in proto dirs ([#2433](https://github.com/feast-dev/feast/issues/2433)) ([9b94f7b](https://github.com/feast-dev/feast/commit/9b94f7b35221af0bf1f813ab47e2f52e37a79ca6))
* Fix DataSource constructor to unbreak custom data sources ([#2492](https://github.com/feast-dev/feast/issues/2492)) ([712653e](https://github.com/feast-dev/feast/commit/712653e91cc4dc51174058a850f00a471c7269b9))
* Fix default feast apply path without any extras ([#2373](https://github.com/feast-dev/feast/issues/2373)) ([6ba7fc7](https://github.com/feast-dev/feast/commit/6ba7fc789da34f0d239d7cae1b3a95e14eae9e2f))
* Fix definitions.py with new definition ([#2541](https://github.com/feast-dev/feast/issues/2541)) ([eefc34a](https://github.com/feast-dev/feast/commit/eefc34a5da978ccc686bbc2586b4af5de3917ff1))
* Fix entity row to use join key instead of name ([#2521](https://github.com/feast-dev/feast/issues/2521)) ([c22fa2c](https://github.com/feast-dev/feast/commit/c22fa2c42c2c2b6b84c1876f12d690806ea8d431))
* Fix Java Master ([#2499](https://github.com/feast-dev/feast/issues/2499)) ([e083458](https://github.com/feast-dev/feast/commit/e0834587ec1450026a17a7c18f2708d44410da07))
* Fix registry proto ([#2435](https://github.com/feast-dev/feast/issues/2435)) ([ea6a9b2](https://github.com/feast-dev/feast/commit/ea6a9b2034c35bf36ee5073fad93dde52279ebcd))
* Fix some inconsistencies in the docs and comments in the code ([#2444](https://github.com/feast-dev/feast/issues/2444)) ([ad008bf](https://github.com/feast-dev/feast/commit/ad008bfd1d826a72247accf993a58505a9e2b308))
* Fix spark docs ([#2382](https://github.com/feast-dev/feast/issues/2382)) ([d4a606a](https://github.com/feast-dev/feast/commit/d4a606ad68fedeb12839038a956043a66e6f518f))
* Fix Spark template to work correctly on feast init -t spark ([#2393](https://github.com/feast-dev/feast/issues/2393)) ([ae133fd](https://github.com/feast-dev/feast/commit/ae133fd3ee55da3ff2fb2848456de527935ee94f))
* Fix the feature repo fixture used by java tests  ([#2469](https://github.com/feast-dev/feast/issues/2469)) ([32e925e](https://github.com/feast-dev/feast/commit/32e925e5edebef98449e295e2b1cf0f98ceb8d77))
* Fix unhashable Snowflake and Redshift sources ([cd8f1c9](https://github.com/feast-dev/feast/commit/cd8f1c9d3a07231343631692b6de67e7f5c93b8f))
* Fixed bug in passing config file params to snowflake python connector ([#2503](https://github.com/feast-dev/feast/issues/2503)) ([34f2b59](https://github.com/feast-dev/feast/commit/34f2b591bc8de3af59dfd0031ff803b4fe028abd))
* Fixing Spark template to include source name ([#2381](https://github.com/feast-dev/feast/issues/2381)) ([a985f1d](https://github.com/feast-dev/feast/commit/a985f1d310915bafba3074f9cea776232b956e29))
* Make name a keyword arg for the Entity class ([#2467](https://github.com/feast-dev/feast/issues/2467)) ([43847de](https://github.com/feast-dev/feast/commit/43847dea74b25bb2081d76b1fd5e43da8cbc0781))
* Making a name for data sources not a breaking change ([#2379](https://github.com/feast-dev/feast/issues/2379)) ([71d7ae2](https://github.com/feast-dev/feast/commit/71d7ae26f0357646a15a41f7c2b53a0cab1b7c12))
* Minor link fix in `CONTRIBUTING.md` ([#2481](https://github.com/feast-dev/feast/issues/2481)) ([2917e27](https://github.com/feast-dev/feast/commit/2917e272ab538955d564552a56ef36bb3f75f646))
* Preserve ordering of features in _get_column_names ([#2457](https://github.com/feast-dev/feast/issues/2457)) ([495b435](https://github.com/feast-dev/feast/commit/495b4353051d9d48886407f2ca7de19c3eb458d7))
* Relax click python requirement to >=7 ([#2450](https://github.com/feast-dev/feast/issues/2450)) ([f202f92](https://github.com/feast-dev/feast/commit/f202f928ff02f4a263f3b4ce450051e526304c51))
* Remove date partition column field from datasources that don't s… ([#2478](https://github.com/feast-dev/feast/issues/2478)) ([ce35835](https://github.com/feast-dev/feast/commit/ce35835413d4f5bc174eb177e277be96e8bb9673))
* Remove docker step from unit test workflow ([#2535](https://github.com/feast-dev/feast/issues/2535)) ([6f22f22](https://github.com/feast-dev/feast/commit/6f22f227a22bea6f9ef171764a4c5e3d74b310ea))
* Remove spark from the AWS Lambda dockerfile ([#2498](https://github.com/feast-dev/feast/issues/2498)) ([6abae16](https://github.com/feast-dev/feast/commit/6abae16803ef4644c91822626820e99b2f7cfbbd))
* Request data api update ([#2488](https://github.com/feast-dev/feast/issues/2488)) ([0c9e5b7](https://github.com/feast-dev/feast/commit/0c9e5b7e2132b619056e9b41519d54a93e977f6c))
* Schema update ([#2509](https://github.com/feast-dev/feast/issues/2509)) ([cf7bbc2](https://github.com/feast-dev/feast/commit/cf7bbc2226e2c3d652df1ddfc411a1feadda26c4))
* Simplify DataSource.from_proto logic ([#2424](https://github.com/feast-dev/feast/issues/2424)) ([6bda4d2](https://github.com/feast-dev/feast/commit/6bda4d2b2080e5a97333e14863f2235aed989661))
* Snowflake api update ([#2487](https://github.com/feast-dev/feast/issues/2487)) ([1181a9e](https://github.com/feast-dev/feast/commit/1181a9e62335ee00ee54cd1bc88baa6297238988))
* Support passing batch source to streaming sources for backfills ([#2523](https://github.com/feast-dev/feast/issues/2523)) ([90db1d1](https://github.com/feast-dev/feast/commit/90db1d1bb079a76a6a26a89a9c70b9f80bb8b08a))
* Timestamp update ([#2486](https://github.com/feast-dev/feast/issues/2486)) ([bf23111](https://github.com/feast-dev/feast/commit/bf23111acedf0b5180e368e1cffbdeb0da5a8f30))
* Typos in Feast UI error message ([#2432](https://github.com/feast-dev/feast/issues/2432)) ([e14369d](https://github.com/feast-dev/feast/commit/e14369def00c116e05c91475c29fa32ab2fa8aeb))
* Update feature view APIs to prefer keyword args ([#2472](https://github.com/feast-dev/feast/issues/2472)) ([7c19cf7](https://github.com/feast-dev/feast/commit/7c19cf7fe3e42674a357e54a6576c772a7bcb107))
* Update file api ([#2470](https://github.com/feast-dev/feast/issues/2470)) ([83a11c6](https://github.com/feast-dev/feast/commit/83a11c6b3af3b78653fe612989a5f5227cb773c1))
* Update Makefile to cd into python dir before running commands ([#2437](https://github.com/feast-dev/feast/issues/2437)) ([ca32155](https://github.com/feast-dev/feast/commit/ca3215502bc4c92d0635efaceae22acc914c73a8))
* Update redshift api ([#2479](https://github.com/feast-dev/feast/issues/2479)) ([4fa73a9](https://github.com/feast-dev/feast/commit/4fa73a9c1fc5f003a5662e497d77e69efdbae6c1))
* Update some fields optional in UI parser ([#2380](https://github.com/feast-dev/feast/issues/2380)) ([cff7ac3](https://github.com/feast-dev/feast/commit/cff7ac3c6440a78a7fa460b2f19bc07d9ea5aab2))
* Use a single version of jackson libraries and upgrade to 2.12.6.1 ([#2473](https://github.com/feast-dev/feast/issues/2473)) ([5be1cc6](https://github.com/feast-dev/feast/commit/5be1cc6768c123b592c26099d12f3469f575a52c))
* Use dateutil parser to parse materialization times ([#2464](https://github.com/feast-dev/feast/issues/2464)) ([6c55e49](https://github.com/feast-dev/feast/commit/6c55e49898e9b5f5aa77fe87ac0807984937a708))
* Use the correct dockerhub image tag when building feature servers ([#2372](https://github.com/feast-dev/feast/issues/2372)) ([0d62c1d](https://github.com/feast-dev/feast/commit/0d62c1d34b02cd67e13e545d081b90fe1562261e))


### Features

* Add `/write-to-online-store` method to the python feature server ([#2423](https://github.com/feast-dev/feast/issues/2423)) ([d2fb048](https://github.com/feast-dev/feast/commit/d2fb0487134f011c1a0f217f1a325e3bf6a52d27))
* Add description, tags, owner fields to all feature view classes ([#2440](https://github.com/feast-dev/feast/issues/2440)) ([ed5e928](https://github.com/feast-dev/feast/commit/ed5e9282678c943c750c5e9d84037376dd1380d3))
* Add DQM Logging on GRPC Server with FileLogStorage for Testing ([#2403](https://github.com/feast-dev/feast/issues/2403)) ([57a97d8](https://github.com/feast-dev/feast/commit/57a97d8e207e38876901b9a6b1d6e0f7cc6ce43d))
* Add Feast types in preparation for changing type system ([#2475](https://github.com/feast-dev/feast/issues/2475)) ([4864252](https://github.com/feast-dev/feast/commit/4864252db4e8ef6effb596c689bd17ec884ebffe))
* Add Field class ([#2500](https://github.com/feast-dev/feast/issues/2500)) ([1279612](https://github.com/feast-dev/feast/commit/1279612fe64d8d24fa57d873f4a617095adc5feb))
* Add support for DynamoDB online_read in batches ([#2371](https://github.com/feast-dev/feast/issues/2371)) ([702ec49](https://github.com/feast-dev/feast/commit/702ec498eff2fb079d5403e9471b73ae840d6d7f))
* Add Support for DynamodbOnlineStoreConfig endpoint_url parameter ([#2485](https://github.com/feast-dev/feast/issues/2485)) ([7b863d1](https://github.com/feast-dev/feast/commit/7b863d14e8442007b1403e467130b974b3aba078))
* Add templating for dynamodb table name ([#2394](https://github.com/feast-dev/feast/issues/2394)) ([f591088](https://github.com/feast-dev/feast/commit/f59108806c6819bcd42828244a2abc66471cd8f8))
* Allow local feature server to use Go feature server if enabled ([#2538](https://github.com/feast-dev/feast/issues/2538)) ([a2ef375](https://github.com/feast-dev/feast/commit/a2ef3755766a7224298f984a2cadb17829b2f3d2))
* Allow using entity's join_key in get_online_features ([#2420](https://github.com/feast-dev/feast/issues/2420)) ([068c765](https://github.com/feast-dev/feast/commit/068c765ccf5f984bc0a73a8354711f39a5c17da7))
* Data Source Api Update ([#2468](https://github.com/feast-dev/feast/issues/2468)) ([6b96b21](https://github.com/feast-dev/feast/commit/6b96b21a32cc6dc3b44b48289711a66b2c33512d))
* Go server ([#2339](https://github.com/feast-dev/feast/issues/2339)) ([d12e7ef](https://github.com/feast-dev/feast/commit/d12e7ef3b9c79cae30a2401c9ae6c0ac783c4b6b)), closes [#2354](https://github.com/feast-dev/feast/issues/2354) [#2361](https://github.com/feast-dev/feast/issues/2361) [#2332](https://github.com/feast-dev/feast/issues/2332) [#2356](https://github.com/feast-dev/feast/issues/2356) [#2363](https://github.com/feast-dev/feast/issues/2363) [#2349](https://github.com/feast-dev/feast/issues/2349) [#2355](https://github.com/feast-dev/feast/issues/2355) [#2336](https://github.com/feast-dev/feast/issues/2336) [#2361](https://github.com/feast-dev/feast/issues/2361) [#2363](https://github.com/feast-dev/feast/issues/2363) [#2344](https://github.com/feast-dev/feast/issues/2344) [#2354](https://github.com/feast-dev/feast/issues/2354) [#2347](https://github.com/feast-dev/feast/issues/2347) [#2350](https://github.com/feast-dev/feast/issues/2350) [#2356](https://github.com/feast-dev/feast/issues/2356) [#2355](https://github.com/feast-dev/feast/issues/2355) [#2349](https://github.com/feast-dev/feast/issues/2349) [#2352](https://github.com/feast-dev/feast/issues/2352) [#2341](https://github.com/feast-dev/feast/issues/2341) [#2336](https://github.com/feast-dev/feast/issues/2336) [#2373](https://github.com/feast-dev/feast/issues/2373) [#2315](https://github.com/feast-dev/feast/issues/2315) [#2372](https://github.com/feast-dev/feast/issues/2372) [#2332](https://github.com/feast-dev/feast/issues/2332) [#2349](https://github.com/feast-dev/feast/issues/2349) [#2336](https://github.com/feast-dev/feast/issues/2336) [#2361](https://github.com/feast-dev/feast/issues/2361) [#2363](https://github.com/feast-dev/feast/issues/2363) [#2344](https://github.com/feast-dev/feast/issues/2344) [#2354](https://github.com/feast-dev/feast/issues/2354) [#2347](https://github.com/feast-dev/feast/issues/2347) [#2350](https://github.com/feast-dev/feast/issues/2350) [#2356](https://github.com/feast-dev/feast/issues/2356) [#2355](https://github.com/feast-dev/feast/issues/2355) [#2349](https://github.com/feast-dev/feast/issues/2349) [#2352](https://github.com/feast-dev/feast/issues/2352) [#2341](https://github.com/feast-dev/feast/issues/2341) [#2336](https://github.com/feast-dev/feast/issues/2336) [#2373](https://github.com/feast-dev/feast/issues/2373) [#2379](https://github.com/feast-dev/feast/issues/2379) [#2380](https://github.com/feast-dev/feast/issues/2380) [#2382](https://github.com/feast-dev/feast/issues/2382) [#2364](https://github.com/feast-dev/feast/issues/2364) [#2366](https://github.com/feast-dev/feast/issues/2366) [#2386](https://github.com/feast-dev/feast/issues/2386)
* Graduate write_to_online_store out of experimental status ([#2426](https://github.com/feast-dev/feast/issues/2426)) ([e7dd4b7](https://github.com/feast-dev/feast/commit/e7dd4b75ba0fbd86338aacf2ecd0cc8979dc803b))
* Make feast PEP 561 compliant ([#2405](https://github.com/feast-dev/feast/issues/2405)) ([3c41f94](https://github.com/feast-dev/feast/commit/3c41f944c68fb6687389c0b154f7297941a1f398)), closes [#2420](https://github.com/feast-dev/feast/issues/2420) [#2418](https://github.com/feast-dev/feast/issues/2418) [#2425](https://github.com/feast-dev/feast/issues/2425) [#2426](https://github.com/feast-dev/feast/issues/2426) [#2427](https://github.com/feast-dev/feast/issues/2427) [#2431](https://github.com/feast-dev/feast/issues/2431) [#2433](https://github.com/feast-dev/feast/issues/2433) [#2420](https://github.com/feast-dev/feast/issues/2420) [#2418](https://github.com/feast-dev/feast/issues/2418) [#2425](https://github.com/feast-dev/feast/issues/2425) [#2426](https://github.com/feast-dev/feast/issues/2426) [#2427](https://github.com/feast-dev/feast/issues/2427) [#2431](https://github.com/feast-dev/feast/issues/2431) [#2433](https://github.com/feast-dev/feast/issues/2433)
* Makefile for contrib for Issue [#2364](https://github.com/feast-dev/feast/issues/2364) ([#2366](https://github.com/feast-dev/feast/issues/2366)) ([a02325b](https://github.com/feast-dev/feast/commit/a02325b20f4d1a949ebb1f26bd3b65a22e3ea4f3))
* Support on demand feature views in go feature server ([#2494](https://github.com/feast-dev/feast/issues/2494)) ([6edd274](https://github.com/feast-dev/feast/commit/6edd274261689b8c67df31d598aa5d5b14dcf5f7))
* Switch from Feature to Field ([#2514](https://github.com/feast-dev/feast/issues/2514)) ([6a03bed](https://github.com/feast-dev/feast/commit/6a03bed82bf408d2f46d209be7ac9524b4ea6dcd))
* Use a daemon thread to monitor the go feature server exclusively ([#2391](https://github.com/feast-dev/feast/issues/2391)) ([0bb5e8c](https://github.com/feast-dev/feast/commit/0bb5e8c5a91f6f986f879f965b84e987e71a9d88))

# [0.19.0](https://github.com/feast-dev/feast/compare/v0.18.0...v0.19.0) (2022-03-05)


### Bug Fixes

* Added additional value types to UI parser and removed references to registry-bq.json ([#2361](https://github.com/feast-dev/feast/issues/2361)) ([d202d51](https://github.com/feast-dev/feast/commit/d202d5170b7e6bf1e1b0f103aac247bfc04c2760))
* Fix Redshift bug that stops waiting on statements after 5 minutes ([#2363](https://github.com/feast-dev/feast/issues/2363)) ([74f887f](https://github.com/feast-dev/feast/commit/74f887f72e109147a2e6214c2e3baade63d55fbe))
* Method _should_use_plan only returns true for local sqlite provider ([#2344](https://github.com/feast-dev/feast/issues/2344)) ([fdb5f21](https://github.com/feast-dev/feast/commit/fdb5f215fe7f4946a6c88cfb34dc0c24eb5ac733))
* Remove redis service to prevent more conflicts and add redis node to master_only ([#2354](https://github.com/feast-dev/feast/issues/2354)) ([993616f](https://github.com/feast-dev/feast/commit/993616fe152d097e2d918e5f56fc13e2e409175c))
* Rollback Redis-py to Redis-py-cluster ([#2347](https://github.com/feast-dev/feast/issues/2347)) ([1ba86fb](https://github.com/feast-dev/feast/commit/1ba86fb0cc7f2e86b8c70477462faa68075f99cd))
* Update github workflow to prevent redis from overlapping ports. ([#2350](https://github.com/feast-dev/feast/issues/2350)) ([c2a6c6c](https://github.com/feast-dev/feast/commit/c2a6c6cc34d64fd3483f0b2cef394f833e9124e2))


### Features

* Add owner field to Entity and rename labels to tags ([412d625](https://github.com/feast-dev/feast/commit/412d62516da184350de473c87dd2ab369ad4cf86))
* Allow all snowflake python connector connection methods to be available to Feast ([#2356](https://github.com/feast-dev/feast/issues/2356)) ([ec7385c](https://github.com/feast-dev/feast/commit/ec7385c16f6899c5388b7a786fce6e0d543776f5))
* Allowing password based authentication and SSL for Redis in Java feature server ([0af8adb](https://github.com/feast-dev/feast/commit/0af8adba1c56a2a29d02c2fc5aeccf51ee78fd5f))
* Event timestamps response ([#2355](https://github.com/feast-dev/feast/issues/2355)) ([5481caf](https://github.com/feast-dev/feast/commit/5481caf37989c347bf4469f5f081f4f15f20fdb7))
* Feast Spark Offline Store ([#2349](https://github.com/feast-dev/feast/issues/2349)) ([98b8d8d](https://github.com/feast-dev/feast/commit/98b8d8da7e8e717ec93197f253368551951ca675))
* Initial merge of Web UI logic ([#2352](https://github.com/feast-dev/feast/issues/2352)) ([ce3bc59](https://github.com/feast-dev/feast/commit/ce3bc598501949f82197c7fa2409319282197276))
* Key ttl setting for redis online store ([#2341](https://github.com/feast-dev/feast/issues/2341)) ([236a108](https://github.com/feast-dev/feast/commit/236a108c87aed106e0a46e48172d31dc94ed9c2b))
* Metadata changes & making data sources top level objects to power Feast UI ([#2336](https://github.com/feast-dev/feast/issues/2336)) ([43da230](https://github.com/feast-dev/feast/commit/43da2302dfcbf3b5e56ed068021b5821d544c05f))

# [v0.18.1](https://github.com/feast-dev/feast/tree/v0.18.1) (2022-02-15)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.18.0...v0.18.1)

### Bug Fixes

- ODFVs raise a PerformanceWarning for very large sets of features [\#2293](https://github.com/feast-dev/feast/issues/2293)
- Don't require `snowflake` to always be installed [\#2309](https://github.com/feast-dev/feast/pull/2309) ([judahrand](https://github.com/judahrand))
- podAnnotations Values in the feature-server chart [\#2304](https://github.com/feast-dev/feast/pull/2304) ([tpvasconcelos](https://github.com/tpvasconcelos))
- Fixing the Java helm charts and adding a demo tutorial on how to use them [\#2298](https://github.com/feast-dev/feast/pull/2298) ([adchia](https://github.com/adchia))
- avoid using transactions on OSS Redis [\#2296](https://github.com/feast-dev/feast/pull/2296) ([DvirDukhan](https://github.com/DvirDukhan))
- Include infra objects in registry dump and fix Infra's from\_proto  [\#2295](https://github.com/feast-dev/feast/pull/2295) ([adchia](https://github.com/adchia))
- Expose snowflake credentials  for unit testing [\#2288](https://github.com/feast-dev/feast/pull/2288) ([sfc-gh-madkins](https://github.com/sfc-gh-madkins))
- Fix flaky tests \(test\_online\_store\_cleanup & test\_feature\_get\_online\_features\_types\_match\) [\#2276](https://github.com/feast-dev/feast/pull/2276) ([pyalex](https://github.com/pyalex))

### Merged Pull Requests

- Remove old flag warning with the python feature server [\#2300](https://github.com/feast-dev/feast/pull/2300) ([adchia](https://github.com/adchia))
- Use an OFFLINE schema for Snowflake offline store tests [\#2291](https://github.com/feast-dev/feast/pull/2291) ([sfc-gh-madkins](https://github.com/sfc-gh-madkins))
- fix typos in markdown files [\#2289](https://github.com/feast-dev/feast/pull/2289) ([charliec443](https://github.com/charliec443))
- Add -SNAPSHOT suffix to pom.xml version [\#2286](https://github.com/feast-dev/feast/pull/2286) ([tsotnet](https://github.com/tsotnet))
- Update CONTRIBUTING.md [\#2282](https://github.com/feast-dev/feast/pull/2282) ([adchia](https://github.com/adchia))

# [v0.18.0](https://github.com/feast-dev/feast/tree/v0.18.0) (2022-02-05)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.17.0...v0.18.0)

### Features

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

### Bug Fixes

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

### Merged Pull Requests

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


# [v0.17.0](https://github.com/feast-dev/feast/tree/v0.17.0) (2021-12-31)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.16.1...v0.17.0)

### Features

- Add feast-python-server helm chart [\#2177](https://github.com/feast-dev/feast/pull/2177) ([michelle-rascati-sp](https://github.com/michelle-rascati-sp))
- Add SqliteTable as an InfraObject [\#2157](https://github.com/feast-dev/feast/pull/2157) ([felixwang9817](https://github.com/felixwang9817))
- Compute property-level diffs for repo objects [\#2156](https://github.com/feast-dev/feast/pull/2156) ([achals](https://github.com/achals))
- Add a feast plan command, and have CLI output differentiates between created, deleted and unchanged objects [\#2147](https://github.com/feast-dev/feast/pull/2147) ([achals](https://github.com/achals))
- Refactor tag methods to infer created, deleted, and kept repo objects  [\#2142](https://github.com/feast-dev/feast/pull/2142) ([achals](https://github.com/achals))
- Add DatastoreTable infra object [\#2140](https://github.com/feast-dev/feast/pull/2140) ([felixwang9817](https://github.com/felixwang9817))
- Dynamodb infra object [\#2131](https://github.com/feast-dev/feast/pull/2131) ([felixwang9817](https://github.com/felixwang9817))
- Add Infra and InfraObjects classes [\#2125](https://github.com/feast-dev/feast/pull/2125) ([felixwang9817](https://github.com/felixwang9817))
- Pre compute the timestamp range for feature views [\#2103](https://github.com/feast-dev/feast/pull/2103) ([judahrand](https://github.com/judahrand))

### Bug Fixes

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

### Closes Issues

- In GH workflow docker images are being built but not published [\#2152](https://github.com/feast-dev/feast/issues/2152)
- Any plan to make Feast 0.10+ support docker [\#2148](https://github.com/feast-dev/feast/issues/2148)
- ODFVs don't respect `full_feature_names` [\#2143](https://github.com/feast-dev/feast/issues/2143)
- Release workflow does not work [\#2136](https://github.com/feast-dev/feast/issues/2136)
- Redis Online Store - Truncate and Load [\#2129](https://github.com/feast-dev/feast/issues/2129)

### Merged Pull Requests

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
# [v0.16.1](https://github.com/feast-dev/feast/tree/v0.16.1) (2021-12-10)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.16.0...v0.16.1)

### Bug Fixes

- Bump log4j-api from 2.13.2 to 2.15.0 in /java [\#2133](https://github.com/feast-dev/feast/pull/2133) ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix release workflow to use the new GCP action [\#2132](https://github.com/feast-dev/feast/pull/2132) ([adchia](https://github.com/adchia))
- Fix Makefile to properly create the ECR\_VERSION [\#2123](https://github.com/feast-dev/feast/pull/2123) ([adchia](https://github.com/adchia))

### Merged Pull Requests

- Updating lambda docker image to feature-server-python-aws [\#2130](https://github.com/feast-dev/feast/pull/2130) ([adchia](https://github.com/adchia))
- Fix README to reflect new integration test suites [\#2124](https://github.com/feast-dev/feast/pull/2124) ([adchia](https://github.com/adchia))
- Remove  argument `feature_refs` [\#2115](https://github.com/feast-dev/feast/pull/2115) ([judahrand](https://github.com/judahrand))

# [v0.16.0](https://github.com/feast-dev/feast/tree/v0.16.0) (2021-12-08)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.15.1...v0.16.0)

### Features

- Install redis extra in AWS Lambda feature server & add hiredis depend… [\#2057](https://github.com/feast-dev/feast/pull/2057) ([tsotnet](https://github.com/tsotnet))
- Support of GC and S3 storages for registry in Java Feature Server [\#2043](https://github.com/feast-dev/feast/pull/2043) ([pyalex](https://github.com/pyalex))
- Adding stream ingestion alpha documentation [\#2005](https://github.com/feast-dev/feast/pull/2005) ([adchia](https://github.com/adchia))

### Bug Fixes

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

### Merged Pull Requests

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

# [v0.15.1](https://github.com/feast-dev/feast/tree/v0.15.1) (2021-11-13)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.15.0...v0.15.1)

### Bug Fixes

- Add missing comma in setup.py [\#2031](https://github.com/feast-dev/feast/pull/2031) ([achals](https://github.com/achals))
- Correct cleanup after usage e2e tests [\#2015](https://github.com/feast-dev/feast/pull/2015) ([pyalex](https://github.com/pyalex))
- Change Environment timestamps to be in UTC [\#2007](https://github.com/feast-dev/feast/pull/2007) ([felixwang9817](https://github.com/felixwang9817))
- ODFV UDFs should handle list types [\#2002](https://github.com/feast-dev/feast/pull/2002) ([Agent007](https://github.com/Agent007))

### Merged Pull Requests

- Remove unsupported java parts [\#2029](https://github.com/feast-dev/feast/pull/2029) ([pyalex](https://github.com/pyalex))
- Fix checked out branch for PR docker image build workflow [\#2018](https://github.com/feast-dev/feast/pull/2018) ([tsotnet](https://github.com/tsotnet))
- Remove duplicates in setup.py and run rudimentary verifications [\#2016](https://github.com/feast-dev/feast/pull/2016) ([achals](https://github.com/achals))
- Upload feature server docker image to ECR on approved PRs [\#2014](https://github.com/feast-dev/feast/pull/2014) ([tsotnet](https://github.com/tsotnet))
- Add integration tests for AWS Lambda feature server [\#2001](https://github.com/feast-dev/feast/pull/2001) ([tsotnet](https://github.com/tsotnet))
- Moving Feast Java back into main repo under java/ package [\#1997](https://github.com/feast-dev/feast/pull/1997) ([adchia](https://github.com/adchia))

# [v0.15.0](https://github.com/feast-dev/feast/tree/v0.15.0) (2021-11-08)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.14.1...v0.15.0)

### Features

- Adding stream ingestion alpha documentation [\#2005](https://github.com/feast-dev/feast/pull/2005) ([adchia](https://github.com/adchia))
- Feature transformation server docker image [\#1972](https://github.com/feast-dev/feast/pull/1972) ([felixwang9817](https://github.com/felixwang9817))
- eventtime check before writing features, use pipelines, ttl [\#1961](https://github.com/feast-dev/feast/pull/1961) ([vas28r13](https://github.com/vas28r13))
- Plugin repo universal tests [\#1946](https://github.com/feast-dev/feast/pull/1946) ([felixwang9817](https://github.com/felixwang9817))
- direct data ingestion into Online store [\#1939](https://github.com/feast-dev/feast/pull/1939) ([vas28r13](https://github.com/vas28r13))
- Add an interface for TransformationService and a basic implementation [\#1932](https://github.com/feast-dev/feast/pull/1932) ([achals](https://github.com/achals))
- Allows registering of features in request data as RequestFeatureView. Refactors common logic into a BaseFeatureView class [\#1931](https://github.com/feast-dev/feast/pull/1931) ([adchia](https://github.com/adchia))
- Add final\_output\_feature\_names in Query context to avoid SELECT \* EXCEPT [\#1911](https://github.com/feast-dev/feast/pull/1911) ([MattDelac](https://github.com/MattDelac))
- Add Dockerfile for GCP CloudRun FeatureServer [\#1887](https://github.com/feast-dev/feast/pull/1887) ([judahrand](https://github.com/judahrand))

### Bug Fixes

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

### Merged Pull Requests

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

# [v0.14.1](https://github.com/feast-dev/feast/tree/v0.14.1) (2021-10-28)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.14.0...v0.14.1)

### Bug Fixes

- Fix duplicate upload entity [\#1981](https://github.com/feast-dev/feast/pull/1981) ([achals](https://github.com/achals))
- Fix bug in feast alpha enable CLI command [\#1940](https://github.com/feast-dev/feast/pull/1940) ([felixwang9817](https://github.com/felixwang9817))
- Fix conditional statements for if OnDemandFVs exist [\#1937](https://github.com/feast-dev/feast/pull/1937) ([codyjlin](https://github.com/codyjlin))
- Fix \_\_getitem\_\_ return value for feature view and on-demand feature view [\#1936](https://github.com/feast-dev/feast/pull/1936) ([mavysavydav](https://github.com/mavysavydav))
- Corrected setup.py BigQuery version that's needed for a contributor's merged PR 1844 [\#1934](https://github.com/feast-dev/feast/pull/1934) ([mavysavydav](https://github.com/mavysavydav))

### Merged Pull Requests

- Updating roadmap + hero image [\#1950](https://github.com/feast-dev/feast/pull/1950) ([adchia](https://github.com/adchia))
- Simplify BigQuery load jobs [\#1935](https://github.com/feast-dev/feast/pull/1935) ([judahrand](https://github.com/judahrand))

# [v0.14.0](https://github.com/feast-dev/feast/tree/v0.14.0) (2021-10-08)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.13.0...v0.14.0)

### Features

- Changed FVProjection 'name\_to\_use' field to 'name\_alias' and changed '.set\_projection' in FeatureView to ".with\_projection". Also adjustments for some edge cases [\#1929](https://github.com/feast-dev/feast/pull/1929) ([mavysavydav](https://github.com/mavysavydav))
- Make serverless alpha feature [\#1928](https://github.com/feast-dev/feast/pull/1928) ([felixwang9817](https://github.com/felixwang9817))
- Feast endpoint [\#1927](https://github.com/feast-dev/feast/pull/1927) ([felixwang9817](https://github.com/felixwang9817))
- Add location to BigQueryOfflineStoreConfig [\#1921](https://github.com/feast-dev/feast/pull/1921) ([loftiskg](https://github.com/loftiskg))
- Create & teardown Lambda & API Gateway resources for serverless feature server [\#1900](https://github.com/feast-dev/feast/pull/1900) ([tsotnet](https://github.com/tsotnet))
- Hide FeatureViewProjections from user interface & have FeatureViews carry FVProjections that carries the modified info of the FeatureView [\#1899](https://github.com/feast-dev/feast/pull/1899) ([mavysavydav](https://github.com/mavysavydav))
- Upload docker image to ECR during feast apply [\#1877](https://github.com/feast-dev/feast/pull/1877) ([felixwang9817](https://github.com/felixwang9817))
- Added .with\_name method in FeatureView/OnDemandFeatureView classes for name aliasing. FeatureViewProjection will hold this information [\#1872](https://github.com/feast-dev/feast/pull/1872) ([mavysavydav](https://github.com/mavysavydav))

### Bug Fixes

- Update makefile to use pip installed dependencies [\#1920](https://github.com/feast-dev/feast/pull/1920) ([loftiskg](https://github.com/loftiskg))
- Delete tables [\#1916](https://github.com/feast-dev/feast/pull/1916) ([felixwang9817](https://github.com/felixwang9817))
- Set a 5 minute limit for redshift statement execution [\#1915](https://github.com/feast-dev/feast/pull/1915) ([achals](https://github.com/achals))
- Use set when parsing repos to prevent duplicates [\#1913](https://github.com/feast-dev/feast/pull/1913) ([achals](https://github.com/achals))
- resolve environment variables in repo config [\#1909](https://github.com/feast-dev/feast/pull/1909) ([samuel100](https://github.com/samuel100))
- Respect specified ValueTypes for features during materialization [\#1906](https://github.com/feast-dev/feast/pull/1906) ([Agent007](https://github.com/Agent007))
- Fix issue with feature views being detected as duplicated when imported [\#1905](https://github.com/feast-dev/feast/pull/1905) ([achals](https://github.com/achals))
- Use contextvars to maintain a call stack during the usage calls [\#1882](https://github.com/feast-dev/feast/pull/1882) ([achals](https://github.com/achals))

### Merged Pull Requests

- Update concepts/README.md [\#1926](https://github.com/feast-dev/feast/pull/1926) ([ysk24ok](https://github.com/ysk24ok))
- Add CI for feature server Docker image [\#1925](https://github.com/feast-dev/feast/pull/1925) ([felixwang9817](https://github.com/felixwang9817))
- cache provider in feature store instance [\#1924](https://github.com/feast-dev/feast/pull/1924) ([DvirDukhan](https://github.com/DvirDukhan))
- Refactor logging and error messages in serverless [\#1923](https://github.com/feast-dev/feast/pull/1923) ([felixwang9817](https://github.com/felixwang9817))
- Add a caching step to our github actions [\#1919](https://github.com/feast-dev/feast/pull/1919) ([achals](https://github.com/achals))
- Add provider, offline store, online store, registry to RTD [\#1918](https://github.com/feast-dev/feast/pull/1918) ([felixwang9817](https://github.com/felixwang9817))
- Cleanup tests [\#1901](https://github.com/feast-dev/feast/pull/1901) ([felixwang9817](https://github.com/felixwang9817))

# [v0.13.0](https://github.com/feast-dev/feast/tree/v0.13.0) (2021-09-22)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.12.1...v0.13.0)

### Breaking Changes

- Enforce case-insensitively unique feature view names [\#1835](https://github.com/feast-dev/feast/pull/1835) ([codyjlin](https://github.com/codyjlin))
- Add init to Provider contract [\#1796](https://github.com/feast-dev/feast/pull/1796) ([woop](https://github.com/woop))

### Features

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

### Bug Fixes

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

### Merged Pull Requests

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

# [v0.12.1](https://github.com/feast-dev/feast/tree/v0.12.1) (2021-08-20)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.12.0...v0.12.1)

### Bug Fixes

- Fix get\_online\_features telemetry to only log every 10000 times [\#1786](https://github.com/feast-dev/feast/pull/1786) ([felixwang9817](https://github.com/felixwang9817))

# [v0.12.0](https://github.com/feast-dev/feast/tree/v0.12.0) (2021-08-05)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.11.0...v0.12.0)

### Breaking Changes

- Set default feature naming to not include feature view name. Add option to include feature view name in feature naming. [\#1641](https://github.com/feast-dev/feast/pull/1641) ([Mwad22](https://github.com/Mwad22))

### Features

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

### Bug Fixes

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

### Merged Pull Requests

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

# [v0.11.0](https://github.com/feast-dev/feast/tree/v0.11.0) (2021-06-24)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.8...v0.11.0)

### Features

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

### Bug Fixes

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

### Merged Pull Requests

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


# [v0.10.8](https://github.com/feast-dev/feast/tree/v0.10.8) (2021-06-17)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.7...v0.10.8)

### Features

- Add `to_bigquery()` function to BigQueryRetrievalJob [\#1634](https://github.com/feast-dev/feast/pull/1634) ([vtao2](https://github.com/vtao2))

### Bug Fixes

- Don't use .result\(\) in BigQueryOfflineStore, since it still leads to OOM [\#1642](https://github.com/feast-dev/feast/pull/1642) ([tsotnet](https://github.com/tsotnet))
- Don't load entire bigquery query results in memory [\#1638](https://github.com/feast-dev/feast/pull/1638) ([tsotnet](https://github.com/tsotnet))
- Add entity column validations when getting historical features from bigquery [\#1614](https://github.com/feast-dev/feast/pull/1614) ([achals](https://github.com/achals))

### Merged Pull Requests

- Make test historical retrieval longer [\#1630](https://github.com/feast-dev/feast/pull/1630) ([MattDelac](https://github.com/MattDelac))
- Fix failing historical retrieval assertion [\#1622](https://github.com/feast-dev/feast/pull/1622) ([woop](https://github.com/woop))
- Optimize historical retrieval with BigQuery offline store [\#1602](https://github.com/feast-dev/feast/pull/1602) ([MattDelac](https://github.com/MattDelac))

# [v0.10.7](https://github.com/feast-dev/feast/tree/v0.10.7) (2021-06-07)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.6...v0.10.7)

### Bug Fixes

- Fix race condition in historical e2e tests [\#1620](https://github.com/feast-dev/feast/pull/1620) ([woop](https://github.com/woop))

### Merged Pull Requests

- Use drop\_duplicates\(\) instead of groupby \(about 1.5~2x faster\) [\#1617](https://github.com/feast-dev/feast/pull/1617) ([rightx2](https://github.com/rightx2))
- Use CONCAT\(\) instead of ROW\_NUMBER\(\) [\#1601](https://github.com/feast-dev/feast/pull/1601) ([MattDelac](https://github.com/MattDelac))
- Minor doc fix in the code snippet: Fix to reference the right instance for the retrieved job instance object [\#1599](https://github.com/feast-dev/feast/pull/1599) ([dmatrix](https://github.com/dmatrix))
- Append nanoseconds to dataset name in test\_historical\_retrival to prevent tests stomping over each other [\#1593](https://github.com/feast-dev/feast/pull/1593) ([achals](https://github.com/achals))
- Make start and end timestamps tz aware in the CLI [\#1590](https://github.com/feast-dev/feast/pull/1590) ([achals](https://github.com/achals))

# [v0.10.6](https://github.com/feast-dev/feast/tree/v0.10.6) (2021-05-27)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.5...v0.10.6)

### Features

- Add datastore namespace option in configs [\#1581](https://github.com/feast-dev/feast/pull/1581) ([tsotnet](https://github.com/tsotnet))

### Bug Fixes

- Fix contention issue [\#1582](https://github.com/feast-dev/feast/pull/1582) ([woop](https://github.com/woop))
- Ensure that only None types fail predicate [\#1580](https://github.com/feast-dev/feast/pull/1580) ([woop](https://github.com/woop))
- Don't create bigquery dataset if it already exists [\#1569](https://github.com/feast-dev/feast/pull/1569) ([tsotnet](https://github.com/tsotnet))

### Merged Pull Requests

- Change OfflineStore class description [\#1571](https://github.com/feast-dev/feast/pull/1571) ([tedhtchang](https://github.com/tedhtchang))


# [v0.10.5](https://github.com/feast-dev/feast/tree/v0.10.5) (2021-05-19)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.4...v0.10.5)

### Features

- Add offline\_store config [\#1552](https://github.com/feast-dev/feast/pull/1552) ([tsotnet](https://github.com/tsotnet))

### Bug Fixes

- Validate project and repo names for apply and init commands [\#1558](https://github.com/feast-dev/feast/pull/1558) ([tedhtchang](https://github.com/tedhtchang))

### Merged Pull Requests

- Fix Sphinx documentation building [\#1563](https://github.com/feast-dev/feast/pull/1563) ([woop](https://github.com/woop))
- Add test coverage and remove MacOS integration tests [\#1562](https://github.com/feast-dev/feast/pull/1562) ([woop](https://github.com/woop))
- Improve GCP exception handling [\#1561](https://github.com/feast-dev/feast/pull/1561) ([woop](https://github.com/woop))
- Update default cli no option help message [\#1550](https://github.com/feast-dev/feast/pull/1550) ([tedhtchang](https://github.com/tedhtchang))
- Add opt-out exception logging telemetry [\#1535](https://github.com/feast-dev/feast/pull/1535) ([jklegar](https://github.com/jklegar))
- Add instruction for install Feast on IKS and OpenShift using Kustomize [\#1534](https://github.com/feast-dev/feast/pull/1534) ([tedhtchang](https://github.com/tedhtchang))

# [v0.10.4](https://github.com/feast-dev/feast/tree/v0.10.4) (2021-05-12)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.3...v0.10.4)

### Features

- Inferencing of Features in FeatureView and timestamp column of DataSource [\#1523](https://github.com/feast-dev/feast/pull/1523) ([mavysavydav](https://github.com/mavysavydav))
- Add Unix Timestamp value type [\#1520](https://github.com/feast-dev/feast/pull/1520) ([MattDelac](https://github.com/MattDelac))
- Fix materialize for None [\#1481](https://github.com/feast-dev/feast/pull/1481) ([qooba](https://github.com/qooba))

### Merged Pull Requests

- BigQuery type to Feast type conversion chart update [\#1530](https://github.com/feast-dev/feast/pull/1530) ([mavysavydav](https://github.com/mavysavydav))
- remove unnecessay path join in setup.py [\#1529](https://github.com/feast-dev/feast/pull/1529) ([shihabuddinbuet](https://github.com/shihabuddinbuet))
- Add roadmap to documentation [\#1528](https://github.com/feast-dev/feast/pull/1528) ([woop](https://github.com/woop))
- Add test matrix for different Python versions [\#1526](https://github.com/feast-dev/feast/pull/1526) ([woop](https://github.com/woop))
- Update broken urls in the github pr template file [\#1521](https://github.com/feast-dev/feast/pull/1521) ([tedhtchang](https://github.com/tedhtchang))
- Upgrade grpcio version in python SDK [\#1508](https://github.com/feast-dev/feast/pull/1508) ([szalai1](https://github.com/szalai1))
- Better logging for materialize command [\#1499](https://github.com/feast-dev/feast/pull/1499) ([jklegar](https://github.com/jklegar))


# [v0.10.3](https://github.com/feast-dev/feast/tree/v0.10.3) (2021-04-21)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.2...v0.10.3)

### Features

- Add support for third party providers [\#1501](https://github.com/feast-dev/feast/pull/1501) ([tsotnet](https://github.com/tsotnet))
- Infer entity dataframe event timestamp column [\#1495](https://github.com/feast-dev/feast/pull/1495) ([jklegar](https://github.com/jklegar))
- Allow Feast apply to import files recursively \(and add .feastignore\) [\#1482](https://github.com/feast-dev/feast/pull/1482) ([tsotnet](https://github.com/tsotnet))

### Bug Fixes

- Bump supported Python version to 3.7 [\#1504](https://github.com/feast-dev/feast/pull/1504) ([tsotnet](https://github.com/tsotnet))
- Fix bug in allowing empty repositories to be applied to a GCS registry [\#1488](https://github.com/feast-dev/feast/pull/1488) ([woop](https://github.com/woop))

### Merged Pull Requests

- Add a fixed timestamp to quickstart data [\#1513](https://github.com/feast-dev/feast/pull/1513) ([jklegar](https://github.com/jklegar))
- Make gcp imports optional [\#1512](https://github.com/feast-dev/feast/pull/1512) ([jklegar](https://github.com/jklegar))
- Fix documentation inconsistency [\#1510](https://github.com/feast-dev/feast/pull/1510) ([jongillham](https://github.com/jongillham))
- pre-commit command typo fix in CONTRIBUTING.md [\#1506](https://github.com/feast-dev/feast/pull/1506) ([mavysavydav](https://github.com/mavysavydav))
- Add optional telemetry to other CLI commands [\#1505](https://github.com/feast-dev/feast/pull/1505) ([jklegar](https://github.com/jklegar))
- Pass entities information to Provider [\#1498](https://github.com/feast-dev/feast/pull/1498) ([MattDelac](https://github.com/MattDelac))
- Update broken urls in contributing.md [\#1489](https://github.com/feast-dev/feast/pull/1489) ([tedhtchang](https://github.com/tedhtchang))
- Python docs formatting fixes [\#1473](https://github.com/feast-dev/feast/pull/1473) ([jklegar](https://github.com/jklegar))

# [v0.10.2](https://github.com/feast-dev/feast/tree/v0.10.2) (2021-04-21)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.1...v0.10.2)

### Bug Fixes

- Fix bug in allowing empty repositories to be applied to a GCS registry [\#1488](https://github.com/feast-dev/feast/pull/1488) ([woop](https://github.com/woop))

# [v0.10.1](https://github.com/feast-dev/feast/tree/v0.10.1) (2021-04-21)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.10.0...v0.10.1)

### Bug Fixes

- Fix time zone issue with get\_historical\_features [\#1475](https://github.com/feast-dev/feast/pull/1475) ([tsotnet](https://github.com/tsotnet))

### Merged Pull Requests

- Improve exception handling, logging, and validation [\#1477](https://github.com/feast-dev/feast/pull/1477) ([woop](https://github.com/woop))
- Remove duped pic [\#1476](https://github.com/feast-dev/feast/pull/1476) ([YikSanChan](https://github.com/YikSanChan))
- Fix created timestamp related errors for BigQuery source [\#1474](https://github.com/feast-dev/feast/pull/1474) ([jklegar](https://github.com/jklegar))
- Remove unnecessary MAVEN\_CONFIG [\#1472](https://github.com/feast-dev/feast/pull/1472) ([danielsiwiec](https://github.com/danielsiwiec))
- Fix CLI entities command & add feature-views command [\#1471](https://github.com/feast-dev/feast/pull/1471) ([tsotnet](https://github.com/tsotnet))


# [v0.10.0](https://github.com/feast-dev/feast/tree/0.10.0) (2021-04-15)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.9.5...v0.10.0)

### Features

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

### Bug Fixes

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

### Merged Pull Requests

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

# [v0.9.1](https://github.com/feast-dev/feast/tree/v0.9.1) (2021-01-29)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.9.0...v0.9.1)

### Features

- Add telemetry to Python SDK [\#1289](https://github.com/feast-dev/feast/pull/1289) ([jklegar](https://github.com/jklegar))

### Bug Fixes

- Fix kafka download url [\#1298](https://github.com/feast-dev/feast/pull/1298) ([jklegar](https://github.com/jklegar))
- disable telemetry in docker-compose test and job\_service [\#1297](https://github.com/feast-dev/feast/pull/1297) ([jklegar](https://github.com/jklegar))


# [v0.9.0](https://github.com/feast-dev/feast/tree/v0.9.0) (2021-01-28)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.8.4...v0.9.0)

### Features

- Enable user to provide spark job template as input for jobservice deployment [\#1285](https://github.com/feast-dev/feast/pull/1285) ([khorshuheng](https://github.com/khorshuheng))
- Add feature table name filter to jobs list api [\#1282](https://github.com/feast-dev/feast/pull/1282) ([terryyylim](https://github.com/terryyylim))
- Report observed value for aggregated checks in pre-ingestion feature validation [\#1278](https://github.com/feast-dev/feast/pull/1278) ([pyalex](https://github.com/pyalex))
- Add docs page for Azure setup [\#1276](https://github.com/feast-dev/feast/pull/1276) ([jklegar](https://github.com/jklegar))
- Azure example terraform [\#1274](https://github.com/feast-dev/feast/pull/1274) ([jklegar](https://github.com/jklegar))


### Bug Fixes

- make EMR jar uploader work the same as k8s one [\#1284](https://github.com/feast-dev/feast/pull/1284) ([oavdeev](https://github.com/oavdeev))
- Don't error when azure vars not set [\#1277](https://github.com/feast-dev/feast/pull/1277) ([jklegar](https://github.com/jklegar))
- Prevent ingestion job config parser from unwanted fieldMapping transformation [\#1261](https://github.com/feast-dev/feast/pull/1261) ([pyalex](https://github.com/pyalex))
- Features are not being ingested due to max age overflow [\#1209](https://github.com/feast-dev/feast/pull/1209) ([pyalex](https://github.com/pyalex))
- Feature Table is not being update when only max\_age was changed [\#1208](https://github.com/feast-dev/feast/pull/1208) ([pyalex](https://github.com/pyalex))
- Truncate staging timestamps in entities dataset to ms [\#1207](https://github.com/feast-dev/feast/pull/1207) ([pyalex](https://github.com/pyalex))
- Bump terraform rds module version [\#1204](https://github.com/feast-dev/feast/pull/1204) ([oavdeev](https://github.com/oavdeev))


### Merged Pull Requests

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


# [v0.8.2](https://github.com/feast-dev/feast/tree/v0.8.2) (2020-12-01)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.8.1...v0.8.2)

### Features

- Configurable materialization destination for view in BigQuerySource [\#1201](https://github.com/feast-dev/feast/pull/1201) ([pyalex](https://github.com/pyalex))

### Bug Fixes

- Fix tag order for release workflow [\#1205](https://github.com/feast-dev/feast/pull/1205) ([terryyylim](https://github.com/terryyylim))
- Fix Feature Table not updated on new feature addition [\#1197](https://github.com/feast-dev/feast/pull/1197) ([khorshuheng](https://github.com/khorshuheng))

### Merged Pull Requests

- Suppress kafka logs in Ingestion Job [\#1206](https://github.com/feast-dev/feast/pull/1206) ([pyalex](https://github.com/pyalex))
- Add project name to metrics labels in Ingestion Job [\#1202](https://github.com/feast-dev/feast/pull/1202) ([pyalex](https://github.com/pyalex))


# [v0.8.1](https://github.com/feast-dev/feast/tree/v0.8.1) (2020-11-24)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.8.0...v0.8.1)

### Features

- Expires Redis Keys based on Feature Table Max Age [\#1161](https://github.com/feast-dev/feast/pull/1161) ([khorshuheng](https://github.com/khorshuheng))
- Jobservice control loop \(based on \#1140\) [\#1156](https://github.com/feast-dev/feast/pull/1156) ([oavdeev](https://github.com/oavdeev))

### Bug Fixes

- Lazy metrics initialization \(to correct pick up in executor\) [\#1195](https://github.com/feast-dev/feast/pull/1195) ([pyalex](https://github.com/pyalex))
- Add missing third\_party folder [\#1185](https://github.com/feast-dev/feast/pull/1185) ([terryyylim](https://github.com/terryyylim))
- Fix missing name variable instantiation [\#1166](https://github.com/feast-dev/feast/pull/1166) ([terryyylim](https://github.com/terryyylim))

### Merged Pull Requests

- Bump ssh-agent version [\#1175](https://github.com/feast-dev/feast/pull/1175) ([terryyylim](https://github.com/terryyylim))
- Refactor configurable options and add sphinx docs [\#1174](https://github.com/feast-dev/feast/pull/1174) ([terryyylim](https://github.com/terryyylim))
- Stabilize flaky e2e tests [\#1173](https://github.com/feast-dev/feast/pull/1173) ([pyalex](https://github.com/pyalex))
- Fix connection resets in CI for Maven [\#1164](https://github.com/feast-dev/feast/pull/1164) ([woop](https://github.com/woop))
- Add dataproc executor resource config [\#1160](https://github.com/feast-dev/feast/pull/1160) ([terryyylim](https://github.com/terryyylim))
- Fix github workflow deprecating env variable [\#1158](https://github.com/feast-dev/feast/pull/1158) ([terryyylim](https://github.com/terryyylim))
- Ensure consistency of github workflow [\#1157](https://github.com/feast-dev/feast/pull/1157) ([terryyylim](https://github.com/terryyylim))


# [v0.8.0](https://github.com/feast-dev/feast/tree/v0.8.0) (2020-11-10)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.7.1...v0.8.0)

### Features

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

### Bug Fixes

- Fix stencil client serialization issue [\#1147](https://github.com/feast-dev/feast/pull/1147) ([pyalex](https://github.com/pyalex))
- Deadletter path is being incorrectly joined [\#1144](https://github.com/feast-dev/feast/pull/1144) ([pyalex](https://github.com/pyalex))
- In Historical Retrieval \(SDK\) use project from client context [\#1138](https://github.com/feast-dev/feast/pull/1138) ([pyalex](https://github.com/pyalex))
- Pass project from context to get entities [\#1137](https://github.com/feast-dev/feast/pull/1137) ([pyalex](https://github.com/pyalex))
- JobService is in crashloop after installing helm chart [\#1135](https://github.com/feast-dev/feast/pull/1135) ([pyalex](https://github.com/pyalex))
- Fix env var names for jupyter terraform config [\#1085](https://github.com/feast-dev/feast/pull/1085) ([oavdeev](https://github.com/oavdeev))
- Fix java class name validation [\#1084](https://github.com/feast-dev/feast/pull/1084) ([oavdeev](https://github.com/oavdeev))
- Multiple tiny AWS related fixes [\#1083](https://github.com/feast-dev/feast/pull/1083) ([oavdeev](https://github.com/oavdeev))

### Merged Pull Requests

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


# [v0.7.1](https://github.com/feast-dev/feast/tree/v0.7.1) (2020-10-07)
[Full Changelog](https://github.com/feast-dev/feast/compare/sdk/go/v0.7.0...v0.7.1)

### Bug Fixes

- Provide stable jobName in RowMetrics labels [\#1028](https://github.com/feast-dev/feast/pull/1028) ([pyalex](https://github.com/pyalex))

# [v0.7.0](https://github.com/feast-dev/feast/tree/v0.7.0) (2020-09-09)
[Full Changelog](https://github.com/feast-dev/feast/compare/sdk/go/v0.6.2...v0.7.0)

### Breaking Changes

- Add request response logging via fluentd [\#961](https://github.com/feast-dev/feast/pull/961) ([terryyylim](https://github.com/terryyylim))
- Run JobCoontroller as separate application [\#951](https://github.com/feast-dev/feast/pull/951) ([pyalex](https://github.com/pyalex))
- Output Subject Claim as Identity in Logging interceptor [\#946](https://github.com/feast-dev/feast/pull/946) ([mrzzy](https://github.com/mrzzy))
- Use JobManager's backend as persistent storage and source of truth [\#903](https://github.com/feast-dev/feast/pull/903) ([pyalex](https://github.com/pyalex))
- Fix invalid characters for project, featureset, entity and features creation [\#976](https://github.com/feast-dev/feast/pull/976) ([terryyylim](https://github.com/terryyylim))

### Features

- Add redis key prefix as an option to Redis cluster [\#975](https://github.com/feast-dev/feast/pull/975) ([khorshuheng](https://github.com/khorshuheng))
- Authentication Support for Java & Go SDKs [\#971](https://github.com/feast-dev/feast/pull/971) ([mrzzy](https://github.com/mrzzy))
- Add configurable prefix to Consumer Group in IngestionJob's Kafka reader [\#969](https://github.com/feast-dev/feast/pull/969) ([terryyylim](https://github.com/terryyylim))
- Configurable kafka consumer in IngestionJob [\#959](https://github.com/feast-dev/feast/pull/959) ([pyalex](https://github.com/pyalex))
- Restart Ingestion Job on code version update [\#949](https://github.com/feast-dev/feast/pull/949) ([pyalex](https://github.com/pyalex))
- Add REST endpoints for Feast UI [\#878](https://github.com/feast-dev/feast/pull/878) ([SwampertX](https://github.com/SwampertX))
- Upgrade Feast dependencies [\#876](https://github.com/feast-dev/feast/pull/876) ([pyalex](https://github.com/pyalex))

### Bug Fixes

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

### Merged Pull Requests

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

# [v0.6.2](https://github.com/feast-dev/feast/tree/v0.6.2) (2020-08-02)
[Full Changelog](https://github.com/feast-dev/feast/compare/v0.6.1...v0.6.2)

### Features

- Redis sink flushes only rows that have more recent eventTimestamp [\#913](https://github.com/feast-dev/feast/pull/913) ([pyalex](https://github.com/pyalex))
- Dataflow runner options: disk type & streaming engine [\#906](https://github.com/feast-dev/feast/pull/906) ([pyalex](https://github.com/pyalex))
- Add Structured Audit Logging [\#891](https://github.com/feast-dev/feast/pull/891) ([mrzzy](https://github.com/mrzzy))
- Add Authentication and Authorization for feast serving [\#865](https://github.com/feast-dev/feast/pull/865) ([jmelinav](https://github.com/jmelinav))
- Throw more informative exception when write\_triggering\_frequency\_seconds is missing [\#917](https://github.com/feast-dev/feast/pull/917) ([pyalex](https://github.com/pyalex))
- Add caching to authorization [\#884](https://github.com/feast-dev/feast/pull/884) ([jmelinav](https://github.com/jmelinav))
- Add Auth header [\#885](https://github.com/feast-dev/feast/pull/885) ([AnujaVane](https://github.com/AnujaVane))

### Bug Fixes

- Fix Online Serving unable to retrieve feature data after Feature Set update. [\#908](https://github.com/feast-dev/feast/pull/908) ([mrzzy](https://github.com/mrzzy))
- Fix Python SDK ingestion for featureset name that exist in multiple projects [\#868](https://github.com/feast-dev/feast/pull/868) ([terryyylim](https://github.com/terryyylim))
- Backport delay in Redis acknowledgement of spec [\#915](https://github.com/feast-dev/feast/pull/915) ([woop](https://github.com/woop))
- Allow unauthenticated access when Authorization is disabled and to Health Probe [\#927](https://github.com/feast-dev/feast/pull/927) ([mrzzy](https://github.com/mrzzy))

### Merged Pull Requests

- Upgrade Feast dependencies [\#876](https://github.com/feast-dev/feast/pull/876) ([pyalex](https://github.com/pyalex))

# [v0.6.1](https://github.com/feast-dev/feast/tree/v0.6.1) (2020-07-17)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.6.0...v0.6.1)

### Features

- Improve parallelization in Redis Sink [\#866](https://github.com/feast-dev/feast/pull/866) ([pyalex](https://github.com/pyalex))
- BQ sink produces sample of successful inserts [\#875](https://github.com/feast-dev/feast/pull/875) ([pyalex](https://github.com/pyalex))

### Bug Fixes

- Add IngestionId & EventTimestamp to FeatureRowBatch to calculate lag metric correctly [\#874](https://github.com/feast-dev/feast/pull/874) ([pyalex](https://github.com/pyalex))
- Prevent race condition in BQ sink jobId generation [\#877](https://github.com/feast-dev/feast/pull/877) ([pyalex](https://github.com/pyalex))

# [v0.6.0](https://github.com/feast-dev/feast/tree/v0.6.0) (2020-07-13)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.5.1...v0.6.0)

### Breaking Changes

- Compute and write metrics for rows prior to store writes [\#763](https://github.com/feast-dev/feast/pull/763) ([zhilingc](https://github.com/zhilingc))

### Features

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

### Bug Fixes

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

### Merged Pull Requests

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

### Features
- Add support for version method in Feast SDK and Core [\#759](https://github.com/feast-dev/feast/pull/759) ([woop](https://github.com/woop))
- Refactor runner configuration, add labels to dataflow options [\#718](https://github.com/feast-dev/feast/pull/718) ([zhilingc](https://github.com/zhilingc))

### Bug Fixes
- Fix pipeline options toArgs\(\) returning empty list [\#765](https://github.com/feast-dev/feast/pull/765) ([zhilingc](https://github.com/zhilingc))
- Fix project argument for feature set describe in CLI [\#731](https://github.com/feast-dev/feast/pull/731) ([terryyylim](https://github.com/terryyylim))
- Fix Go and Java SDK Regressions [\#729](https://github.com/feast-dev/feast/pull/729) ([mrzzy](https://github.com/mrzzy))
- Remove usage of parallel stream for feature value map generation [\#751](https://github.com/feast-dev/feast/pull/751) ([khorshuheng](https://github.com/khorshuheng))
- Restore Feast Java SDK and Ingestion compatibility with Java 8 runtimes [\#722](https://github.com/feast-dev/feast/pull/722) ([ches](https://github.com/ches))
- Python sdk bug fixes [\#723](https://github.com/feast-dev/feast/pull/723) ([zhilingc](https://github.com/zhilingc))

### Merged Pull Requests
- Increase Jaeger Tracing coverage [\#719](https://github.com/feast-dev/feast/pull/719) ([terryyylim](https://github.com/terryyylim))
- Recompile golang protos to include new FeatureSetStatus [\#755](https://github.com/feast-dev/feast/pull/755) ([zhilingc](https://github.com/zhilingc))
- Merge Redis cluster connector with Redis connector [\#752](https://github.com/feast-dev/feast/pull/752) ([pyalex](https://github.com/pyalex))
- Remove unused Hibernate dep from Serving [\#721](https://github.com/feast-dev/feast/pull/721) ([ches](https://github.com/ches))

# [v0.5.0](https://github.com/feast-dev/feast/tree/v0.5.0) (2020-05-19)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.7...v0.5.0)

### Breaking Changes

- Add .proto to packages of Protobuf generated Java classes [\#700](https://github.com/feast-dev/feast/pull/700) ([woop](https://github.com/woop))
- Add support for feature set updates and remove versions [\#676](https://github.com/feast-dev/feast/pull/676) ([zhilingc](https://github.com/zhilingc))
- Feast configuration files refactored [\#611](https://github.com/feast-dev/feast/pull/611) ([woop](https://github.com/woop))

See [Feast 0.5 Release Issue](https://github.com/feast-dev/feast/issues/527) for more details.

### Features

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

### Bug Fixes

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

### Merged Pull Requests

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

# [v0.4.7](https://github.com/feast-dev/feast/tree/v0.4.7) (2020-03-17)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.6...v0.4.7)

### Merged Pull Requests
- Add log4j-web jar to core and serving. [\#498](https://github.com/feast-dev/feast/pull/498) ([Yanson](https://github.com/Yanson))
- Clear all the futures when sync is called. [\#501](https://github.com/feast-dev/feast/pull/501) ([lavkesh](https://github.com/lavkesh))
- Encode feature row before storing in Redis [\#530](https://github.com/feast-dev/feast/pull/530) ([khorshuheng](https://github.com/khorshuheng))
- Remove transaction when listing projects [\#522](https://github.com/feast-dev/feast/pull/522) ([davidheryanto](https://github.com/davidheryanto))
- Remove unused ingestion deps [\#520](https://github.com/feast-dev/feast/pull/520) ([ches](https://github.com/ches))
- Parameterize end to end test scripts. [\#433](https://github.com/feast-dev/feast/pull/433) ([Yanson](https://github.com/Yanson))
- Replacing Jedis With Lettuce in ingestion and serving [\#485](https://github.com/feast-dev/feast/pull/485) ([lavkesh](https://github.com/lavkesh))

# [v0.4.6](https://github.com/feast-dev/feast/tree/v0.4.6) (2020-02-26)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.5...v0.4.6)

### Merged Pull Requests
- Rename metric name for request latency in feast serving [\#488](https://github.com/feast-dev/feast/pull/488) ([davidheryanto](https://github.com/davidheryanto))
- Allow use of secure gRPC in Feast Python client [\#459](https://github.com/feast-dev/feast/pull/459) ([Yanson](https://github.com/Yanson))
- Extend WriteMetricsTransform in Ingestion to write feature value stats to StatsD [\#486](https://github.com/feast-dev/feast/pull/486) ([davidheryanto](https://github.com/davidheryanto))
- Remove transaction from Ingestion [\#480](https://github.com/feast-dev/feast/pull/480) ([imjuanleonard](https://github.com/imjuanleonard))
- Fix fastavro version used in Feast to avoid Timestamp delta error [\#490](https://github.com/feast-dev/feast/pull/490) ([davidheryanto](https://github.com/davidheryanto))
- Fail Spotless formatting check before tests execute [\#487](https://github.com/feast-dev/feast/pull/487) ([ches](https://github.com/ches))
- Reduce refresh rate of specification refresh in Serving to 10 seconds [\#481](https://github.com/feast-dev/feast/pull/481) ([woop](https://github.com/woop))

# [v0.4.5](https://github.com/feast-dev/feast/tree/v0.4.5) (2020-02-14)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.4...v0.4.5)

### Merged Pull Requests
- Use bzip2 compressed feature set json as pipeline option [\#466](https://github.com/feast-dev/feast/pull/466) ([khorshuheng](https://github.com/khorshuheng))
- Make redis key creation more determinisitic [\#471](https://github.com/feast-dev/feast/pull/471) ([zhilingc](https://github.com/zhilingc))
- Helm Chart Upgrades [\#458](https://github.com/feast-dev/feast/pull/458) ([Yanson](https://github.com/Yanson))
- Exclude version from grouping [\#441](https://github.com/feast-dev/feast/pull/441) ([khorshuheng](https://github.com/khorshuheng))
- Use concrete class for AvroCoder compatibility [\#465](https://github.com/feast-dev/feast/pull/465) ([zhilingc](https://github.com/zhilingc))
- Fix typo in split string length check [\#464](https://github.com/feast-dev/feast/pull/464) ([zhilingc](https://github.com/zhilingc))
- Update README.md and remove versions from Helm Charts [\#457](https://github.com/feast-dev/feast/pull/457) ([woop](https://github.com/woop))
- Deduplicate example notebooks [\#456](https://github.com/feast-dev/feast/pull/456) ([woop](https://github.com/woop))
- Allow users not to set max age for batch retrieval [\#446](https://github.com/feast-dev/feast/pull/446) ([zhilingc](https://github.com/zhilingc))

# [v0.4.4](https://github.com/feast-dev/feast/tree/v0.4.4) (2020-01-28)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.3...v0.4.4)

### Merged Pull Requests

- Change RedisBackedJobService to use a connection pool [\#439](https://github.com/feast-dev/feast/pull/439) ([zhilingc](https://github.com/zhilingc))
- Update GKE installation and chart values to work with 0.4.3 [\#434](https://github.com/feast-dev/feast/pull/434) ([lgvital](https://github.com/lgvital))
- Remove "resource" concept and the need to specify a kind in feature sets [\#432](https://github.com/feast-dev/feast/pull/432) ([woop](https://github.com/woop))
- Add retry options to BigQuery [\#431](https://github.com/feast-dev/feast/pull/431) ([Yanson](https://github.com/Yanson))
- Fix logging [\#430](https://github.com/feast-dev/feast/pull/430) ([Yanson](https://github.com/Yanson))
- Add documentation for bigquery batch retrieval [\#428](https://github.com/feast-dev/feast/pull/428) ([zhilingc](https://github.com/zhilingc))
- Publish datatypes/java along with sdk/java [\#426](https://github.com/feast-dev/feast/pull/426) ([ches](https://github.com/ches))
- Update basic Feast example to Feast 0.4 [\#424](https://github.com/feast-dev/feast/pull/424) ([woop](https://github.com/woop))
- Introduce datatypes/java module for proto generation [\#391](https://github.com/feast-dev/feast/pull/391) ([ches](https://github.com/ches))

# [v0.4.3](https://github.com/feast-dev/feast/tree/v0.4.3) (2020-01-08)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.2...v0.4.3)

### Bug Fixes

- Bugfix for redis ingestion retries throwing NullPointerException on remote runners [\#417](https://github.com/feast-dev/feast/pull/417) ([khorshuheng](https://github.com/khorshuheng))

# [v0.4.2](https://github.com/feast-dev/feast/tree/v0.4.2) (2020-01-07)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.1...v0.4.2)

### Bug Fixes

- Missing argument in error string in ValidateFeatureRowDoFn [\#401](https://github.com/feast-dev/feast/issues/401)

### Merged Pull Requests

- Define maven revision property when packaging jars in Dockerfile so the images are built successfully [\#410](https://github.com/feast-dev/feast/pull/410) ([davidheryanto](https://github.com/davidheryanto))
- Deduplicate rows in subquery [\#409](https://github.com/feast-dev/feast/pull/409) ([zhilingc](https://github.com/zhilingc))
- Filter out extra fields, deduplicate fields in ingestion [\#404](https://github.com/feast-dev/feast/pull/404) ([zhilingc](https://github.com/zhilingc))
- Automatic documentation generation for gRPC API [\#403](https://github.com/feast-dev/feast/pull/403) ([woop](https://github.com/woop))
- Update feast core default values to include hibernate merge strategy [\#400](https://github.com/feast-dev/feast/pull/400) ([zhilingc](https://github.com/zhilingc))
- Move cli into feast package [\#398](https://github.com/feast-dev/feast/pull/398) ([zhilingc](https://github.com/zhilingc))
- Use Nexus staging plugin for deployment [\#394](https://github.com/feast-dev/feast/pull/394) ([khorshuheng](https://github.com/khorshuheng))
- Handle retry for redis io flow [\#274](https://github.com/feast-dev/feast/pull/274) ([khorshuheng](https://github.com/khorshuheng))

# [v0.4.1](https://github.com/feast-dev/feast/tree/v0.4.1) (2019-12-30)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.4.0...v0.4.1)

### Merged Pull Requests

- Add project-related commands to CLI [\#397](https://github.com/feast-dev/feast/pull/397) ([zhilingc](https://github.com/zhilingc))

# [v0.4.0](https://github.com/feast-dev/feast/tree/v0.4.0) (2019-12-28)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.5...v0.4.0)

### Features

- Edit description in feature specification to also reflect in BigQuery schema description. [\#239](https://github.com/feast-dev/feast/issues/239)
- Allow for disabling of metrics pushing [\#57](https://github.com/feast-dev/feast/issues/57)

### Merged Pull Requests

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

# [v0.3.8](https://github.com/feast-dev/feast/tree/v0.3.8) (2020-06-10)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.7...v0.3.8)

### Features

- v0.3 backport: Add feature and feature set labels [\#737](https://github.com/feast-dev/feast/pull/737) ([ches](https://github.com/ches))

### Merged Pull Requests

- v0.3 backport: Add Java coverage reporting [\#734](https://github.com/feast-dev/feast/pull/734) ([ches](https://github.com/ches))

# [v0.3.7](https://github.com/feast-dev/feast/tree/v0.3.7) (2020-05-01)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.6...v0.3.7)

### Merged Pull Requests

- Moved end-to-end test scripts from .prow to infra [\#657](https://github.com/feast-dev/feast/pull/657) ([khorshuheng](https://github.com/khorshuheng))
- Backported \#566 & \#647 to v0.3 [\#654](https://github.com/feast-dev/feast/pull/654) ([ches](https://github.com/ches))

# [v0.3.6](https://github.com/feast-dev/feast/tree/v0.3.6) (2020-01-03)

### Merged Pull Requests

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.5...v0.3.6)

- Add support for file paths for providing entity rows during batch retrieval [\#375](https://github.com/feast-dev/feast/pull/375) ([voonhous](https://github.com/voonhous))

# [v0.3.5](https://github.com/feast-dev/feast/tree/v0.3.5) (2019-12-26)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.4...v0.3.5)

### Merged Pull Requests

- Always set destination table in BigQuery query config in Feast Batch Serving so it can handle large results [\#392](https://github.com/feast-dev/feast/pull/392) ([davidheryanto](https://github.com/davidheryanto))

# [v0.3.4](https://github.com/feast-dev/feast/tree/v0.3.4) (2019-12-23)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.3...v0.3.4)

### Merged Pull Requests

- Make redis key creation more determinisitic [\#380](https://github.com/feast-dev/feast/pull/380) ([zhilingc](https://github.com/zhilingc))

# [v0.3.3](https://github.com/feast-dev/feast/tree/v0.3.3) (2019-12-18)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.2...v0.3.3)

### Features

- Added Docker Compose for Feast [\#272](https://github.com/feast-dev/feast/issues/272)
- Added ability to check import job status and cancel job through Python SDK [\#194](https://github.com/feast-dev/feast/issues/194)
- Added basic customer transactions example [\#354](https://github.com/feast-dev/feast/pull/354) ([woop](https://github.com/woop))

### Merged Pull Requests

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

# [v0.3.2](https://github.com/feast-dev/feast/tree/v0.3.2) (2019-11-29)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.1...v0.3.2)

### Merged Pull Requests

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

# [v0.3.1](https://github.com/feast-dev/feast/tree/v0.3.1) (2019-11-25)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.3.0...v0.3.1)

### Merged Pull Requests

- Added Prometheus metrics to serving [\#316](https://github.com/feast-dev/feast/pull/316) ([zhilingc](https://github.com/zhilingc))
- Changed default job metrics sink to Statsd [\#315](https://github.com/feast-dev/feast/pull/315) ([zhilingc](https://github.com/zhilingc))
- Fixed module import error in Feast CLI [\#314](https://github.com/feast-dev/feast/pull/314) ([davidheryanto](https://github.com/davidheryanto))

# [v0.3.0](https://github.com/feast-dev/feast/tree/v0.3.0) (2019-11-19)

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

### Merged Pull Requests

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

# [v0.1.8](https://github.com/feast-dev/feast/tree/v0.1.8) (2019-10-30)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.1.2...v0.1.8)

### Features

- Feast cli config file should be settable by an env var [\#149](https://github.com/feast-dev/feast/issues/149)
- Helm chart for deploying feast using Flink as runner [\#64](https://github.com/feast-dev/feast/issues/64)
- Get ingestion metrics when running on Flink runner [\#63](https://github.com/feast-dev/feast/issues/63)
- Move source types into their own package and discover them using java.util.ServiceLoader [\#61](https://github.com/feast-dev/feast/issues/61)
- Change config to yaml [\#51](https://github.com/feast-dev/feast/issues/51)
- Ability to pass runner option during ingestion job submission [\#50](https://github.com/feast-dev/feast/issues/50)

### Bug Fixes

- Fix Print Method in Feast CLI [\#211](https://github.com/feast-dev/feast/issues/211)
- Dataflow monitoring by core is failing with incorrect job id [\#153](https://github.com/feast-dev/feast/issues/153)
- Feast core crashes without logger set [\#150](https://github.com/feast-dev/feast/issues/150)

### Merged Pull Requests

- Remove redis transaction [\#280](https://github.com/feast-dev/feast/pull/280) ([pradithya](https://github.com/pradithya))
- Fix tracing to continue from existing trace created by grpc client [\#245](https://github.com/feast-dev/feast/pull/245) ([pradithya](https://github.com/pradithya))

# [v0.1.2](https://github.com/feast-dev/feast/tree/v0.1.2) (2019-08-23)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.1.1...v0.1.2)

### Bug Fixes

- Batch Import, feature with datetime format issue [\#203](https://github.com/feast-dev/feast/issues/203)
- Serving not correctly reporting readiness check if there is no activity [\#190](https://github.com/feast-dev/feast/issues/190)
- Serving stop periodically reloading feature specification after a while [\#188](https://github.com/feast-dev/feast/issues/188)

### Merged Pull Requests

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

# [v0.1.1](https://github.com/feast-dev/feast/tree/v0.1.1) (2019-04-18)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.1.0...v0.1.1)

### Bug Fixes

- Fix BigQuery query template to retrieve training data [\#182](https://github.com/feast-dev/feast/pull/182) ([davidheryanto](https://github.com/davidheryanto))

### Merged Pull Requests

- Add python init files [\#176](https://github.com/feast-dev/feast/pull/176) ([zhilingc](https://github.com/zhilingc))
- Change pypi package from Feast to feast [\#173](https://github.com/feast-dev/feast/pull/173) ([zhilingc](https://github.com/zhilingc))

# [v0.1.0](https://github.com/feast-dev/feast/tree/v0.1.0) (2019-04-09)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.0.2...v0.1.0)

### Features

- Removal of storing historical value of feature in serving storage [\#53](https://github.com/feast-dev/feast/issues/53)
- Remove feature "granularity" and relegate to metadata [\#17](https://github.com/feast-dev/feast/issues/17)

### Closes Issues

- Add ability to name an import job [\#167](https://github.com/feast-dev/feast/issues/167)
- Ingestion retrying an invalid FeatureRow endlessly [\#163](https://github.com/feast-dev/feast/issues/163)
- Ability to associate data ingested in Warehouse store to its ingestion job [\#145](https://github.com/feast-dev/feast/issues/145)
- Missing \(Fixing\) unit test for FeatureRowKafkaIO [\#132](https://github.com/feast-dev/feast/issues/132)

### Merged Pull Requests

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

# [v0.0.2](https://github.com/feast-dev/feast/tree/v0.0.2) (2019-03-11)

[Full Changelog](https://github.com/feast-dev/feast/compare/v0.0.1...v0.0.2)

### Features

- Coalesce FeatureRows for improved "latest" value consistency in serving stores [\#88](https://github.com/feast-dev/feast/issues/88)
- Kafka source [\#22](https://github.com/feast-dev/feast/issues/22)

### Closes Issues

- Preload Feast's spec in serving cache [\#151](https://github.com/feast-dev/feast/issues/151)
- Feast csv data upload job [\#137](https://github.com/feast-dev/feast/issues/137)
- Blocking call to start feast ingestion job [\#136](https://github.com/feast-dev/feast/issues/136)
- Python SDK fails to apply feature when submitting job [\#134](https://github.com/feast-dev/feast/issues/134)
- Default dump format should be changed for Python SDK [\#133](https://github.com/feast-dev/feast/issues/133)
- Listing resources and finding out system state [\#131](https://github.com/feast-dev/feast/issues/131)
- Reorganise ingestion store classes to match architecture  [\#109](https://github.com/feast-dev/feast/issues/109)

# [v0.0.1](https://github.com/feast-dev/feast/tree/v0.0.1) (2019-02-11)

[Full Changelog](https://github.com/feast-dev/feast/compare/ec9def2bbb06dc759538e4424caadd70f548ea64...v0.0.1)

### Features

- Spring boot CLI logs show up as JSON [\#104](https://github.com/feast-dev/feast/issues/104)
- Allow for registering feature that doesn't have warehouse store [\#5](https://github.com/feast-dev/feast/issues/5)

### Bug Fixes

- Error when submitting large import spec [\#125](https://github.com/feast-dev/feast/issues/125)
- Ingestion is not ignoring unknown feature in streaming source [\#99](https://github.com/feast-dev/feast/issues/99)
- Vulnerability in dependency \(core - jackson-databind \)  [\#92](https://github.com/feast-dev/feast/issues/92)
- TF file for cloud build trigger broken [\#72](https://github.com/feast-dev/feast/issues/72)
- Job Execution Failure with NullPointerException [\#46](https://github.com/feast-dev/feast/issues/46)
- Runtime Dependency Error After Upgrade to Beam 2.9.0 [\#44](https://github.com/feast-dev/feast/issues/44)
- \[FlinkRunner\] Core should not follow remote flink runner job to completion [\#21](https://github.com/feast-dev/feast/issues/21)
- Go packages in protos use incorrect repo [\#16](https://github.com/feast-dev/feast/issues/16)

### Merged Pull Requests

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
