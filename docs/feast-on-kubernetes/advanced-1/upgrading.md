# Upgrading Feast

### Migration from v0.6 to v0.7

#### Feast Core Validation changes

In v0.7, Feast Core no longer accepts starting with number \(0-9\) and using dash in names for:

* Project
* Feature Set
* Entities
* Features

Migrate all project, feature sets, entities, feature names:

* with ‘-’ by recreating them with '-' replace with '\_'
* recreate any names with a number \(0-9\)  as the first letter to one without.

Feast now prevents feature sets from being applied if no store is subscribed to that Feature Set.

* Ensure that a store is configured to subscribe to the Feature Set before applying the Feature Set.

#### Feast Core's Job Coordinator is now Feast Job Controller

In v0.7,  Feast Core's Job Coordinator has been decoupled from Feast Core and runs as a separate Feast Job Controller application. See its [Configuration reference](../reference-1/configuration-reference.md#2-feast-core-serving-and-job-controller) for how to configure Feast Job Controller.

**Ingestion Job API**

In v0.7, the following changes are made to the Ingestion Job API:

* Changed List Ingestion Job API  to return list of `FeatureSetReference` instead of list of FeatureSet in response.
* Moved `ListIngestionJobs`, `StopIngestionJob`, `RestartIngestionJob` calls from `CoreService`  to `JobControllerService`.
* Python SDK/CLI: Added new [Job Controller client ](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/contrib/job_controller/client.py)and `jobcontroller_url` config option.

Users of the Ingestion Job API via gRPC should migrate by:

* Add new client to connect to Job Controller endpoint to call `JobControllerService` and call `ListIngestionJobs`, `StopIngestionJob`, `RestartIngestionJob` from new client.
* Migrate code to accept feature references instead of feature sets returned in `ListIngestionJobs` response.

Users of Ingestion Job via Python SDK \(ie `feast ingest-jobs list` or `client.stop_ingest_job()` etc.\) should migrate by:

* `ingest_job()`methods only: Create a new separate [Job Controller client](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/contrib/job_controller/client.py) to connect to the job controller and call `ingest_job()` methods using the new client.
* Configure the Feast Job Controller endpoint url via `jobcontroller_url` config option.

#### Configuration Properties Changes

* Rename `feast.jobs.consolidate-jobs-per-source property` to `feast.jobs.controller.consolidate-jobs-per-sources`
* Rename`feast.security.authorization.options.subjectClaim` to  `feast.security.authentication.options.subjectClaim`
* Rename `feast.logging.audit.messageLoggingEnabled` to `feast.audit.messageLogging.enabled`

### Migration from v0.5 to v0.6

#### Database schema

In Release 0.6 we introduced [Flyway](https://flywaydb.org/) to handle schema migrations in PostgreSQL. Flyway is integrated into `core` and for now on all migrations will be run automatically on `core` start. It uses table `flyway_schema_history` in the same database \(also created automatically\) to keep track of already applied migrations. So no specific maintenance should be needed.

If you already have existing deployment of feast 0.5 - Flyway will detect existing tables and omit first baseline migration.

After `core` started you should have `flyway_schema_history` look like this

```text
>> select version, description, script, checksum from flyway_schema_history

version |              description                |                          script         |  checksum
--------+-----------------------------------------+-----------------------------------------+------------
 1       | << Flyway Baseline >>                   | << Flyway Baseline >>                   | 
 2       | RELEASE 0.6 Generalizing Source AND ... | V2__RELEASE_0.6_Generalizing_Source_... | 1537500232
```

In this release next major schema changes were done:

* Source is not shared between FeatureSets anymore. It's changed to 1:1 relation

  and source's primary key is now auto-incremented number.

* Due to generalization of Source `sources.topics` & `sources.bootstrap_servers` columns were deprecated.

  They will be replaced with `sources.config`. Data migration handled by code when respected Source is used.

  `topics` and `bootstrap_servers` will be deleted in the next release.

* Job \(table `jobs`\) is no longer connected to `Source` \(table `sources`\) since it uses consolidated source for optimization purposes.

  All data required by Job would be embedded in its table.

New Models \(tables\):

* feature\_statistics

Minor changes:

* FeatureSet has new column version \(see [proto](https://github.com/feast-dev/feast/blob/master/protos/feast/core/FeatureSet.proto) for details\)
* Connecting table `jobs_feature_sets` in many-to-many relation between jobs & feature sets

  has now `version` and `delivery_status`.

### Migration from v0.4 to v0.6

#### Database

For all versions earlier than 0.5 seamless migration is not feasible due to earlier breaking changes and creation of new database will be required.

Since database will be empty - first \(baseline\) migration would be applied:

```text
>> select version, description, script, checksum from flyway_schema_history

version |              description                |                          script         |  checksum
--------+-----------------------------------------+-----------------------------------------+------------
 1       | Baseline                                | V1__Baseline.sql                        | 1091472110
 2       | RELEASE 0.6 Generalizing Source AND ... | V2__RELEASE_0.6_Generalizing_Source_... | 1537500232
```

