# Upgrading Feast

## Migration 0.5 -&gt; 0.6

### Database schema

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

## Migration 0.4 -&gt; 0.6

### Database

For all versions earlier than 0.5 seamless migration is not feasible due to earlier breaking changes and creation of new database will be required.

Since database will be empty - first \(baseline\) migration would be applied:

```text
>> select version, description, script, checksum from flyway_schema_history

version |              description                |                          script         |  checksum
--------+-----------------------------------------+-----------------------------------------+------------
 1       | Baseline                                | V1__Baseline.sql                        | 1091472110
 2       | RELEASE 0.6 Generalizing Source AND ... | V2__RELEASE_0.6_Generalizing_Source_... | 1537500232
```

