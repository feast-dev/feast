# Feast Spec Document

This documentation describe spec file used by Feast. There are 4 kind of specs:
1. Entity Spec, which describe an entity definition.
2. Feature Spec, which describe feature definition.
3. Import Spec, which describe how to ingest/populate data for one/more features.
4. Storage Spec, which describe storage either for warehouse or serving.

# Entity Spec
An entity is a type with an associated key which generally maps onto a known domain object, e.g. `driver`, `customer`, `area`, and `merchant`. An entity determines how a feature may be retrieved. e.g. for a `driver` entity all driver features must be looked up with an associated driver id entity key.
Entity is described by entity spec. Following is an example of an entity spec

```
name: driver
description: GO-JEK’s 2 wheeler driver
tags:
  - two wheeler
  - gojek
```

### Attributes Supported In Entity Spec

| Name | Type | Convention | Description |
| ----- |----------| -----------| ------------|
| `name`  | string   | Lower snake case (e.g driver or driver_area) | Entity name MUST be unique |
| `description` | string | N.A. | Description of the entity |
| `tags` | List of string | N.A. | Free form grouping |



## Feature Spec
A Feature is an individual measurable property or characteristic of an Entity. A feature has many to one relationship with an entity by referencing the entity's name from its feature spec. In the context of Feast, a Feature has the following attributes: 

1. Entity - it must be associated with a known Entity to Feast (see Entity Spec)
2. ValueType - the feature type must be defined, e.g. String, Bytes, Int64, Int32, Float etc.
3. Granularity - Time series features require a defined granularity
4. Data stores - for both serving and training must be defined

Following is en example of feature spec

```
id: titanic_passenger.none.alone
name: alone
entity: titanic_passenger
owner: zhiling.c@go-jek.com
description: binary variable denoting whether the passenger was alone on the titanic.
valueType:  INT64
granularity: NONE
uri: http://jupyter.s.ds.golabs.io/user/your_user/lab/tree/shared/zhiling.c/feast_titanic/titanic.ipynb
dataStores:
  serving:
    id: REDIS1
  warehouse:
    id: BIGQUERY1
```

### Attributes Supported In Feature Spec

|Name|Type|Convention|Description|
|:----:|----|----------|-----------|
|`entity`|string|lower snake case (e.g. `driver`, `driver_area`)| Entity related to this feature|
|`id`|string|lower snake case with format `[entity].[granularity].[featureName]` (e.g.: `customer.none.age_prediction`) | feature id is a unique identifier of a feature, it is used both for feature retrieval from serving / warehouse|
|`name`|string|lower snake case, e.g: `age_prediction`| short name of a feature |
|`owner`|string|use email or any unique identifier| `owner` is mainly be used to inform user who is responsible of maintaining the feature |
|`description`|string| Keep the description concise, more information about the feature can be provided as documentation linked by `uri` | human readable description of a feature |
|`granularity`| Enum (one of: `NONE`, `SECOND`, `MINUTE`, `HOUR`, `DAY`) | N.A. | Identifies the level of detail for a feature, e.g. every second, minute, hour, day, or none.|
|`valueType`| Enum(one of: `BYTES`,`STRING`,`INT32`,`INT64`,`DOUBLE`,`FLOAT`, `BOOL`, `TIMESTAMP`)| N.A.| Value type of the feature |
|`group`| string | lower snake case | feature group inherited by this feature.|
| `tags` | List of string | N.A. | Free form grouping |
| `options` | Key value string | N.A. | Option is used for extendability of feature spec|
| `dataStores`| See below table | See below table | Storage used to store the feature|

#### DataStores 
`dataStores` attribute is used to define which storage to be used by the said feature. Feature must define two kind of storage: serving and warehouse. See storage spec section for more details about storage. Following table define attributes of `dataStores`. `dataStores` contains two parts: `serving` and `warehouse`. `serving` attribute is used to define what serving storage to be used, same goes for `warehouse` which define warehouse storage to be used.

|Name|Type|Convention|Description|
|----|----|----------|-----------|
|`id`|string| Upper snake case (e.g. BIGTABLE_STAGING)| identifier of the storage (see storage spec)|
|`options`| key-value string| N.A. | feature specific storage option |


#### Feature-specific storage option
|Storage Type|Key|Value Convetion|Description|
|------------|---|---------------|-----------|
|Redis|`expiry`| ISO8601 Period String (e.g. PT1H for 1 hour)| It is recommended to set expiry for redis so that old data might be removed. No data expiration is applied if `expiry` is not specified. |
|Redis|`bucketSize`| ISO8601 Period String (e.g. PT1H for 1 hour) | It is used for bucketing time-series data in order to optimize range query. Default: 1 hour |
|Big Table | `family` | As short as possible | See [Big table column family](https://cloud.google.com/bigtable/docs/schema-design#column_families). Default: `default`|

Example of feature spec having storage option:
1. Redis

```
id: driver.day.completed_booking
name: completed_booking
entity: driver
owner: zhiling.c@go-jek.com
description: total daily completed booking of a driver.
valueType:  INT64
granularity: DAY
uri: -
dataStores:
  serving:
    id: REDIS1
    options:
      expiry: P10D // 10 days
      bucketSize : P2D // 2 days bucket size
  warehouse:
    id: BIGQUERY1
```
2. BigTable

```
id: driver.day.completed_booking
name: completed_booking
entity: driver
owner: zhiling.c@go-jek.com
description: total daily completed booking of a driver.
valueType:  INT64
granularity: DAY
uri: -
dataStores:
  serving:
    id: BIGTABLE1
    options:
      family: driver 
  warehouse:
    id: BIGQUERY1
```

## Import Spec
Import spec describe how data is ingested into Feast to populate one or more features. An import spec contains information about:
* The entities to be imported.
* Import source type.
* Import source options, as required by the type.
* Import source schema, if required by the type.

Feast supports ingesting feature from 4 type of sources:
* File (either CSV or JSON)
* Bigquery Table
* Pubsub Topic
* Pubsub Subscription

Following section describes the import spec that has to be created for each of sources:

#### File
Example of csv file import spec:
```
type: file.csv
options:
  path: gs://my-bucket/customer_total_purchase.csv
entities:
  - customer
schema:
  entityIdColumn: customer_id
  timestampValue: 2018-09-25T00:00:00.000Z
  fields:
    - name: timestamp
    - name: customer_id
    - name: total_purchase
      featureId: customer_id.none.total_purchase
```
Example of json file import spec:
```
type: file.json
options:
  path: gs://my-bucket/customer_total_purchase.json
entities:
  - customer
schema:
  entityIdColumn: customer_id
  timestampValue: 2018-09-25T00:00:00.000Z
  fields:
    - name: timestamp
    - name: customer_id
    - name: total_purchase
      featureId: customer_id.none.total_purchase
```
Notes on import spec for `file` type:
- The `type` field must be `file.csv` or `file.json`
- `options.path` specify the location of file, it must be accessible by Ingestion Job. (e.g. GCS)
- `entities` list must only contain one entity.
- `schema` must be specified.
- `schema.entityIdColumn` must be same as one of the `fields` name. It is used to find out which column is used as entity ID.
- `schema.timestampColumn` can be specified to inform the `timestamp` column name. This field should be exclusively used with `schema.timestampValue`. If `schema.timestampValue` is used instead, all feature will have same timestamp.
- `schema.fields` is a list of column/fields in the files. You can specify whether the field contains a feature by adding `featureId` attribute and specify the feature ID.

#### BigQuery

Import spec for Big Query is almost similar to importing a file. The only difference is that all `schema.fields.name` has to match column name of the source table. 

For example to import following table from BigQuery table `gcp-project.source-dataset.source-table` to a feature with id `customer.none.last_login`:

|customer_id|last_login|
|------------|---|
|...|...|
|...|...|

You will need to specify import spec which looks like this:
```
type: bigquery
options:
    project: gcp-project
    dataset: source-dataset
    table: source-table
entities:
    - customer
schema:
    entityIdColumn: customer_id
    timestampValue: 2018-10-25T00:00:00.000Z # ignored because features have NONE granularity
    fields:
    - name: customer_id
    - name: last_login
        featureId: customer.none.last_login
```

Notes on import spec for `bigquery` type:
1. The `type` field must be `bigquery`
2. `options.project` specifies GCP project of the source table.
3. `options.dataset` specifies Big Query data set of the source table.
4. `options.table` specifies the source table name.
5. `entities` list must only contain one entity.
6. `schema.entityIdColumn` must be same as one of the `fields` name. It is used to find out which column is used as entity ID.
7. `schema.timestampColumn` can be specified to inform the `timestamp` column name. This field should be exclusively used with `schema.timestampValue`. If `schema.timestampValue` is used instead, all feature will have same timestamp.
8. `schema.fields` is a list of column in the table and all `schema.fields.name` must match column name of the table. You can specify whether the column contains a feature by adding `featureId` attribute and specify the feature ID.

**Important Notes**
Be careful when ingesting a partitioned table since by default ingestion job will read the whole table and might cause unpredictable ingestion result. You can specify which partition to ingest by using table decorator, as follow:

```
type: bigquery
options:
    project: gcp-project
    dataset: source-dataset
    table: source-table$20181101 # yyyyMMdd format, it will only ingest the said partition.
entities:
    - customer
schema:
    entityIdColumn: customer_id
    timestampValue: 2018-10-25T00:00:00.000Z # ignored because features have NONE granularity
    fields:
    - name: customer_id
    - name: last_login
        featureId: customer.none.last_login
```

#### PubSub

You can ingest from either PubSub topic or subscription. 
For example following import spec will ingest data from PubSub topic `feast-test` in GCP project `my-gcp-project`

```
type: pubsub
options:
  topic: projects/my-gcp-project/topics/feast-test
entities:
  - customer
schema:
  fields:
  - featureId: customer.none.last_login
```

Or if the source data is a subscription with name `feast-test-subscription`
```
type: pubsub
options:
  topic: projects/my-gcp-project/subscriptions/feast-test-subscription
entities:
  - customer
schema:
  fields:
  - featureId: customer.none.last_login
```

The PubSub source type expects that all messages that the ingestion job receives are binary encoded FeatureRow protobufs. See [FeatureRow.proto](protos/feast/types/FeatureRow.proto).

Note that:
1. You must be explicit about which features should be ingested using `schema.fields`.


# Storage Spec
There are 2 kinds of storage in feast:
1. Serving Storage (BigTable and Redis)
2. Warehouse Storage (BigQuery)

Serving storage is intended to support low latency feature retrieval, whereas warehouse storage is intended for training or data exploration. 
Each of supported storage has slightly different option. Following are the storage spec description for each storage.

#### Big Query

BigQuery is used as warehouse storage in Feast.

```
id: BIGQUERY1
type: bigquery
options:
  dataset: "test_feast"
  project: "the-big-data-staging-007"
  tempLocation: "gs://zl-test-bucket"
```

#### BigTable
BigTable is used as serving storage. It is advisable to use BigTable for feature which doesn't require low latency access or feature which has huge amount of data.

```
id: BIGTABLE1
type: bigtable
options:
  instance: "ds-staging"
  project: "the-big-data-staging-007"
```

#### Redis
Redis is recommended to be used by feature which requires very low latency access. However, it has limited storage size compared to BigTable.
```
id: REDIS1
type: redis
options:
  host: "127.0.0.1"
  port: "6379"

```
