Feast Python API Documentation
==============================


Feature Store
==================

.. automodule:: feast.feature_store
   :members:
   :undoc-members:
   :show-inheritance:

Config
==================

.. automodule:: feast.repo_config
    :members:
    :exclude-members: load_repo_config, FeastBaseModel

Data Source
==================

.. automodule:: feast.data_source
    :members:
    :exclude-members: KafkaOptions, KafkaSource, KinesisOptions, KinesisSource

BigQuery Source
------------------

.. automodule:: feast.infra.offline_stores.bigquery_source
    :members:
    :exclude-members: BigQueryOptions

Redshift Source
------------------

.. automodule:: feast.infra.offline_stores.redshift_source
    :members:
    :exclude-members: RedshiftOptions

Snowflake Source
------------------

.. automodule:: feast.infra.offline_stores.snowflake_source
    :members:
    :exclude-members: SnowflakeOptions

Spark Source
------------------

.. automodule:: feast.infra.offline_stores.contrib.spark_offline_store.spark_source
    :members:
    :exclude-members: SparkOptions

Trino Source
------------------

.. automodule:: feast.infra.offline_stores.contrib.trino_offline_store.trino_source
    :members:
    :exclude-members: TrinoOptions

PostgreSQL Source
------------------

.. automodule:: feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source
    :members:
    :exclude-members: PostgreSQLOptions

File Source
------------------

.. automodule:: feast.infra.offline_stores.file_source
    :members:
    :exclude-members: FileOptions

Entity
==================

.. automodule:: feast.entity
    :inherited-members:
    :members:

Feature View
==================

.. automodule:: feast.feature_view
    :members:

On Demand Feature View
======================

.. automodule:: feast.on_demand_feature_view
    :members:

Feature
==================

.. automodule:: feast.feature
    :inherited-members:
    :members:

Feature Service
==================

.. automodule:: feast.feature_service
    :inherited-members:
    :members:

Registry
==================

.. automodule:: feast.registry
    :inherited-members:
    :members:

Registry Store
==================

.. automodule:: feast.registry_store
    :inherited-members:
    :members:
    :exclude-members: NoopRegistryStore


Provider
==================

.. automodule:: feast.infra.provider
    :inherited-members:
    :members:

Passthrough Provider
--------------------

.. automodule:: feast.infra.passthrough_provider
    :members:

Local Provider
------------------

.. automodule:: feast.infra.local
    :members:
    :exclude-members: LocalRegistryStore

GCP Provider
------------------

.. automodule:: feast.infra.gcp
    :members:
    :exclude-members: GCSRegistryStore

AWS Provider
------------------

.. automodule:: feast.infra.aws
    :members:
    :exclude-members: S3RegistryStore

Offline Store
==================

.. automodule:: feast.infra.offline_stores.offline_store
    :members:

File Offline Store
------------------

.. automodule:: feast.infra.offline_stores.file
    :members:

BigQuery Offline Store
----------------------

.. automodule:: feast.infra.offline_stores.bigquery
    :members:

Redshift Offline Store
----------------------

.. automodule:: feast.infra.offline_stores.redshift
    :members:

Snowflake Offline Store
-----------------------

.. automodule:: feast.infra.offline_stores.snowflake
    :members:

Spark Offline Store
-------------------

.. automodule:: feast.infra.offline_stores.contrib.spark_offline_store.spark
    :members:

Trino Offline Store
-------------------

.. automodule:: feast.infra.offline_stores.contrib.trino_offline_store.trino
    :members:

PostgreSQL Offline Store
------------------------

.. automodule:: feast.infra.offline_stores.contrib.postgres_offline_store.postgres
    :members:


Online Store
==================

.. automodule:: feast.infra.online_stores.online_store
    :inherited-members:
    :members:

Sqlite Online Store
-------------------

.. automodule:: feast.infra.online_stores.sqlite
    :members:

Datastore Online Store
----------------------

.. automodule:: feast.infra.online_stores.datastore
    :members:

DynamoDB Online Store
---------------------

.. automodule:: feast.infra.online_stores.dynamodb
    :members:

Redis Online Store
------------------

.. automodule:: feast.infra.online_stores.redis
    :members:
    :noindex:

PostgreSQL Online Store
-----------------------

.. automodule:: feast.infra.online_stores.contrib.postgres
    :members:
    :noindex:
