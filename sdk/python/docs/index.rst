Feast Python API Documentation
==============================

..  We prefer 'autoclass' instead of 'autoclass' as 'autoclass' can specify a class, whereas
    'autoclass' will pull in all public classes and methods from that module, which we typically
    do not want.

Feature Store
==================

.. autoclass:: feast.feature_store.FeatureStore
    :members:

Config
==================

.. autoclass:: feast.repo_config.RepoConfig
    :members:

.. autoclass:: feast.repo_config.RegistryConfig
    :members:

Data Source
==================

.. autoclass:: feast.data_source.DataSource
    :members:

File Source
------------------

.. autoclass:: feast.infra.offline_stores.file_source.FileSource
    :members:

Snowflake Source
------------------

.. autoclass:: feast.infra.offline_stores.snowflake_source.SnowflakeSource
    :members:

BigQuery Source
------------------

.. autoclass:: feast.infra.offline_stores.bigquery_source.BigQuerySource
    :members:

Redshift Source
------------------

.. autoclass:: feast.infra.offline_stores.redshift_source.RedshiftSource
    :members:

Spark Source
------------------

.. autoclass:: feast.infra.offline_stores.contrib.spark_offline_store.spark_source.SparkSource
    :members:

Trino Source
------------------

.. autoclass:: feast.infra.offline_stores.contrib.trino_offline_store.trino_source.TrinoSource
    :members:

PostgreSQL Source
------------------

.. autoclass:: feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source.PostgreSQLSource
    :members:

Request Source
------------------

.. autoclass:: feast.data_source.RequestSource
    :members:

Push Source
------------------

.. autoclass:: feast.data_source.PushSource
    :members:

Kafka Source
------------------

.. autoclass:: feast.data_source.KafkaSource
    :members:

Kinesis Source
------------------

.. autoclass:: feast.data_source.KinesisSource
    :members:

Entity
==================

.. autoclass:: feast.entity.Entity
    :members:

Feature View
==================

.. autoclass:: feast.base_feature_view.BaseFeatureView
    :members:

Feature View
----------------------

.. autoclass:: feast.feature_view.FeatureView
    :members:

On Demand Feature View
----------------------

.. autoclass:: feast.on_demand_feature_view.OnDemandFeatureView
    :members:

Batch Feature View
----------------------

.. autoclass:: feast.batch_feature_view.BatchFeatureView
    :members:

Stream Feature View
----------------------

.. autoclass:: feast.stream_feature_view.StreamFeatureView
    :members:

Field
==================

.. autoclass:: feast.field.Field
    :members:

Feature Service
==================

.. autoclass:: feast.feature_service.FeatureService
    :members:

Registry
==================

.. autoclass:: feast.infra.registry.base_registry.BaseRegistry
    :members:

Registry
----------------------

.. autoclass:: feast.infra.registry.registry.Registry
    :members:

SQL Registry
----------------------

.. autoclass:: feast.infra.registry.sql.SqlRegistry
    :members:

Registry Store
==================

.. autoclass:: feast.infra.registry.registry_store.RegistryStore
    :members:

File Registry Store
-----------------------

.. autoclass:: feast.infra.registry.file.FileRegistryStore
    :members:

GCS Registry Store
-----------------------

.. autoclass:: feast.infra.registry.gcs.GCSRegistryStore
    :members:

S3 Registry Store
-----------------------

.. autoclass:: feast.infra.registry.s3.S3RegistryStore
    :members:

Provider
==================

.. autoclass:: feast.infra.provider.Provider
    :members:

Passthrough Provider
--------------------

.. autoclass:: feast.infra.passthrough_provider.PassthroughProvider
    :members:

Local Provider
------------------

.. autoclass:: feast.infra.local.LocalProvider
    :members:

GCP Provider
------------------

.. autoclass:: feast.infra.gcp.GcpProvider
    :members:

AWS Provider
------------------

.. autoclass:: feast.infra.aws.AwsProvider
    :members:

Offline Store
==================

.. autoclass:: feast.infra.offline_stores.offline_store.OfflineStore
    :members:

.. autoclass:: feast.infra.offline_stores.offline_store.RetrievalJob
    :members:

File Offline Store
------------------

.. autoclass:: feast.infra.offline_stores.file.FileOfflineStore
    :members:

.. autoclass:: feast.infra.offline_stores.file.FileOfflineStoreConfig
    :members:

.. autoclass:: feast.infra.offline_stores.file.FileRetrievalJob
    :members:

Snowflake Offline Store
-----------------------

.. autoclass:: feast.infra.offline_stores.snowflake.SnowflakeOfflineStore
    :members:

.. autoclass:: feast.infra.offline_stores.snowflake.SnowflakeOfflineStoreConfig
    :members:

.. autoclass:: feast.infra.offline_stores.snowflake.SnowflakeRetrievalJob
    :members:

BigQuery Offline Store
----------------------

.. autoclass:: feast.infra.offline_stores.bigquery.BigQueryOfflineStore
    :members:

.. autoclass:: feast.infra.offline_stores.bigquery.BigQueryOfflineStoreConfig
    :members:

.. autoclass:: feast.infra.offline_stores.bigquery.BigQueryRetrievalJob
    :members:

Redshift Offline Store
----------------------

.. autoclass:: feast.infra.offline_stores.redshift.RedshiftOfflineStore
    :members:

.. autoclass:: feast.infra.offline_stores.redshift.RedshiftOfflineStoreConfig
    :members:

.. autoclass:: feast.infra.offline_stores.redshift.RedshiftRetrievalJob
    :members:

Spark Offline Store
-------------------

.. autoclass:: feast.infra.offline_stores.contrib.spark_offline_store.spark.SparkOfflineStore
    :members:

.. autoclass:: feast.infra.offline_stores.contrib.spark_offline_store.spark.SparkOfflineStoreConfig
    :members:

.. autoclass:: feast.infra.offline_stores.contrib.spark_offline_store.spark.SparkRetrievalJob
    :members:

Trino Offline Store
-------------------

.. autoclass:: feast.infra.offline_stores.contrib.trino_offline_store.trino.TrinoOfflineStore
    :members:

.. autoclass:: feast.infra.offline_stores.contrib.trino_offline_store.trino.TrinoOfflineStoreConfig
    :members:

.. autoclass:: feast.infra.offline_stores.contrib.trino_offline_store.trino.TrinoRetrievalJob
    :members:

PostgreSQL Offline Store
------------------------

.. autoclass:: feast.infra.offline_stores.contrib.postgres_offline_store.postgres.PostgreSQLOfflineStore
    :members:

.. autoclass:: feast.infra.offline_stores.contrib.postgres_offline_store.postgres.PostgreSQLOfflineStoreConfig
    :members:

.. autoclass:: feast.infra.offline_stores.contrib.postgres_offline_store.postgres.PostgreSQLRetrievalJob
    :members:

Online Store
==================

.. autoclass:: feast.infra.online_stores.online_store.OnlineStore
    :members:

Sqlite Online Store
-------------------

.. autoclass:: feast.infra.online_stores.sqlite.SqliteOnlineStore
    :members:

.. autoclass:: feast.infra.online_stores.sqlite.SqliteOnlineStoreConfig
    :members:

Datastore Online Store
----------------------

.. autoclass:: feast.infra.online_stores.datastore.DatastoreOnlineStore
    :members:

.. autoclass:: feast.infra.online_stores.datastore.DatastoreOnlineStoreConfig
    :members:

DynamoDB Online Store
---------------------

.. autoclass:: feast.infra.online_stores.dynamodb.DynamoDBOnlineStore
    :members:

.. autoclass:: feast.infra.online_stores.dynamodb.DynamoDBOnlineStoreConfig
    :members:

Redis Online Store
------------------

.. autoclass:: feast.infra.online_stores.redis.RedisOnlineStore
    :members:

.. autoclass:: feast.infra.online_stores.redis.RedisOnlineStoreConfig
    :members:

Snowflake Online Store
------------------

.. autoclass:: feast.infra.online_stores.snowflake.SnowflakeOnlineStore
    :members:

.. autoclass:: feast.infra.online_stores.snowflake.SnowflakeOnlineStoreConfig
    :members:

PostgreSQL Online Store
-----------------------

.. autoclass:: feast.infra.online_stores.postgres_online_store.PostgreSQLOnlineStore
    :members:

.. autoclass:: feast.infra.online_stores.postgres_online_store.PostgreSQLOnlineStoreConfig
    :members:

HBase Online Store
-----------------------

.. autoclass:: feast.infra.online_stores.hbase_online_store.hbase.HbaseOnlineStore
    :members:

.. autoclass:: feast.infra.online_stores.hbase_online_store.hbase.HbaseOnlineStoreConfig
    :members:

Cassandra Online Store
-----------------------

.. autoclass:: feast.infra.online_stores.cassandra_online_store.cassandra_online_store.CassandraOnlineStore
    :members:

.. autoclass:: feast.infra.online_stores.cassandra_online_store.cassandra_online_store.CassandraOnlineStoreConfig
    :members:

Batch Materialization Engine
============================

.. autoclass:: feast.infra.materialization.batch_materialization_engine.BatchMaterializationEngine
    :members:

.. autoclass:: feast.infra.materialization.batch_materialization_engine.MaterializationJob
    :members:

.. autoclass:: feast.infra.materialization.batch_materialization_engine.MaterializationTask
    :members:

Local Engine
------------

.. autoclass:: feast.infra.materialization.local_engine.LocalMaterializationEngine
    :members:

.. autoclass:: feast.infra.materialization.local_engine.LocalMaterializationEngineConfig
    :members:

.. autoclass:: feast.infra.materialization.local_engine.LocalMaterializationJob
    :members:

Bytewax Engine
---------------------------

.. autoclass:: feast.infra.materialization.contrib.bytewax.bytewax_materialization_engine.BytewaxMaterializationEngine
    :members:

.. autoclass:: feast.infra.materialization.contrib.bytewax.bytewax_materialization_engine.BytewaxMaterializationEngineConfig
    :members:

.. autoclass:: feast.infra.materialization.contrib.bytewax.bytewax_materialization_job.BytewaxMaterializationJob
    :members:

Snowflake Engine
---------------------------

.. autoclass:: feast.infra.materialization.snowflake_engine.SnowflakeMaterializationEngine
    :members:

.. autoclass:: feast.infra.materialization.snowflake_engine.SnowflakeMaterializationEngineConfig
    :members:

.. autoclass:: feast.infra.materialization.snowflake_engine.SnowflakeMaterializationJob
    :members:

(Alpha) AWS Lambda Engine
---------------------------

.. autoclass:: feast.infra.materialization.aws_lambda.lambda_engine.LambdaMaterializationEngine
    :members:

.. autoclass:: feast.infra.materialization.aws_lambda.lambda_engine.LambdaMaterializationEngineConfig
    :members:

.. autoclass:: feast.infra.materialization.aws_lambda.lambda_engine.LambdaMaterializationJob
    :members:

(Alpha) Spark Engine
---------------------------

.. autoclass:: feast.infra.materialization.contrib.spark.spark_materialization_engine.SparkMaterializationEngine
    :members:

.. autoclass:: feast.infra.materialization.contrib.spark.spark_materialization_engine.SparkMaterializationEngineConfig
    :members:

.. autoclass:: feast.infra.materialization.contrib.spark.spark_materialization_engine.SparkMaterializationJob
    :members:

Permission
============================

.. autoclass:: feast.permissions.permission.Permission
    :members:

.. autoclass:: feast.permissions.action.AuthzedAction
    :members:

.. autoclass:: feast.permissions.policy.Policy
    :members:

.. autofunction:: feast.permissions.enforcer.enforce_policy

Auth Config
---------------------------

.. autoclass:: feast.permissions.auth_model.AuthConfig
    :members:

.. autoclass:: feast.permissions.auth_model.KubernetesAuthConfig
    :members:

.. autoclass:: feast.permissions.auth_model.OidcAuthConfig
    :members:

Auth Manager
---------------------------

.. autoclass:: feast.permissions.auth.AuthManager
    :members:

.. autoclass:: feast.permissions.auth.token_parser.TokenParser
    :members:

.. autoclass:: feast.permissions.auth.token_extractor.TokenExtractor
    :members:

.. autoclass:: feast.permissions.auth.kubernetes_token_parser.KubernetesTokenParser
    :members:

.. autoclass:: feast.permissions.auth.oidc_token_parser.OidcTokenParser
    :members:

Auth Client Manager
---------------------------

.. autoclass:: feast.permissions.client.auth_client_manager.AuthenticationClientManager
    :members:

.. autoclass:: feast.permissions.client.kubernetes_auth_client_manager.KubernetesAuthClientManager
    :members:

.. autoclass:: feast.permissions.client.oidc_authentication_client_manager.OidcAuthClientManager
    :members:
