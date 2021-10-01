Feast Python API Documentation
=============================


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
==================

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

Provider
==================

.. automodule:: feast.infra.provider
    :inherited-members:
    :members:

Passthrough Provider
------------------

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
------------------

.. automodule:: feast.infra.offline_stores.bigquery
    :members:

Redshift Offline Store
------------------

.. automodule:: feast.infra.offline_stores.redshift
    :members:

Online Store
==================

.. automodule:: feast.infra.online_stores.online_store
    :inherited-members:
    :members:

Sqlite Online Store
------------------

.. automodule:: feast.infra.online_stores.sqlite
    :members:

Datastore Online Store
------------------

.. automodule:: feast.infra.online_stores.datastore
    :members:

DynamoDB Online Store
------------------

.. automodule:: feast.infra.online_stores.dynamodb
    :members:

Redis Online Store
------------------

.. automodule:: feast.infra.online_stores.redis
    :members: