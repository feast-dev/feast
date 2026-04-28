from importlib import import_module
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _version

_LAZY_EXPORTS = {
    "Aggregation": ("feast.aggregation", "Aggregation"),
    "AthenaSource": (
        "feast.infra.offline_stores.contrib.athena_offline_store.athena_source",
        "AthenaSource",
    ),
    "BaseChunker": ("feast.chunker", "BaseChunker"),
    "BaseEmbedder": ("feast.embedder", "BaseEmbedder"),
    "BatchFeatureView": ("feast.batch_feature_view", "BatchFeatureView"),
    "BigQuerySource": ("feast.infra.offline_stores.bigquery_source", "BigQuerySource"),
    "ChrononSource": (
        "feast.infra.offline_stores.contrib.chronon_offline_store.chronon_source",
        "ChrononSource",
    ),
    "ChunkingConfig": ("feast.chunker", "ChunkingConfig"),
    "DataFrameEngine": ("feast.dataframe", "DataFrameEngine"),
    "DocEmbedder": ("feast.doc_embedder", "DocEmbedder"),
    "EmbeddingConfig": ("feast.embedder", "EmbeddingConfig"),
    "Entity": ("feast.entity", "Entity"),
    "Feature": ("feast.feature", "Feature"),
    "FeatureService": ("feast.feature_service", "FeatureService"),
    "FeatureStore": ("feast.feature_store", "FeatureStore"),
    "FeatureView": ("feast.feature_view", "FeatureView"),
    "FeastDataFrame": ("feast.dataframe", "FeastDataFrame"),
    "FeastVectorStore": ("feast.vector_store", "FeastVectorStore"),
    "Field": ("feast.field", "Field"),
    "FileSource": ("feast.infra.offline_stores.file_source", "FileSource"),
    "KafkaSource": ("feast.data_source", "KafkaSource"),
    "KinesisSource": ("feast.data_source", "KinesisSource"),
    "MultiModalEmbedder": ("feast.embedder", "MultiModalEmbedder"),
    "OnDemandFeatureView": ("feast.on_demand_feature_view", "OnDemandFeatureView"),
    "OracleSource": (
        "feast.infra.offline_stores.contrib.oracle_offline_store.oracle_source",
        "OracleSource",
    ),
    "Project": ("feast.project", "Project"),
    "PushSource": ("feast.data_source", "PushSource"),
    "RedshiftSource": ("feast.infra.offline_stores.redshift_source", "RedshiftSource"),
    "RepoConfig": ("feast.repo_config", "RepoConfig"),
    "RequestSource": ("feast.data_source", "RequestSource"),
    "SchemaTransformFn": ("feast.doc_embedder", "SchemaTransformFn"),
    "SnowflakeSource": (
        "feast.infra.offline_stores.snowflake_source",
        "SnowflakeSource",
    ),
    "StreamFeatureView": ("feast.stream_feature_view", "StreamFeatureView"),
    "TextChunker": ("feast.chunker", "TextChunker"),
    "ValueType": ("feast.value_type", "ValueType"),
}

__all__ = sorted(_LAZY_EXPORTS)


def __getattr__(name: str):
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module 'feast' has no attribute {name!r}")

    module_name, attr_name = _LAZY_EXPORTS[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


try:
    __version__ = _version("feast")
except PackageNotFoundError:
    pass
