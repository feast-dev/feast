from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _version
from typing import Any

from feast.demos import copy_demo_notebooks
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.offline_stores.contrib.athena_offline_store.athena_source import (
    AthenaSource,
)
from feast.infra.offline_stores.contrib.oracle_offline_store.oracle_source import (
    OracleSource,
)
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.redshift_source import RedshiftSource
from feast.infra.offline_stores.snowflake_source import SnowflakeSource

from .aggregation import Aggregation
from .batch_feature_view import BatchFeatureView
from .chunker import BaseChunker, ChunkingConfig, TextChunker
from .data_source import KafkaSource, KinesisSource, PushSource, RequestSource
from .dataframe import DataFrameEngine, FeastDataFrame
from .doc_embedder import DocEmbedder, SchemaTransformFn
from .embedder import (
    BaseEmbedder,
    EmbeddingConfig,
    EmbeddingProvider,
    MultiModalEmbedder,
    SentenceTransformersEmbeddingProvider,
    get_embedding_provider,
)
from .entity import Entity
from .feature import Feature
from .feature_service import FeatureService
from .feature_store import FeatureStore
from .feature_view import FeatureView, FeatureViewState
from .field import Field
from .filter_models import FilterTranslator
from .labeling import ConflictPolicy, LabelView
from .on_demand_feature_view import OnDemandFeatureView
from .project import Project
from .repo_config import RepoConfig
from .stream_feature_view import StreamFeatureView
from .value_type import ValueType
from .vector_store import FeastVectorStore

try:
    __version__ = _version("feast")
except PackageNotFoundError:
    # package is not installed
    pass

__all__ = [
    "Aggregation",
    "BatchFeatureView",
    "copy_demo_notebooks",
    "DataFrameEngine",
    "Entity",
    "KafkaSource",
    "KinesisSource",
    "FeastDataFrame",
    "Feature",
    "Field",
    "FeatureService",
    "FeatureStore",
    "FeatureView",
    "FeatureViewState",
    "LabelView",
    "ConflictPolicy",
    "OnDemandFeatureView",
    "RepoConfig",
    "StreamFeatureView",
    "ValueType",
    "BigQuerySource",
    "FileSource",
    "RedshiftSource",
    "SnowflakeSource",
    "PushSource",
    "RequestSource",
    "MlflowDatasetSource",
    "AthenaSource",
    "OracleSource",
    "Project",
    "FeastVectorStore",
    "DocEmbedder",
    "SchemaTransformFn",
    "BaseChunker",
    "TextChunker",
    "ChunkingConfig",
    "BaseEmbedder",
    "MultiModalEmbedder",
    "EmbeddingConfig",
    "EmbeddingProvider",
    "SentenceTransformersEmbeddingProvider",
    "get_embedding_provider",
    "FilterTranslator",
]


def __getattr__(name: str) -> Any:
    # Lazy export: importing MlflowDatasetSource during feast package init
    # re-enters feast.data_source → type_map while __init__ is incomplete and
    # can fail with ModuleNotFoundError for google.protobuf.* in CLI subprocesses
    # (e.g. macOS CI ``feast init`` / test_parse_repo).
    if name == "MlflowDatasetSource":
        from feast.infra.data_sources.mlflow import MlflowDatasetSource

        return MlflowDatasetSource
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
