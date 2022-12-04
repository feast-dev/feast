try:
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _version
except ModuleNotFoundError:
    from importlib_metadata import PackageNotFoundError, version as _version  # type: ignore

from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.offline_stores.contrib.athena_offline_store.athena_source import (
    AthenaSource,
)
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.redshift_source import RedshiftSource
from feast.infra.offline_stores.snowflake_source import SnowflakeSource

from .core import Entity, Feature
from .core.feature_views import (
    BatchFeatureView,
    FeatureView,
    OnDemandFeatureView,
    RequestFeatureView,
    StreamFeatureView,
)
from .data_source import KafkaSource, KinesisSource, PushSource, RequestSource
from .feature_service import FeatureService
from .feature_store import FeatureStore
from .field import Field
from .repo_config import RepoConfig
from .value_type import ValueType

try:
    __version__ = _version("feast")
except PackageNotFoundError:
    # package is not installed
    pass

__all__ = [
    "BatchFeatureView",
    "Entity",
    "KafkaSource",
    "KinesisSource",
    "Feature",
    "Field",
    "FeatureService",
    "FeatureStore",
    "FeatureView",
    "OnDemandFeatureView",
    "RepoConfig",
    "StreamFeatureView",
    "ValueType",
    "BigQuerySource",
    "FileSource",
    "RedshiftSource",
    "RequestFeatureView",
    "SnowflakeSource",
    "PushSource",
    "RequestSource",
    "AthenaSource",
]
