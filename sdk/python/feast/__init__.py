import logging

from pkg_resources import DistributionNotFound, get_distribution

from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.redshift_source import RedshiftSource
from feast.infra.offline_stores.snowflake_source import SnowflakeSource

from .data_source import KafkaSource, KinesisSource, SourceType
from .entity import Entity
from .feature import Feature
from .feature_service import FeatureService
from .feature_store import FeatureStore
from .feature_view import FeatureView
from .on_demand_feature_view import OnDemandFeatureView
from .repo_config import RepoConfig
from .value_type import ValueType

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=logging.INFO,
)

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

__all__ = [
    "Entity",
    "KafkaSource",
    "KinesisSource",
    "Feature",
    "FeatureService",
    "FeatureStore",
    "FeatureView",
    "OnDemandFeatureView",
    "RepoConfig",
    "SourceType",
    "ValueType",
    "BigQuerySource",
    "FileSource",
    "RedshiftSource",
    "SnowflakeSource",
]
