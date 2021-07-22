import logging

from pkg_resources import DistributionNotFound, get_distribution

from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.redshift_source import RedshiftSource

from .client import Client
from .data_source import KafkaSource, KinesisSource, SourceType
from .entity import Entity
from .feature import Feature
from .feature_service import FeatureService
from .feature_store import FeatureStore
from .feature_table import FeatureTable
from .feature_view import FeatureView
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
    "Client",
    "Entity",
    "KafkaSource",
    "KinesisSource",
    "Feature",
    "FeatureService",
    "FeatureStore",
    "FeatureTable",
    "FeatureView",
    "RepoConfig",
    "SourceType",
    "ValueType",
    "BigQuerySource",
    "FileSource",
    "RedshiftSource",
]
