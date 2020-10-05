from pkg_resources import DistributionNotFound, get_distribution

from .client import Client
from .data_source import (
    BigQueryOptions,
    DataSource,
    FileOptions,
    KafkaOptions,
    KinesisOptions,
    SourceType,
)
from .entity import Entity
from .feature import Feature
from .feature_table import FeatureTable
from .value_type import ValueType

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

__all__ = [
    "Client",
    "Entity",
    "DataSource",
    "BigQueryOptions",
    "FileOptions",
    "KafkaOptions",
    "KinesisOptions",
    "Feature",
    "FeatureTable",
    "SourceType",
    "ValueType",
]
