from pkg_resources import DistributionNotFound, get_distribution

from .client import Client
from .entity import Entity
from .feature import Feature
from .feature_set import FeatureSet
from .source import KafkaSource, Source
from .value_type import ValueType

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

__all__ = [
    "Client",
    "Entity",
    "Feature",
    "FeatureSet",
    "Source",
    "KafkaSource",
    "ValueType",
]
