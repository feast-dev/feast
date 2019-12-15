from pkg_resources import get_distribution, DistributionNotFound
try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

from .client import Client
from .entity import Entity
from .feature_set import FeatureSet
from .feature import Feature
from .source import Source, KafkaSource
from .value_type import ValueType
