from typing import Union

from .entity import Entity
from .feature_service import FeatureService
from .feature_view import FeatureView
from .on_demand_feature_view import OnDemandFeatureView
from .request_feature_view import RequestFeatureView

# Convenience type representing all Feast objects
FeastObject = Union[
    FeatureView, OnDemandFeatureView, RequestFeatureView, Entity, FeatureService
]
