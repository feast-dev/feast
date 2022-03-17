from typing import Union

from .data_source import DataSource
from .entity import Entity
from .feature_service import FeatureService
from .feature_view import FeatureView
from .on_demand_feature_view import OnDemandFeatureView
from .proto_core.DataSource_pb2 import DataSource as DataSourceProto
from .proto_core.Entity_pb2 import EntitySpecV2
from .proto_core.FeatureService_pb2 import FeatureServiceSpec
from .proto_core.FeatureView_pb2 import FeatureViewSpec
from .proto_core.OnDemandFeatureView_pb2 import OnDemandFeatureViewSpec
from .proto_core.RequestFeatureView_pb2 import RequestFeatureViewSpec
from .request_feature_view import RequestFeatureView

# Convenience type representing all Feast objects
FeastObject = Union[
    FeatureView,
    OnDemandFeatureView,
    RequestFeatureView,
    Entity,
    FeatureService,
    DataSource,
]

FeastObjectSpecProto = Union[
    FeatureViewSpec,
    OnDemandFeatureViewSpec,
    RequestFeatureViewSpec,
    EntitySpecV2,
    FeatureServiceSpec,
    DataSourceProto,
]
