from typing import Union

from .data_source import DataSource
from .entity import Entity
from .feature_service import FeatureService
from .feature_view import FeatureView
from .on_demand_feature_view import OnDemandFeatureView
from .protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from .protos.feast.core.Entity_pb2 import EntitySpecV2
from .protos.feast.core.FeatureService_pb2 import FeatureServiceSpec
from .protos.feast.core.FeatureView_pb2 import FeatureViewSpec
from .protos.feast.core.OnDemandFeatureView_pb2 import OnDemandFeatureViewSpec
from .protos.feast.core.RequestFeatureView_pb2 import RequestFeatureViewSpec
from .protos.feast.core.ValidationProfile_pb2 import (
    ValidationReference as ValidationReferenceProto,
)
from .request_feature_view import RequestFeatureView
from .saved_dataset import ValidationReference

# Convenience type representing all Feast objects
FeastObject = Union[
    FeatureView,
    OnDemandFeatureView,
    RequestFeatureView,
    Entity,
    FeatureService,
    DataSource,
    ValidationReference,
]

FeastObjectSpecProto = Union[
    FeatureViewSpec,
    OnDemandFeatureViewSpec,
    RequestFeatureViewSpec,
    EntitySpecV2,
    FeatureServiceSpec,
    DataSourceProto,
    ValidationReferenceProto,
]
