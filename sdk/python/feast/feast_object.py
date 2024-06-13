from typing import Union

from .batch_feature_view import BatchFeatureView
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
from .protos.feast.core.StreamFeatureView_pb2 import StreamFeatureViewSpec
from .protos.feast.core.ValidationProfile_pb2 import (
    ValidationReference as ValidationReferenceProto,
)
from .saved_dataset import ValidationReference
from .stream_feature_view import StreamFeatureView

# Convenience type representing all Feast objects
FeastObject = Union[
    FeatureView,
    OnDemandFeatureView,
    BatchFeatureView,
    StreamFeatureView,
    Entity,
    FeatureService,
    DataSource,
    ValidationReference,
]

FeastObjectSpecProto = Union[
    FeatureViewSpec,
    OnDemandFeatureViewSpec,
    StreamFeatureViewSpec,
    EntitySpecV2,
    FeatureServiceSpec,
    DataSourceProto,
    ValidationReferenceProto,
]
