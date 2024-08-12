from typing import Union, get_args

from .batch_feature_view import BatchFeatureView
from .data_source import DataSource
from .entity import Entity
from .feature_service import FeatureService
from .feature_view import FeatureView
from .on_demand_feature_view import OnDemandFeatureView
from .permissions.permission import Permission
from .protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from .protos.feast.core.Entity_pb2 import EntitySpecV2
from .protos.feast.core.FeatureService_pb2 import FeatureServiceSpec
from .protos.feast.core.FeatureView_pb2 import FeatureViewSpec
from .protos.feast.core.OnDemandFeatureView_pb2 import OnDemandFeatureViewSpec
from .protos.feast.core.Permission_pb2 import Permission as PermissionProto
from .protos.feast.core.SavedDataset_pb2 import SavedDatasetSpec
from .protos.feast.core.StreamFeatureView_pb2 import StreamFeatureViewSpec
from .protos.feast.core.ValidationProfile_pb2 import (
    ValidationReference as ValidationReferenceProto,
)
from .saved_dataset import SavedDataset, ValidationReference
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
    SavedDataset,
    Permission,
]

FeastObjectSpecProto = Union[
    FeatureViewSpec,
    OnDemandFeatureViewSpec,
    StreamFeatureViewSpec,
    EntitySpecV2,
    FeatureServiceSpec,
    DataSourceProto,
    ValidationReferenceProto,
    SavedDatasetSpec,
    PermissionProto,
]

ALL_RESOURCE_TYPES = list(get_args(FeastObject))
ALL_FEATURE_VIEW_TYPES = [
    FeatureView,
    OnDemandFeatureView,
    BatchFeatureView,
    StreamFeatureView,
]
