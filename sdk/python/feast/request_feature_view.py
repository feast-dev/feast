from typing import List, Type

from feast.base_feature_view import BaseFeatureView
from feast.data_source import RequestDataSource
from feast.feature import Feature
from feast.feature_view_projection import FeatureViewProjection
from feast.protos.feast.core.RequestFeatureView_pb2 import (
    RequestFeatureView as RequestFeatureViewProto,
)
from feast.protos.feast.core.RequestFeatureView_pb2 import RequestFeatureViewSpec
from feast.usage import log_exceptions


class RequestFeatureView(BaseFeatureView):
    """
    [Experimental] An RequestFeatureView defines a feature that is available from the inference request.

    Args:
        name: Name of the group of features.
        features: Output schema of transformation with feature names
        inputs: The input feature views passed into the transform.
        udf: User defined transformation function that takes as input pandas dataframes
    """

    request_data_source: RequestDataSource

    @log_exceptions
    def __init__(
        self, name: str, request_data_source: RequestDataSource,
    ):
        """
        Creates an RequestFeatureView object.
        """

        self._name = name
        self._features = [
            Feature(name=name, dtype=dtype)
            for name, dtype in request_data_source.schema.items()
        ]
        self._projection = FeatureViewProjection.from_definition(self)
        self.request_data_source = request_data_source

    @property
    def name(self) -> str:
        return self._name

    @property
    def features(self) -> List[Feature]:
        return self._features

    @features.setter
    def features(self, value):
        self._features = value

    @property
    def projection(self) -> FeatureViewProjection:
        return self._projection

    @projection.setter
    def projection(self, value):
        self._projection = value

    @property
    def proto_class(self) -> Type[RequestFeatureViewProto]:
        return RequestFeatureViewProto

    def to_proto(self) -> RequestFeatureViewProto:
        """
        Converts an request feature view object to its protobuf representation.

        Returns:
            A RequestFeatureViewProto protobuf.
        """
        spec = RequestFeatureViewSpec(
            name=self.name, request_data_source=self.request_data_source.to_proto()
        )

        return RequestFeatureViewProto(spec=spec)

    @classmethod
    def from_proto(cls, request_feature_view_proto: RequestFeatureViewProto):
        """
        Creates an on demand feature view from a protobuf representation.

        Args:
            request_feature_view_proto: A protobuf representation of an on-demand feature view.

        Returns:
            A RequestFeatureView object based on the on-demand feature view protobuf.
        """

        request_feature_view_obj = cls(
            name=request_feature_view_proto.spec.name,
            request_data_source=RequestDataSource.from_proto(
                request_feature_view_proto.spec.request_data_source
            ),
        )

        # FeatureViewProjections are not saved in the RequestFeatureView proto.
        # Create the default projection.
        request_feature_view_obj.projection = FeatureViewProjection.from_definition(
            request_feature_view_obj
        )

        return request_feature_view_obj
