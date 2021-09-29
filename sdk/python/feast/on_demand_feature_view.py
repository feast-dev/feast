import functools
from types import MethodType
from typing import Dict, List, Union, cast

import dill
import pandas as pd

from feast import errors
from feast.data_source import RequestDataSource
from feast.errors import RegistryInferenceFailure
from feast.feature import Feature
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureView as OnDemandFeatureViewProto,
)
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureViewSpec,
    OnDemandInput,
)
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    UserDefinedFunction as UserDefinedFunctionProto,
)
from feast.type_map import (
    feast_value_type_to_pandas_type,
    python_type_to_feast_value_type,
)
from feast.usage import log_exceptions
from feast.value_type import ValueType


class OnDemandFeatureView:
    """
    [Experimental] An OnDemandFeatureView defines on demand transformations on existing feature view values and request
    data.

    Args:
        name: Name of the group of features.
        features: Output schema of transformation with feature names
        inputs: The input feature views passed into the transform.
        udf: User defined transformation function that takes as input pandas dataframes
    """

    name: str
    features: List[Feature]
    inputs: Dict[str, Union[FeatureView, RequestDataSource]]
    udf: MethodType

    @log_exceptions
    def __init__(
        self,
        name: str,
        features: List[Feature],
        inputs: Dict[str, Union[FeatureView, RequestDataSource]],
        udf: MethodType,
    ):
        """
        Creates an OnDemandFeatureView object.
        """

        self.name = name
        self.features = features
        self.inputs = inputs
        self.udf = udf

    def to_proto(self) -> OnDemandFeatureViewProto:
        """
        Converts an on demand feature view object to its protobuf representation.

        Returns:
            A OnDemandFeatureViewProto protobuf.
        """
        inputs = {}
        for feature_ref, input in self.inputs.items():
            if type(input) == FeatureView:
                fv = cast(FeatureView, input)
                inputs[feature_ref] = OnDemandInput(feature_view=fv.to_proto())
            else:
                request_data_source = cast(RequestDataSource, input)
                inputs[feature_ref] = OnDemandInput(
                    request_data_source=request_data_source.to_proto()
                )

        spec = OnDemandFeatureViewSpec(
            name=self.name,
            features=[feature.to_proto() for feature in self.features],
            inputs=inputs,
            user_defined_function=UserDefinedFunctionProto(
                name=self.udf.__name__, body=dill.dumps(self.udf, recurse=True),
            ),
        )

        return OnDemandFeatureViewProto(spec=spec)

    @classmethod
    def from_proto(cls, on_demand_feature_view_proto: OnDemandFeatureViewProto):
        """
        Creates an on demand feature view from a protobuf representation.

        Args:
            on_demand_feature_view_proto: A protobuf representation of an on-demand feature view.

        Returns:
            A OnDemandFeatureView object based on the on-demand feature view protobuf.
        """
        inputs = {}
        for (
            input_name,
            on_demand_input,
        ) in on_demand_feature_view_proto.spec.inputs.items():
            if on_demand_input.WhichOneof("input") == "feature_view":
                inputs[input_name] = FeatureView.from_proto(
                    on_demand_input.feature_view
                )
            else:
                inputs[input_name] = RequestDataSource.from_proto(
                    on_demand_input.request_data_source
                )
        on_demand_feature_view_obj = cls(
            name=on_demand_feature_view_proto.spec.name,
            features=[
                Feature(
                    name=feature.name,
                    dtype=ValueType(feature.value_type),
                    labels=dict(feature.labels),
                )
                for feature in on_demand_feature_view_proto.spec.features
            ],
            inputs=inputs,
            udf=dill.loads(
                on_demand_feature_view_proto.spec.user_defined_function.body
            ),
        )

        return on_demand_feature_view_obj

    def get_transformed_features_df(
        self, full_feature_names: bool, df_with_features: pd.DataFrame
    ) -> pd.DataFrame:
        # Apply on demand transformations
        # TODO(adchia): Include only the feature values from the specified input FVs in the ODFV.
        # Copy over un-prefixed features even if not requested since transform may need it
        columns_to_cleanup = []
        if full_feature_names:
            for input in self.inputs.values():
                if type(input) != FeatureView:
                    continue
                input_fv = cast(FeatureView, input)
                for feature in input_fv.features:
                    full_feature_ref = f"{input_fv.name}__{feature.name}"
                    if full_feature_ref in df_with_features.keys():
                        df_with_features[feature.name] = df_with_features[
                            full_feature_ref
                        ]
                        columns_to_cleanup.append(feature.name)

        # Compute transformed values and apply to each result row
        df_with_transformed_features = self.udf.__call__(df_with_features)

        # Cleanup extra columns used for transformation
        df_with_features.drop(columns=columns_to_cleanup, inplace=True)
        return df_with_transformed_features

    def __getitem__(self, item) -> FeatureViewProjection:
        assert isinstance(item, list)

        referenced_features = []
        for feature in self.features:
            if feature.name in item:
                referenced_features.append(feature)

        return FeatureViewProjection(self.name, referenced_features)

    def infer_features(self):
        """
        Infers the set of features associated to this feature view from the input source.

        Args:
            config: Configuration object used to configure the feature store.

        Raises:
            RegistryInferenceFailure: The set of features could not be inferred.
        """
        df = pd.DataFrame()
        for input in self.inputs.values():
            if type(input) == FeatureView:
                feature_view = cast(FeatureView, input)
                for feature in feature_view.features:
                    dtype = feast_value_type_to_pandas_type(feature.dtype)
                    df[f"{feature_view.name}__{feature.name}"] = pd.Series(dtype=dtype)
                    df[f"{feature.name}"] = pd.Series(dtype=dtype)
            else:
                request_data = cast(RequestDataSource, input)
                for feature_name, feature_type in request_data.schema.items():
                    dtype = feast_value_type_to_pandas_type(feature_type)
                    df[f"{feature_name}"] = pd.Series(dtype=dtype)
        output_df: pd.DataFrame = self.udf.__call__(df)
        inferred_features = []
        for f, dt in zip(output_df.columns, output_df.dtypes):
            inferred_features.append(
                Feature(
                    name=f, dtype=python_type_to_feast_value_type(f, type_name=str(dt))
                )
            )

        if self.features:
            missing_features = []
            for specified_features in self.features:
                if specified_features not in inferred_features:
                    missing_features.append(specified_features)
            if missing_features:
                raise errors.SpecifiedFeaturesNotPresentError(
                    [f.name for f in missing_features], self.name
                )
        else:
            self.features = inferred_features

        if not self.features:
            raise RegistryInferenceFailure(
                "OnDemandFeatureView",
                f"Could not infer Features for the feature view '{self.name}'.",
            )

    @staticmethod
    def get_requested_odfvs(feature_refs, project, registry):
        all_on_demand_feature_views = registry.list_on_demand_feature_views(
            project, allow_cache=True
        )
        requested_on_demand_feature_views: List[OnDemandFeatureView] = []
        for odfv in all_on_demand_feature_views:
            for feature in odfv.features:
                if f"{odfv.name}:{feature.name}" in feature_refs:
                    requested_on_demand_feature_views.append(odfv)
                    break
        return requested_on_demand_feature_views


def on_demand_feature_view(features: List[Feature], inputs: Dict[str, FeatureView]):
    """
    Declare an on-demand feature view

    :param features: Output schema with feature names
    :param inputs: The inputs passed into the transform.
    :return: An On Demand Feature View.
    """

    def decorator(user_function):
        on_demand_feature_view_obj = OnDemandFeatureView(
            name=user_function.__name__,
            inputs=inputs,
            features=features,
            udf=user_function,
        )
        functools.update_wrapper(
            wrapper=on_demand_feature_view_obj, wrapped=user_function
        )
        return on_demand_feature_view_obj

    return decorator
