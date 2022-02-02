import copy
import functools
from types import MethodType
from typing import Dict, List, Type, Union

import dill
import pandas as pd

from feast.base_feature_view import BaseFeatureView
from feast.data_source import RequestDataSource
from feast.errors import RegistryInferenceFailure, SpecifiedFeaturesNotPresentError
from feast.feature import Feature
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureView as OnDemandFeatureViewProto,
)
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureViewMeta,
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


class OnDemandFeatureView(BaseFeatureView):
    """
    [Experimental] An OnDemandFeatureView defines on demand transformations on existing feature view values and request
    data.

    Args:
        name: Name of the group of features.
        features: Output schema of transformation with feature names
        inputs: The input feature views passed into the transform.
        udf: User defined transformation function that takes as input pandas dataframes
    """

    # TODO(adchia): remove inputs from proto and declaration
    input_feature_view_projections: Dict[str, FeatureViewProjection]
    input_request_data_sources: Dict[str, RequestDataSource]
    udf: MethodType

    @log_exceptions
    def __init__(
        self,
        name: str,
        features: List[Feature],
        inputs: Dict[str, Union[FeatureView, FeatureViewProjection, RequestDataSource]],
        udf: MethodType,
    ):
        """
        Creates an OnDemandFeatureView object.
        """
        super().__init__(name, features)
        self.input_feature_view_projections: Dict[str, FeatureViewProjection] = {}
        self.input_request_data_sources: Dict[str, RequestDataSource] = {}
        for input_ref, odfv_input in inputs.items():
            if isinstance(odfv_input, RequestDataSource):
                self.input_request_data_sources[input_ref] = odfv_input
            elif isinstance(odfv_input, FeatureViewProjection):
                self.input_feature_view_projections[input_ref] = odfv_input
            else:
                self.input_feature_view_projections[input_ref] = odfv_input.projection

        self.udf = udf

    @property
    def proto_class(self) -> Type[OnDemandFeatureViewProto]:
        return OnDemandFeatureViewProto

    def __copy__(self):
        fv = OnDemandFeatureView(
            name=self.name,
            features=self.features,
            inputs=dict(
                **self.input_feature_view_projections, **self.input_request_data_sources
            ),
            udf=self.udf,
        )
        fv.projection = copy.copy(self.projection)
        return fv

    def __eq__(self, other):
        if not super().__eq__(other):
            return False

        if (
            not self.input_feature_view_projections
            == other.input_feature_view_projections
        ):
            return False

        if not self.input_request_data_sources == other.input_request_data_sources:
            return False

        if not self.udf.__code__.co_code == other.udf.__code__.co_code:
            return False

        return True

    def __hash__(self):
        return super().__hash__()

    def to_proto(self) -> OnDemandFeatureViewProto:
        """
        Converts an on demand feature view object to its protobuf representation.

        Returns:
            A OnDemandFeatureViewProto protobuf.
        """
        meta = OnDemandFeatureViewMeta()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)
        if self.last_updated_timestamp:
            meta.last_updated_timestamp.FromDatetime(self.last_updated_timestamp)
        inputs = {}
        for input_ref, fv_projection in self.input_feature_view_projections.items():
            inputs[input_ref] = OnDemandInput(
                feature_view_projection=fv_projection.to_proto()
            )
        for input_ref, request_data_source in self.input_request_data_sources.items():
            inputs[input_ref] = OnDemandInput(
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

        return OnDemandFeatureViewProto(spec=spec, meta=meta)

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
                ).projection
            elif on_demand_input.WhichOneof("input") == "feature_view_projection":
                inputs[input_name] = FeatureViewProjection.from_proto(
                    on_demand_input.feature_view_projection
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

        # FeatureViewProjections are not saved in the OnDemandFeatureView proto.
        # Create the default projection.
        on_demand_feature_view_obj.projection = FeatureViewProjection.from_definition(
            on_demand_feature_view_obj
        )

        if on_demand_feature_view_proto.meta.HasField("created_timestamp"):
            on_demand_feature_view_obj.created_timestamp = (
                on_demand_feature_view_proto.meta.created_timestamp.ToDatetime()
            )
        if on_demand_feature_view_proto.meta.HasField("last_updated_timestamp"):
            on_demand_feature_view_obj.last_updated_timestamp = (
                on_demand_feature_view_proto.meta.last_updated_timestamp.ToDatetime()
            )

        return on_demand_feature_view_obj

    def get_request_data_schema(self) -> Dict[str, ValueType]:
        schema: Dict[str, ValueType] = {}
        for request_data_source in self.input_request_data_sources.values():
            schema.update(request_data_source.schema)
        return schema

    def get_transformed_features_df(
        self, df_with_features: pd.DataFrame, full_feature_names: bool = False,
    ) -> pd.DataFrame:
        # Apply on demand transformations
        columns_to_cleanup = []
        for input_fv_projection in self.input_feature_view_projections.values():
            for feature in input_fv_projection.features:
                full_feature_ref = f"{input_fv_projection.name}__{feature.name}"
                if full_feature_ref in df_with_features.keys():
                    # Make sure the partial feature name is always present
                    df_with_features[feature.name] = df_with_features[full_feature_ref]
                    columns_to_cleanup.append(feature.name)
                elif feature.name in df_with_features.keys():
                    # Make sure the full feature name is always present
                    df_with_features[full_feature_ref] = df_with_features[feature.name]
                    columns_to_cleanup.append(full_feature_ref)

        # Compute transformed values and apply to each result row
        df_with_transformed_features = self.udf.__call__(df_with_features)

        # Work out whether the correct columns names are used.
        rename_columns: Dict[str, str] = {}
        for feature in self.features:
            short_name = feature.name
            long_name = f"{self.projection.name_to_use()}__{feature.name}"
            if (
                short_name in df_with_transformed_features.columns
                and full_feature_names
            ):
                rename_columns[short_name] = long_name
            elif not full_feature_names:
                # Long name must be in dataframe.
                rename_columns[long_name] = short_name

        # Cleanup extra columns used for transformation
        df_with_features.drop(columns=columns_to_cleanup, inplace=True)
        return df_with_transformed_features.rename(columns=rename_columns)

    def infer_features(self):
        """
        Infers the set of features associated to this feature view from the input source.

        Raises:
            RegistryInferenceFailure: The set of features could not be inferred.
        """
        df = pd.DataFrame()
        for feature_view_projection in self.input_feature_view_projections.values():
            for feature in feature_view_projection.features:
                dtype = feast_value_type_to_pandas_type(feature.dtype)
                df[f"{feature_view_projection.name}__{feature.name}"] = pd.Series(
                    dtype=dtype
                )
                df[f"{feature.name}"] = pd.Series(dtype=dtype)
        for request_data in self.input_request_data_sources.values():
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
                raise SpecifiedFeaturesNotPresentError(
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


def on_demand_feature_view(
    features: List[Feature], inputs: Dict[str, Union[FeatureView, RequestDataSource]]
):
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
