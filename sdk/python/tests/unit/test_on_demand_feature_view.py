# Copyright 2022 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import Any, Dict, List

import pandas as pd
import pytest

from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.on_demand_feature_view import (
    OnDemandFeatureView,
    PandasTransformation,
    PythonTransformation,
)
from feast.types import Float32


def udf1(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["output1"] = features_df["feature1"]
    df["output2"] = features_df["feature2"]
    return df


def udf2(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["output1"] = features_df["feature1"] + 100
    df["output2"] = features_df["feature2"] + 101
    return df


def python_native_udf(features_dict: Dict[str, Any]) -> Dict[str, Any]:
    output_dict: Dict[str, List[Any]] = {
        "output1": features_dict["feature1"] + 100,
        "output2": features_dict["feature2"] + 101,
    }
    return output_dict


@pytest.mark.filterwarnings("ignore:udf and udf_string parameters are deprecated")
def test_hash():
    file_source = FileSource(name="my-file-source", path="test.parquet")
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=file_source,
    )
    sources = [feature_view]
    on_demand_feature_view_1 = OnDemandFeatureView(
        name="my-on-demand-feature-view",
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
        feature_transformation=PandasTransformation(
            udf=udf1, udf_string="udf1 source code"
        ),
    )
    on_demand_feature_view_2 = OnDemandFeatureView(
        name="my-on-demand-feature-view",
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
        feature_transformation=PandasTransformation(
            udf=udf1, udf_string="udf1 source code"
        ),
    )
    on_demand_feature_view_3 = OnDemandFeatureView(
        name="my-on-demand-feature-view",
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
        feature_transformation=PandasTransformation(
            udf=udf2, udf_string="udf2 source code"
        ),
    )
    on_demand_feature_view_4 = OnDemandFeatureView(
        name="my-on-demand-feature-view",
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
        feature_transformation=PandasTransformation(
            udf=udf2, udf_string="udf2 source code"
        ),
        description="test",
    )
    on_demand_feature_view_5 = OnDemandFeatureView(
        name="my-on-demand-feature-view",
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
        feature_transformation=PandasTransformation(
            udf=udf2, udf_string="udf2 source code"
        ),
        description="test",
    )

    s1 = {on_demand_feature_view_1, on_demand_feature_view_2}
    assert len(s1) == 1

    s2 = {on_demand_feature_view_1, on_demand_feature_view_3}
    assert len(s2) == 2

    s3 = {on_demand_feature_view_3, on_demand_feature_view_4}
    assert len(s3) == 2

    s4 = {
        on_demand_feature_view_1,
        on_demand_feature_view_2,
        on_demand_feature_view_3,
        on_demand_feature_view_4,
    }
    assert len(s4) == 3

    assert on_demand_feature_view_5.feature_transformation == PandasTransformation(
        udf2, "udf2 source code"
    )


def test_python_native_transformation_mode():
    file_source = FileSource(name="my-file-source", path="test.parquet")
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=file_source,
    )
    sources = [feature_view]

    on_demand_feature_view_python_native = OnDemandFeatureView(
        name="my-on-demand-feature-view",
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
        feature_transformation=PythonTransformation(
            udf=python_native_udf, udf_string="python native udf source code"
        ),
        description="test",
        mode="python",
    )

    on_demand_feature_view_python_native_err = OnDemandFeatureView(
        name="my-on-demand-feature-view",
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
        feature_transformation=PandasTransformation(
            udf=python_native_udf, udf_string="python native udf source code"
        ),
        description="test",
        mode="python",
    )

    assert (
        on_demand_feature_view_python_native.feature_transformation
        == PythonTransformation(python_native_udf, "python native udf source code")
    )

    with pytest.raises(TypeError):
        assert (
            on_demand_feature_view_python_native_err.feature_transformation
            == PythonTransformation(python_native_udf, "python native udf source code")
        )

    assert on_demand_feature_view_python_native.transform_dict(
        {
            "feature1": 0,
            "feature2": 1,
        }
    ) == {"feature1": 0, "feature2": 1, "output1": 100, "output2": 102}


@pytest.mark.filterwarnings("ignore:udf and udf_string parameters are deprecated")
def test_from_proto_backwards_compatible_udf():
    file_source = FileSource(name="my-file-source", path="test.parquet")
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=file_source,
    )
    sources = [feature_view]
    on_demand_feature_view = OnDemandFeatureView(
        name="my-on-demand-feature-view",
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
        feature_transformation=PandasTransformation(
            udf=udf1, udf_string="udf1 source code"
        ),
    )

    # We need a proto with the "udf1 source code" in the user_defined_function.body_text
    # and to populate it in feature_transformation
    proto = on_demand_feature_view.to_proto()
    assert (
        on_demand_feature_view.feature_transformation.udf_string
        == proto.spec.feature_transformation.user_defined_function.body_text
    )
    # Because of the current set of code this is just confirming it is empty
    assert proto.spec.user_defined_function.body_text == ""
    assert proto.spec.user_defined_function.body == b""
    assert proto.spec.user_defined_function.name == ""

    # Assuming we pull it from the registry we set it to the feature_transformation proto values
    proto.spec.user_defined_function.name = (
        proto.spec.feature_transformation.user_defined_function.name
    )
    proto.spec.user_defined_function.body = (
        proto.spec.feature_transformation.user_defined_function.body
    )
    proto.spec.user_defined_function.body_text = (
        proto.spec.feature_transformation.user_defined_function.body_text
    )

    # And now we're going to null the feature_transformation proto object before reserializing the entire proto
    # proto.spec.user_defined_function.body_text = on_demand_feature_view.transformation.udf_string
    proto.spec.feature_transformation.user_defined_function.name = ""
    proto.spec.feature_transformation.user_defined_function.body = b""
    proto.spec.feature_transformation.user_defined_function.body_text = ""

    # And now we expect the to get the same object back under feature_transformation
    reserialized_proto = OnDemandFeatureView.from_proto(proto)
    assert (
        reserialized_proto.feature_transformation.udf_string
        == on_demand_feature_view.feature_transformation.udf_string
    )
