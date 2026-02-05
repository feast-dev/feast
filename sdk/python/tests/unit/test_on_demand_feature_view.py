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
import datetime
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
    on_demand_feature_view,
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


def python_writes_test_udf(features_dict: Dict[str, Any]) -> Dict[str, Any]:
    output_dict: Dict[str, List[Any]] = {
        "output1": features_dict["feature1"] + 100,
        "output2": features_dict["feature2"] + 101,
        "output3": datetime.datetime.now(),
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
        udf2, udf_string="udf2 source code"
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

    assert (
        on_demand_feature_view_python_native.feature_transformation
        == PythonTransformation(python_native_udf, "python native udf source code")
    )

    with pytest.raises(TypeError):
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

    # For objects that are already registered, feature_transformation and mode is not set
    proto.spec.feature_transformation.Clear()
    proto.spec.ClearField("mode")

    # And now we expect the to get the same object back under feature_transformation
    reserialized_proto = OnDemandFeatureView.from_proto(proto)
    assert (
        reserialized_proto.feature_transformation.udf_string
        == on_demand_feature_view.feature_transformation.udf_string
    )


def test_on_demand_feature_view_writes_protos():
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
        write_to_online_store=True,
    )

    proto = on_demand_feature_view.to_proto()
    reserialized_proto = OnDemandFeatureView.from_proto(proto)

    assert on_demand_feature_view.write_to_online_store
    assert proto.spec.write_to_online_store
    assert reserialized_proto.write_to_online_store

    proto.spec.write_to_online_store = False
    reserialized_proto = OnDemandFeatureView.from_proto(proto)
    assert not reserialized_proto.write_to_online_store


def test_on_demand_feature_view_stored_writes():
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
        feature_transformation=PythonTransformation(
            udf=python_writes_test_udf, udf_string="python native udf source code"
        ),
        description="testing on demand feature view stored writes",
        mode="python",
        write_to_online_store=True,
    )

    transformed_output = on_demand_feature_view.transform_dict(
        {
            "feature1": 0,
            "feature2": 1,
        }
    )
    expected_output = {"feature1": 0, "feature2": 1, "output1": 100, "output2": 102}
    keys_to_validate = [
        "feature1",
        "feature2",
        "output1",
        "output2",
    ]
    for k in keys_to_validate:
        assert transformed_output[k] == expected_output[k]

    assert transformed_output["output3"] is not None and isinstance(
        transformed_output["output3"], datetime.datetime
    )


def test_function_call_syntax():
    CUSTOM_FUNCTION_NAME = "custom-function-name"
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

    def transform_features(features_df: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame()
        df["output1"] = features_df["feature1"]
        df["output2"] = features_df["feature2"]
        return df

    odfv = on_demand_feature_view(
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
    )(transform_features)

    assert odfv.name == transform_features.__name__
    assert isinstance(odfv, OnDemandFeatureView)

    proto = odfv.to_proto()
    assert proto.spec.name == transform_features.__name__

    deserialized = OnDemandFeatureView.from_proto(proto)
    assert deserialized.name == transform_features.__name__

    def another_transform(features_df: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame()
        df["output1"] = features_df["feature1"]
        df["output2"] = features_df["feature2"]
        return df

    odfv_custom = on_demand_feature_view(
        name=CUSTOM_FUNCTION_NAME,
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
    )(another_transform)

    assert odfv_custom.name == CUSTOM_FUNCTION_NAME
    assert isinstance(odfv_custom, OnDemandFeatureView)

    proto = odfv_custom.to_proto()
    assert proto.spec.name == CUSTOM_FUNCTION_NAME

    deserialized = OnDemandFeatureView.from_proto(proto)
    assert deserialized.name == CUSTOM_FUNCTION_NAME


def test_on_demand_feature_view_without_transformation():
    """Test that OnDemandFeatureView can be created without a UDF or transformation."""
    from feast.data_source import RequestSource
    from feast.types import Float32, String

    # Create a request source
    request_source = RequestSource(
        name="test_request",
        schema=[
            Field(name="user_id", dtype=String),
            Field(name="value", dtype=Float32),
        ],
    )

    # Test 1: Create ODFV without transformation
    odfv_no_transform = OnDemandFeatureView(
        name="test_odfv_no_transform",
        sources=[request_source],
        schema=[
            Field(name="output_feature", dtype=Float32),
        ],
        mode="pandas",
    )

    assert odfv_no_transform.feature_transformation is None
    assert odfv_no_transform.name == "test_odfv_no_transform"
    assert len(odfv_no_transform.features) == 1

    # Test 2: Verify transform_arrow returns table as-is
    import pyarrow as pa

    test_table = pa.table({
        "user_id": ["user1", "user2"],
        "value": [1.0, 2.0],
    })

    result = odfv_no_transform.transform_arrow(test_table)
    assert result == test_table

    # Test 3: Verify transform_dict returns dict as-is
    test_dict = {
        "user_id": ["user1", "user2"],
        "value": [1.0, 2.0],
    }

    result = odfv_no_transform.transform_dict(test_dict)
    assert result == test_dict

    # Test 4: Test serialization/deserialization
    proto = odfv_no_transform.to_proto()
    assert proto.spec.name == "test_odfv_no_transform"

    deserialized = OnDemandFeatureView.from_proto(proto)
    assert deserialized.name == "test_odfv_no_transform"
    assert deserialized.feature_transformation is None

    # Test 5: Verify that ODFVs with transformations still work
    def simple_transform(features_df: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame()
        df["doubled_value"] = features_df["value"] * 2
        return df

    odfv_with_transform = OnDemandFeatureView(
        name="test_odfv_with_transform",
        sources=[request_source],
        schema=[
            Field(name="doubled_value", dtype=Float32),
        ],
        udf=simple_transform,
        mode="pandas",
    )

    assert odfv_with_transform.feature_transformation is not None
    assert isinstance(odfv_with_transform.feature_transformation, PandasTransformation)
