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

from feast.aggregation import Aggregation
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


def test_track_metrics_defaults_to_false():
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
    odfv = OnDemandFeatureView(
        name="metrics-default-odfv",
        sources=[feature_view],
        schema=[Field(name="output1", dtype=Float32)],
        feature_transformation=PandasTransformation(
            udf=udf1, udf_string="udf1 source code"
        ),
    )
    assert odfv.track_metrics is False


def test_track_metrics_true_persists_via_proto():
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
    odfv = OnDemandFeatureView(
        name="tracked-metrics-odfv",
        sources=[feature_view],
        schema=[Field(name="output1", dtype=Float32)],
        feature_transformation=PandasTransformation(
            udf=udf1, udf_string="udf1 source code"
        ),
        track_metrics=True,
    )
    assert odfv.track_metrics is True

    proto = odfv.to_proto()
    assert proto.spec.tags.get("feast:track_metrics") == "true"

    restored = OnDemandFeatureView.from_proto(proto)
    assert restored.track_metrics is True
    assert "feast:track_metrics" not in restored.tags, (
        "Internal feast:track_metrics tag leaked into user-facing self.tags "
        "after proto round-trip"
    )


def test_track_metrics_proto_roundtrip_preserves_user_tags():
    """User tags must survive a proto round-trip without internal tag pollution."""
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
    user_tags = {"team": "ml-platform", "priority": "high"}
    odfv = OnDemandFeatureView(
        name="tagged-odfv",
        sources=[feature_view],
        schema=[Field(name="output1", dtype=Float32)],
        feature_transformation=PandasTransformation(
            udf=udf1, udf_string="udf1 source code"
        ),
        tags=user_tags,
        track_metrics=True,
    )
    assert odfv.tags == user_tags

    proto = odfv.to_proto()
    restored = OnDemandFeatureView.from_proto(proto)

    assert restored.tags == user_tags
    assert restored.track_metrics is True


def test_track_metrics_false_not_stored_in_tags():
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
    odfv = OnDemandFeatureView(
        name="no-metrics-odfv",
        sources=[feature_view],
        schema=[Field(name="output1", dtype=Float32)],
        feature_transformation=PandasTransformation(
            udf=udf1, udf_string="udf1 source code"
        ),
        track_metrics=False,
    )
    proto = odfv.to_proto()
    assert "feast:track_metrics" not in proto.spec.tags

    restored = OnDemandFeatureView.from_proto(proto)
    assert restored.track_metrics is False


def test_copy_preserves_track_metrics():
    """__copy__ must carry track_metrics so FeatureService projections keep timing enabled."""
    import copy

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
    odfv = OnDemandFeatureView(
        name="tracked-odfv",
        sources=[feature_view],
        schema=[Field(name="output1", dtype=Float32)],
        feature_transformation=PandasTransformation(
            udf=udf1, udf_string="udf1 source code"
        ),
        track_metrics=True,
    )
    assert odfv.track_metrics is True

    copied = copy.copy(odfv)
    assert copied.track_metrics is True, (
        "__copy__ lost track_metrics; ODFV timing metrics will be silently disabled "
        "when using FeatureService projections"
    )


def test_eq_considers_track_metrics():
    """__eq__ must distinguish ODFVs that differ only in track_metrics."""
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
    common = dict(
        name="eq-odfv",
        sources=[feature_view],
        schema=[Field(name="output1", dtype=Float32)],
        feature_transformation=PandasTransformation(
            udf=udf1, udf_string="udf1 source code"
        ),
    )
    odfv_tracked = OnDemandFeatureView(**common, track_metrics=True)
    odfv_untracked = OnDemandFeatureView(**common, track_metrics=False)

    assert odfv_tracked != odfv_untracked


def test_aggregations_valid_without_udf():
    """An ODFV with aggregations must pass ensure_valid() without a udf or feature_transformation."""
    from datetime import timedelta

    file_source = FileSource(name="my-file-source", path="test.parquet")
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[Field(name="purchase_count", dtype=Float32)],
        source=file_source,
    )
    odfv = OnDemandFeatureView(
        name="agg-odfv",
        sources=[feature_view],
        schema=[Field(name="purchase_sum_30d", dtype=Float32)],
        aggregations=[
            Aggregation(
                column="purchase_count",
                function="sum",
                time_window=timedelta(days=30),
            )
        ],
    )
    # Must not raise
    odfv.ensure_valid()


def test_aggregations_only_odfv_proto_roundtrip():
    """Aggregation-only ODFV must survive a proto round-trip without crashing on dill.loads(b'')."""
    from datetime import timedelta

    file_source = FileSource(name="my-file-source", path="test.parquet")
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[Field(name="purchase_count", dtype=Float32)],
        source=file_source,
    )
    odfv = OnDemandFeatureView(
        name="agg-odfv",
        sources=[feature_view],
        schema=[Field(name="purchase_sum_30d", dtype=Float32)],
        aggregations=[
            Aggregation(
                column="purchase_count",
                function="sum",
                time_window=timedelta(days=30),
            )
        ],
    )
    proto = odfv.to_proto()
    restored = OnDemandFeatureView.from_proto(proto)
    assert restored.feature_transformation is None
    assert len(restored.aggregations) == 1
    assert restored.aggregations[0].column == "purchase_count"
