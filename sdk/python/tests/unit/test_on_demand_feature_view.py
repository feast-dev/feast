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

import pandas as pd
import pytest

from feast import RequestSource
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.on_demand_feature_view import OnDemandFeatureView, on_demand_feature_view
from feast.types import Float32, String, UnixTimestamp


def udf1(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["output1"] = features_df["feature1"]
    df["output2"] = features_df["feature2"]
    return df


def udf2(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["output1"] = features_df["feature1"] + 100
    df["output2"] = features_df["feature2"] + 100
    return df


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
        udf=udf1,
    )
    on_demand_feature_view_2 = OnDemandFeatureView(
        name="my-on-demand-feature-view",
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
        udf=udf1,
    )
    on_demand_feature_view_3 = OnDemandFeatureView(
        name="my-on-demand-feature-view",
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
        udf=udf2,
    )
    on_demand_feature_view_4 = OnDemandFeatureView(
        name="my-on-demand-feature-view",
        sources=sources,
        schema=[
            Field(name="output1", dtype=Float32),
            Field(name="output2", dtype=Float32),
        ],
        udf=udf2,
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


def test_inputs_parameter_deprecation_in_odfv():
    date_request = RequestSource(
        name="date_request", schema=[Field(name="some_date", dtype=UnixTimestamp)],
    )
    with pytest.warns(DeprecationWarning):

        @on_demand_feature_view(
            inputs={"date_request": date_request},
            schema=[
                Field(name="output", dtype=UnixTimestamp),
                Field(name="string_output", dtype=String),
            ],
        )
        def test_view(features_df: pd.DataFrame) -> pd.DataFrame:
            data = pd.DataFrame()
            data["output"] = features_df["some_date"]
            data["string_output"] = features_df["some_date"].astype(pd.StringDtype())
            return data

    odfv = test_view
    assert odfv.name == "test_view"
    assert len(odfv.source_request_sources) == 1
    assert odfv.source_request_sources["date_request"].name == "date_request"
    assert odfv.source_request_sources["date_request"].schema == date_request.schema

    with pytest.raises(ValueError):

        @on_demand_feature_view(
            inputs={"date_request": date_request},
            sources=[date_request],
            schema=[
                Field(name="output", dtype=UnixTimestamp),
                Field(name="string_output", dtype=String),
            ],
        )
        def incorrect_testview(features_df: pd.DataFrame) -> pd.DataFrame:
            data = pd.DataFrame()
            data["output"] = features_df["some_date"]
            data["string_output"] = features_df["some_date"].astype(pd.StringDtype())
            return data

    @on_demand_feature_view(
        inputs={"odfv": date_request},
        schema=[
            Field(name="output", dtype=UnixTimestamp),
            Field(name="string_output", dtype=String),
        ],
    )
    def test_correct_view(features_df: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data["output"] = features_df["some_date"]
        data["string_output"] = features_df["some_date"].astype(pd.StringDtype())
        return data

    odfv = test_correct_view
    assert odfv.name == "test_correct_view"
    assert odfv.source_request_sources["date_request"].schema == date_request.schema
