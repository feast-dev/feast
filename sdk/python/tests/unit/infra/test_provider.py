# Copyright 2020 The Feast Authors
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

from datetime import timedelta

from feast import BigQuerySource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.types import String
from feast.utils import _get_column_names


def test_get_column_names_preserves_feature_ordering():
    entity = Entity(name="my-entity", description="My entity")
    fv = FeatureView(
        name="my-fv",
        entities=[entity],
        ttl=timedelta(days=1),
        source=BigQuerySource(table="non-existent-mock"),
        schema=[
            Field(name="a", dtype=String),
            Field(name="b", dtype=String),
            Field(name="c", dtype=String),
            Field(name="d", dtype=String),
            Field(name="e", dtype=String),
            Field(name="f", dtype=String),
            Field(name="g", dtype=String),
            Field(name="h", dtype=String),
            Field(name="i", dtype=String),
            Field(name="j", dtype=String),
        ],
    )

    _, feature_list, _, _ = _get_column_names(fv, [entity])
    assert feature_list == ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
