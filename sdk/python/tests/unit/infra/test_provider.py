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

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pytest

from feast import BigQuerySource, FileSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.passthrough_provider import PassthroughProvider
from feast.type_map import feast_value_type_to_python_type
from feast.types import Array, FeastType, Int64, Json, Map, ScalarMap, String, Struct
from feast.utils import _get_column_names
from feast.value_type import ValueType


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


def _ingest(
    dtype: FeastType, values: List[Any], field_mapping: Optional[Dict[str, str]] = None
) -> List[Any]:
    entity = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    fv = FeatureView(
        name="fv",
        entities=[entity],
        ttl=timedelta(days=1),
        source=FileSource(
            path="unused.parquet",
            timestamp_field="event_timestamp",
            field_mapping=field_mapping,
        ),
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="payload", dtype=dtype),
        ],
    )
    column = next(iter(field_mapping)) if field_mapping else "payload"
    df = pd.DataFrame(
        {
            "driver_id": list(range(len(values))),
            "event_timestamp": [datetime(2026, 1, 1, tzinfo=timezone.utc)]
            * len(values),
            column: values,
        }
    )
    rows = PassthroughProvider._prep_rows_to_write_for_ingestion(fv, df)
    return [feast_value_type_to_python_type(row[1]["payload"]) for row in rows]


def _with_types(value: Any) -> Any:
    # 1 == 1.0, so compare type names too.
    if isinstance(value, dict):
        return {key: _with_types(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_with_types(item) for item in value]
    return (type(value).__name__, value)


@pytest.mark.parametrize(
    "dtype,values",
    [
        (Map, [{"a": 1}, {"b": "x"}]),
        (Map, [{"a": 1}, {"a": None}]),
        (Array(Map), [[{"a": 1}], [{"b": 2}]]),
        (ScalarMap, [{1: "a"}, {2: "b"}]),
        (Json, [{"a": 1}, {"b": 2}]),
        (Json, [[1, 2], 3]),
        (Array(Json), [[{"a": 1}], [{"b": 2}]]),
        (
            Struct({"a": Int64, "b": String}),
            [{"a": 1, "b": "x"}, {"a": None, "b": "y"}],
        ),
    ],
)
def test_ingest_keeps_dict_feature_values(dtype: FeastType, values: List[Any]) -> None:
    assert _with_types(_ingest(dtype, values)) == _with_types(values)


def test_ingest_dict_feature_values_with_nulls_and_field_mapping() -> None:
    values = [{"a": 1}, None, np.nan]

    assert _ingest(Map, values, field_mapping={"raw": "payload"}) == [
        {"a": 1},
        None,
        None,
    ]
