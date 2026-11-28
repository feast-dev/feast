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
from unittest.mock import Mock

import pytest

from feast import BigQuerySource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.infra.passthrough_provider import PassthroughProvider
from feast.saved_dataset import SavedDataset
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


@pytest.mark.parametrize(
    "min_event_timestamp, max_event_timestamp, expected_start, expected_end",
    [
        (None, None, None, None),
        (
            datetime(2026, 1, 1),
            None,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            None,
        ),
        (
            None,
            datetime(2026, 1, 3),
            None,
            datetime(2026, 1, 3, 0, 0, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2026, 1, 1),
            datetime(2026, 1, 3),
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 3, 0, 0, 1, tzinfo=timezone.utc),
        ),
    ],
)
def test_retrieve_saved_dataset_leaves_missing_event_bounds_open(
    min_event_timestamp, max_event_timestamp, expected_start, expected_end
):
    dataset = SavedDataset(
        name="ds",
        features=["fv:f1"],
        join_keys=["e_id"],
        storage=SavedDatasetFileStorage(path="ds.parquet"),
    )
    dataset.min_event_timestamp = min_event_timestamp
    dataset.max_event_timestamp = max_event_timestamp
    provider = PassthroughProvider(Mock())
    provider._offline_store = Mock()

    provider.retrieve_saved_dataset(config=Mock(), dataset=dataset)

    pull_kwargs = provider._offline_store.pull_all_from_table_or_query.call_args.kwargs
    assert pull_kwargs["start_date"] == expected_start
    assert pull_kwargs["end_date"] == expected_end
