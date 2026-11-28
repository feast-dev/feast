"""Unit tests for Registry._update_metadata_fields.

TTL handling (issue #6703): re-applying a FeatureView with its ttl cleared to ``None`` or
``timedelta(0)`` (both the documented ways to express "no ttl") used to be
silently dropped, because the update was gated on ``updated_fv.ttl`` being
truthy -- and both of those values are falsy.

``_update_metadata_fields`` does not use any instance state, so it is exercised
directly via the class here rather than standing up a full registry backend.
"""

from datetime import timedelta

import pytest

from feast.data_source import PushSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.registry import Registry
from feast.labeling.label_view import LabelView
from feast.types import Float32, String


def _feature_view(ttl):
    return FeatureView(
        name="fv",
        entities=[Entity(name="e", join_keys=["e_id"])],
        schema=[Field(name="f1", dtype=Float32)],
        source=FileSource(path="file://feast/*", timestamp_field="ts_col"),
        ttl=ttl,
    )


@pytest.mark.parametrize("cleared_ttl", [None, timedelta(0)])
def test_update_metadata_fields_clears_ttl(cleared_ttl):
    existing_proto = _feature_view(timedelta(days=10)).to_proto()
    # sanity: the existing view starts with a finite ttl
    assert existing_proto.spec.ttl.ToNanoseconds() != 0

    updated_fv = _feature_view(cleared_ttl)
    Registry._update_metadata_fields(None, existing_proto, updated_fv)

    # the cleared ttl (None / timedelta(0)) must now be reflected as "no ttl"
    assert existing_proto.spec.ttl.ToNanoseconds() == 0


def test_update_metadata_fields_preserves_finite_ttl():
    existing_proto = _feature_view(timedelta(days=10)).to_proto()

    updated_fv = _feature_view(timedelta(days=3))
    Registry._update_metadata_fields(None, existing_proto, updated_fv)

    assert existing_proto.spec.ttl.ToTimedelta() == timedelta(days=3)


def _label_view(source):
    return LabelView(
        name="lv",
        entities=[Entity(name="e", join_keys=["e_id"])],
        schema=[
            Field(name="label", dtype=String),
            Field(name="labeler", dtype=String),
        ],
        source=source,
    )


def _file_source(path):
    return FileSource(name="labels", path=path, timestamp_field="ts_col")


@pytest.mark.parametrize(
    "make_source",
    [
        _file_source,
        lambda path: PushSource(name="push", batch_source=_file_source(path)),
    ],
    ids=["file_source", "push_source"],
)
def test_update_metadata_fields_updates_label_view_source(make_source):
    existing_proto = _label_view(make_source("file://old/*")).to_proto()

    updated = _label_view(make_source("file://new/*"))
    Registry._update_metadata_fields(None, existing_proto, updated)

    assert existing_proto.spec.source == updated.to_proto().spec.source
    assert LabelView.from_proto(existing_proto).source == updated.source


def test_update_metadata_fields_clears_label_view_source():
    existing_proto = _label_view(_file_source("file://old/*")).to_proto()

    Registry._update_metadata_fields(None, existing_proto, _label_view(None))

    assert not existing_proto.spec.HasField("source")
