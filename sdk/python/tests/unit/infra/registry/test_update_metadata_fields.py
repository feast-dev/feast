"""Unit tests for Registry._update_metadata_fields TTL handling (issue #6703).

Re-applying a FeatureView with its ttl cleared to ``None`` or
``timedelta(0)`` (both the documented ways to express "no ttl") used to be
silently dropped, because the update was gated on ``updated_fv.ttl`` being
truthy -- and both of those values are falsy.

``_update_metadata_fields`` does not use any instance state, so it is exercised
directly via the class here rather than standing up a full registry backend.
"""

from datetime import timedelta

import pytest

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.registry import Registry
from feast.types import Float32


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
