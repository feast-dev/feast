from __future__ import annotations

import pytest

from feast.errors import FeastObjectNotFoundException
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.types import Float32, String


def _build_feature_view(
    version: str = "latest",
    schema: list[Field] | None = None,
) -> FeatureView:
    file_source = FileSource(name="my-file-source", path="test.parquet")
    return FeatureView(
        name="driver_stats",
        entities=[],
        schema=schema
        or [
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=String),
        ],
        source=file_source,
        version=version,
    )


class _MockRegistry:
    """Registry stub for resolve_pending_refs: version snapshots + promoted view."""

    def __init__(self, by_version=None, promoted=None):
        self._by_version = by_version or {}  # (name, version) -> FeatureView
        self._promoted = promoted or {}  # name -> FeatureView
        self.calls: list[tuple] = []

    def get_feature_view_by_version(
        self, name, project, version_number, allow_cache=False
    ):
        self.calls.append(("by_version", name, version_number))
        key = (name, version_number)
        if key not in self._by_version:
            raise FeastObjectNotFoundException(f"{name}@v{version_number}")
        return self._by_version[key]

    def get_any_feature_view(self, name, project, allow_cache=False):
        self.calls.append(("any", name))
        if name not in self._promoted:
            raise FeastObjectNotFoundException(name)
        return self._promoted[name]


def test_pinned_feature_view_stamps_version_tag_on_projection():
    fv = _build_feature_view(version="v2")
    service = FeatureService(name="pinned_service", features=[fv])

    projection = service.feature_view_projections[0]
    assert projection.version_tag == 2
    assert projection.name_to_use() == "driver_stats@v2"


def test_pinned_feature_view_slice_stamps_version_tag():
    fv = _build_feature_view(version="v2")
    service = FeatureService(name="pinned_service", features=[fv[["conv_rate"]]])

    projection = service.feature_view_projections[0]
    assert projection.version_tag == 2
    assert projection.name_to_use() == "driver_stats@v2"
    assert [f.name for f in projection.features] == ["conv_rate"]


def test_version_tag_survives_proto_round_trip():
    fv = _build_feature_view(version="v3")
    service = FeatureService(name="pinned_service", features=[fv])

    restored = FeatureService.from_proto(service.to_proto())

    projection = restored.feature_view_projections[0]
    assert projection.version_tag == 3
    assert projection.name_to_use() == "driver_stats@v3"


def test_unversioned_feature_view_leaves_version_tag_none():
    # Backward compatibility: the default "latest" version must not stamp a
    # version_tag, so existing unversioned feature services are unaffected.
    fv = _build_feature_view(version="latest")
    service = FeatureService(name="unpinned_service", features=[fv])

    projection = service.feature_view_projections[0]
    assert projection.version_tag is None
    assert projection.name_to_use() == "driver_stats"


# --- string feature refs (resolve_pending_refs) ---


def test_string_refs_stashed_not_resolved_at_construction():
    service = FeatureService(name="svc", features=["driver_stats@v1"])
    assert service.feature_view_projections == []
    assert service._pending_feature_refs == ["driver_stats@v1"]


def test_string_ref_pins_version_and_selects_single_feature():
    fv_v1 = _build_feature_view(version="v1")
    registry = _MockRegistry(by_version={("driver_stats", 1): fv_v1})
    service = FeatureService(name="svc", features=["driver_stats@v1:conv_rate"])

    service.resolve_pending_refs("proj", registry)

    projection = service.feature_view_projections[0]
    assert projection.version_tag == 1
    assert projection.name_to_use() == "driver_stats@v1"
    assert [f.name for f in projection.features] == ["conv_rate"]
    assert ("by_version", "driver_stats", 1) in registry.calls
    # Source view's own projection must not be mutated.
    assert [f.name for f in fv_v1.projection.features] == ["conv_rate", "acc_rate"]


def test_string_ref_pins_version_whole_view():
    fv_v1 = _build_feature_view(version="v1")
    registry = _MockRegistry(by_version={("driver_stats", 1): fv_v1})
    service = FeatureService(name="svc", features=["driver_stats@v1"])

    service.resolve_pending_refs("proj", registry)

    projection = service.feature_view_projections[0]
    assert projection.version_tag == 1
    assert [f.name for f in projection.features] == ["conv_rate", "acc_rate"]


def test_unversioned_string_ref_uses_promoted_view():
    promoted = _build_feature_view(version="latest")
    registry = _MockRegistry(promoted={"driver_stats": promoted})
    service = FeatureService(name="svc", features=["driver_stats"])

    service.resolve_pending_refs("proj", registry)

    projection = service.feature_view_projections[0]
    assert projection.version_tag is None
    assert projection.name_to_use() == "driver_stats"
    assert registry.calls == [("any", "driver_stats")]


def test_unversioned_string_ref_prefers_fvs_to_update():
    batch_fv = _build_feature_view(version="latest")
    registry = _MockRegistry()
    service = FeatureService(name="svc", features=["driver_stats"])

    service.resolve_pending_refs(
        "proj", registry, fvs_to_update={"driver_stats": batch_fv}
    )

    assert service.feature_view_projections[0].version_tag is None
    # Resolved from the apply batch, so the registry was never queried.
    assert registry.calls == []


def test_pinned_ref_ignores_fvs_to_update():
    # The pinned v1 snapshot has a different schema than the "latest" object in
    # the apply batch; the pin must resolve from the registry, not the batch.
    fv_v1 = _build_feature_view(
        version="v1",
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=String),
        ],
    )
    latest = _build_feature_view(
        version="latest",
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="new_col", dtype=String),
        ],
    )
    registry = _MockRegistry(by_version={("driver_stats", 1): fv_v1})
    service = FeatureService(name="svc", features=["driver_stats@v1"])

    service.resolve_pending_refs(
        "proj", registry, fvs_to_update={"driver_stats": latest}
    )

    projection = service.feature_view_projections[0]
    assert projection.version_tag == 1
    assert [f.name for f in projection.features] == ["conv_rate", "acc_rate"]
    assert ("by_version", "driver_stats", 1) in registry.calls


def test_string_ref_unknown_feature_raises():
    fv_v1 = _build_feature_view(version="v1")
    registry = _MockRegistry(by_version={("driver_stats", 1): fv_v1})
    service = FeatureService(name="svc", features=["driver_stats@v1:missing"])

    with pytest.raises(ValueError, match="'missing' not found"):
        service.resolve_pending_refs("proj", registry)


def test_string_ref_unknown_version_raises():
    registry = _MockRegistry()  # no versions registered
    service = FeatureService(name="svc", features=["driver_stats@v9"])

    with pytest.raises(FeastObjectNotFoundException):
        service.resolve_pending_refs("proj", registry)


def test_resolve_pending_refs_is_noop_without_refs():
    fv = _build_feature_view(version="latest")
    service = FeatureService(name="svc", features=[fv])
    service.resolve_pending_refs("proj", _MockRegistry())
    # Only the object-based projection is present; no extra work done.
    assert len(service.feature_view_projections) == 1


def test_infer_features_skips_string_refs():
    fv_v1 = _build_feature_view(version="v1")
    registry = _MockRegistry(by_version={("driver_stats", 1): fv_v1})
    service = FeatureService(name="svc", features=["driver_stats@v1"])
    service.resolve_pending_refs("proj", registry)

    # Must not raise "invalid type str" for the still-present string entry.
    service.infer_features(fvs_to_update={})
    assert service.feature_view_projections[0].version_tag == 1
