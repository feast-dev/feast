from __future__ import annotations

from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.types import Float32, String


def _build_feature_view(version: str = "latest") -> FeatureView:
    file_source = FileSource(name="my-file-source", path="test.parquet")
    return FeatureView(
        name="driver_stats",
        entities=[],
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=String),
        ],
        source=file_source,
        version=version,
    )


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
