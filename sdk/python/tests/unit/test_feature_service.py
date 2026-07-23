from feast.errors import FeastObjectNotFoundException
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2
from feast.protos.feast.core.FeatureViewProjection_pb2 import (
    FeatureViewProjection as FeatureViewProjectionProto,
)
from feast.types import Float32, String
from tests.utils.test_wrappers import no_warnings


class _MockRegistry:
    def __init__(self, feature_view: FeatureView):
        self._feature_view = feature_view

    def get_any_feature_view(self, name, project, allow_cache=False):
        if name != self._feature_view.name:
            raise FeastObjectNotFoundException(name)
        return self._feature_view


def _build_feature_view():
    file_source = FileSource(name="my-file-source", path="test.parquet")
    return FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=String),
        ],
        source=file_source,
    )


def test_prepare_for_apply_whole_view():
    feature_view = _build_feature_view()
    feature_service = FeatureService.from_proto(
        FeatureService(
            name="my-feature-service",
            features=[],
        ).to_proto()
    )
    feature_service.feature_view_projections = [
        FeatureViewProjection(
            name=feature_view.name,
            name_alias=None,
            desired_features=[],
            features=[],
        )
    ]

    feature_service.prepare_for_apply(_MockRegistry(feature_view), "test_project")

    assert len(feature_service.feature_view_projections) == 1
    resolved = feature_service.feature_view_projections[0]
    assert [feature.name for feature in resolved.features] == [
        "feature1",
        "feature2",
    ]
    assert resolved.features[0].dtype == Float32
    assert resolved.features[1].dtype == String


def test_prepare_for_apply_feature_subset():
    feature_view = _build_feature_view()
    feature_service = FeatureService(
        name="my-feature-service",
        features=[],
    )
    feature_service.feature_view_projections = [
        FeatureViewProjection(
            name=feature_view.name,
            name_alias=None,
            desired_features=["feature2"],
            features=[],
        )
    ]

    feature_service.prepare_for_apply(_MockRegistry(feature_view), "test_project")

    resolved = feature_service.feature_view_projections[0]
    assert len(resolved.features) == 1
    assert resolved.features[0].name == "feature2"
    assert resolved.features[0].dtype == String


def test_prepare_for_apply_name_only_columns():
    feature_view = _build_feature_view()
    projection_proto = FeatureViewProjectionProto(
        feature_view_name=feature_view.name,
    )
    projection_proto.feature_columns.append(FeatureSpecV2(name="feature2"))
    feature_service = FeatureService.from_proto(
        FeatureService(name="my-feature-service", features=[]).to_proto()
    )
    feature_service.feature_view_projections = [
        FeatureViewProjection.from_proto(projection_proto)
    ]

    feature_service.prepare_for_apply(_MockRegistry(feature_view), "test_project")

    resolved = feature_service.feature_view_projections[0]
    assert len(resolved.features) == 1
    assert resolved.features[0].name == "feature2"
    assert resolved.features[0].dtype == String


def test_prepare_for_apply_keeps_complete_projection():
    feature_view = _build_feature_view()
    feature_service = FeatureService(
        name="my-feature-service",
        features=[feature_view[["feature1"]]],
    )
    original_projection = feature_service.feature_view_projections[0]

    feature_service.prepare_for_apply(_MockRegistry(feature_view), "test_project")

    assert feature_service.feature_view_projections[0] == original_projection


def test_build_apply_request():
    request = FeatureService.build_apply_request(
        name="my-feature-service",
        project="test_project",
        feature_view_refs=[
            ("feature_view_a", None),
            ("feature_view_b", ["feature1"]),
        ],
        description="test service",
        tags={"team": "ml"},
        owner="owner@example.com",
    )

    spec = request.feature_service.spec
    assert spec.name == "my-feature-service"
    assert spec.description == "test service"
    assert spec.owner == "owner@example.com"
    assert dict(spec.tags) == {"team": "ml"}
    assert request.project == "test_project"
    assert len(spec.features) == 2
    assert spec.features[0].feature_view_name == "feature_view_a"
    assert len(spec.features[0].feature_columns) == 0
    assert spec.features[1].feature_view_name == "feature_view_b"
    assert [column.name for column in spec.features[1].feature_columns] == ["feature1"]


def test_feature_service_with_description():
    feature_service = FeatureService(
        name="my-feature-service", features=[], description="a clear description"
    )
    assert feature_service.to_proto().spec.description == "a clear description"


def test_feature_service_without_description():
    feature_service = FeatureService(name="my-feature-service", features=[])
    assert feature_service.to_proto().spec.description == ""


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
    feature_service_1 = FeatureService(
        name="my-feature-service", features=[feature_view[["feature1", "feature2"]]]
    )
    feature_service_2 = FeatureService(
        name="my-feature-service", features=[feature_view[["feature1", "feature2"]]]
    )
    feature_service_3 = FeatureService(
        name="my-feature-service", features=[feature_view[["feature1"]]]
    )
    feature_service_4 = FeatureService(
        name="my-feature-service",
        features=[feature_view[["feature1"]]],
        description="test",
    )

    s1 = {feature_service_1, feature_service_2}
    assert len(s1) == 1

    s2 = {feature_service_1, feature_service_3}
    assert len(s2) == 2

    s3 = {feature_service_3, feature_service_4}
    assert len(s3) == 2

    s4 = {feature_service_1, feature_service_2, feature_service_3, feature_service_4}
    assert len(s4) == 3


@no_warnings
def test_feature_view_kw_args_normal():
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
    _ = FeatureService(
        name="my-feature-service", features=[feature_view[["feature1", "feature2"]]]
    )
