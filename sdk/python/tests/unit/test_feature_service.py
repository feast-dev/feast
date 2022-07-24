import pytest

from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.types import Float32


def test_feature_service_with_description():
    feature_service = FeatureService(
        name="my-feature-service", features=[], description="a clear description"
    )
    assert feature_service.to_proto().spec.description == "a clear description"


def test_feature_service_without_description():
    feature_service = FeatureService(name="my-feature-service", features=[])
    #
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


def test_feature_view_kw_args_warning():
    with pytest.warns(DeprecationWarning):
        service = FeatureService("name", [], tags={"tag_1": "tag"}, description="desc")
        assert service.name == "name"
        assert service.tags == {"tag_1": "tag"}
        assert service.description == "desc"

    # More positional args than name and features
    with pytest.raises(ValueError):
        service = FeatureService("name", [], {"tag_1": "tag"}, "desc")

    # No name defined.
    with pytest.raises(ValueError):
        service = FeatureService(features=[], tags={"tag_1": "tag"}, description="desc")


def no_warnings(func):
    def wrapper_no_warnings(*args, **kwargs):
        with pytest.warns(None) as warnings:
            func(*args, **kwargs)

        if len(warnings) > 0:
            raise AssertionError(
                "Warnings were raised: " + ", ".join([str(w) for w in warnings])
            )

    return wrapper_no_warnings


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
