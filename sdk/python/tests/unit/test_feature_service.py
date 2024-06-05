from datetime import datetime, timedelta

from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.types import Float32
from tests.utils.test_wrappers import no_warnings


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


def test_update_meta():
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
    current_time = datetime.now()
    # Create a feature service that is already present in the SQL registry
    stored_feature_service = FeatureService(
        name="test_feature_service", features=[feature_view[["feature1", "feature2"]]]
    )
    stored_feature_service.created_timestamp = current_time - timedelta(days=1)
    stored_feature_service.last_updated_timestamp = current_time - timedelta(days=1)
    stored_feature_service_proto = stored_feature_service.to_proto()
    serialized_proto = stored_feature_service_proto.SerializeToString()

    # Update the Feature Service i.e. here it's simply the name
    updated_feature_service = FeatureService(
        name="my-feature-service-1", features=[feature_view[["feature1", "feature2"]]]
    )
    updated_feature_service.last_updated_timestamp = current_time

    updated_feature_service.update_meta(serialized_proto)

    assert (
        updated_feature_service.created_timestamp
        == stored_feature_service.created_timestamp
    )
    assert updated_feature_service.last_updated_timestamp == current_time
