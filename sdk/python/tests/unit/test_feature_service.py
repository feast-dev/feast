from feast import FeatureService


def test_feature_service_with_description():
    feature_service = FeatureService(
        name="my-feature-service", features=[], description="a clear description"
    )
    assert feature_service.to_proto().spec.description == "a clear description"


def test_feature_service_without_description():
    feature_service = FeatureService(name="my-feature-service", features=[])
    #
    assert feature_service.to_proto().spec.description == ""
