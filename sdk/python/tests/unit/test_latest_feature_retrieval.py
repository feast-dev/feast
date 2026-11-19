
from feast.feature_store import FeatureStore


def test_feature_store_exposes_latest_features_api():
    """The public FeatureStore should expose latest-only retrieval."""
    assert hasattr(FeatureStore, "get_latest_features"), (
        "FeatureStore does not expose get_latest_features()"
    )
