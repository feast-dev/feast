from datetime import timedelta

from feast import Feature, FeatureView, ValueType
from feast.data_source import DataSource


def correctness_feature_view(data_source: DataSource) -> FeatureView:
    return FeatureView(
        name="test_correctness",
        entities=["driver"],
        features=[Feature("value", ValueType.FLOAT)],
        ttl=timedelta(days=5),
        input=data_source,
    )
