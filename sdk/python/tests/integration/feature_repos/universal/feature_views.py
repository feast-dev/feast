from datetime import timedelta

from feast import Feature, FeatureView, ValueType
from feast.data_source import DataSource


def driver_feature_view(
    data_source: DataSource, name="test_correctness"
) -> FeatureView:
    return FeatureView(
        name=name,
        entities=["driver"],
        features=[Feature("value", ValueType.FLOAT)],
        ttl=timedelta(days=5),
        input=data_source,
    )
