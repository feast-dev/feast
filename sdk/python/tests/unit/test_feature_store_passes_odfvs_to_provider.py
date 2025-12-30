from types import SimpleNamespace

import pytest

from feast.feature_store import FeatureStore
from feast.infra.offline_stores.offline_store import RetrievalJob


class _CapturingProvider:
    def __init__(self):
        self.captured_on_demand_feature_views = None

    def get_historical_features(
        self,
        config,
        feature_views,
        on_demand_feature_views,
        feature_refs,
        entity_df,
        registry,
        project,
        full_feature_names,
        **kwargs,
    ):
        self.captured_on_demand_feature_views = on_demand_feature_views
        return RetrievalJob()


def test_feature_store_passes_on_demand_feature_views_to_provider(monkeypatch):
    provider = _CapturingProvider()

    store = FeatureStore.__new__(FeatureStore)
    store.config = SimpleNamespace(project="test_project", coerce_tz_aware=False)
    store._registry = object()
    store._get_provider = lambda: provider

    fv = object()
    odfv = object()

    monkeypatch.setattr(
        "feast.utils._get_features",
        lambda registry, project, features, allow_cache=False: ["odfv:feat"],
    )
    monkeypatch.setattr(
        "feast.utils._get_feature_views_to_use",
        lambda registry, project, features, allow_cache=False, hide_dummy_entity=True: (
            [fv],
            [odfv],
        ),
    )
    monkeypatch.setattr(
        "feast.utils._group_feature_refs",
        lambda feature_refs, feature_views, on_demand_feature_views: (
            [(fv, [])],
            [(odfv, [])],
        ),
    )
    monkeypatch.setattr("feast.utils._validate_feature_refs", lambda *args, **kwargs: None)

    store.get_historical_features(entity_df="SELECT 1", features=["odfv:feat"])

    assert provider.captured_on_demand_feature_views == [odfv]
