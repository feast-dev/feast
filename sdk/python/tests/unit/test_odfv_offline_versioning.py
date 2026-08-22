from __future__ import annotations

import pandas as pd
import pytest

from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.on_demand_feature_view import (
    OnDemandFeatureView,
    PandasTransformation,
)
from feast.types import Float32
from feast.utils import _get_requested_on_demand_feature_views


def _udf(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["output1"] = features_df["feature1"]
    return df


def _build_odfv(name: str = "my_odfv") -> OnDemandFeatureView:
    source = FeatureView(
        name="source_fv",
        entities=[],
        schema=[Field(name="feature1", dtype=Float32)],
        source=FileSource(name="src", path="test.parquet"),
    )
    return OnDemandFeatureView(
        name=name,
        sources=[source],
        schema=[Field(name="output1", dtype=Float32)],
        feature_transformation=PandasTransformation(udf=_udf, udf_string="src"),
    )


def _build_feature_view(name: str = "plain_fv") -> FeatureView:
    return FeatureView(
        name=name,
        entities=[],
        schema=[Field(name="feature1", dtype=Float32)],
        source=FileSource(name="src2", path="test.parquet"),
    )


class _MockRegistry:
    def __init__(self, promoted=None, by_version=None, versioned_supported=True):
        self._promoted = promoted or []
        self._by_version = by_version or {}  # (name, version) -> view
        self._versioned_supported = versioned_supported

    def list_on_demand_feature_views(self, project, allow_cache=True):
        return self._promoted

    def get_feature_view_by_version(
        self, name, project, version_number, allow_cache=False
    ):
        if not self._versioned_supported:
            raise NotImplementedError
        return self._by_version[(name, version_number)]

    def get_any_feature_view(self, name, project, allow_cache=False):
        for odfv in self._promoted:
            if odfv.name == name:
                return odfv
        raise KeyError(name)


def test_unversioned_ref_returns_promoted_odfv():
    promoted = _build_odfv("my_odfv")
    registry = _MockRegistry(promoted=[promoted])

    result = _get_requested_on_demand_feature_views(
        ["my_odfv:output1"], "proj", registry
    )

    assert [odfv.name for odfv in result] == ["my_odfv"]
    assert result[0].projection.version_tag is None


def test_versioned_ref_returns_pinned_snapshot_with_version_tag():
    pinned = _build_odfv("my_odfv")
    registry = _MockRegistry(by_version={("my_odfv", 1): pinned})

    result = _get_requested_on_demand_feature_views(
        ["my_odfv@v1:output1"], "proj", registry
    )

    assert len(result) == 1
    assert result[0].projection.version_tag == 1
    assert result[0].projection.name_to_use() == "my_odfv@v1"


def test_versioned_ref_to_regular_feature_view_is_skipped():
    # get_feature_view_by_version can return a plain FeatureView; it is not an
    # ODFV and must be dropped from the ODFV list.
    plain = _build_feature_view("plain_fv")
    registry = _MockRegistry(by_version={("plain_fv", 1): plain})

    result = _get_requested_on_demand_feature_views(
        ["plain_fv@v1:feature1"], "proj", registry
    )

    assert result == []


def test_duplicate_refs_are_deduplicated():
    pinned = _build_odfv("my_odfv")
    registry = _MockRegistry(by_version={("my_odfv", 1): pinned})

    result = _get_requested_on_demand_feature_views(
        ["my_odfv@v1:output1", "my_odfv@v1:output1"], "proj", registry
    )

    assert len(result) == 1


def test_v0_falls_back_when_versioned_lookup_unsupported():
    promoted = _build_odfv("my_odfv")
    registry = _MockRegistry(promoted=[promoted], versioned_supported=False)

    result = _get_requested_on_demand_feature_views(
        ["my_odfv@v0:output1"], "proj", registry
    )

    assert [odfv.name for odfv in result] == ["my_odfv"]


def test_nonzero_version_reraises_when_lookup_unsupported():
    registry = _MockRegistry(versioned_supported=False)

    with pytest.raises(NotImplementedError):
        _get_requested_on_demand_feature_views(["my_odfv@v2:output1"], "proj", registry)
