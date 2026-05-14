"""
Unit tests for BFV compute-on-read in SparkOfflineStore.get_historical_features().

Verifies that BatchFeatureViews with a UDF have their transformation applied
during get_historical_features(), with the transformed DataFrame registered as
a temp view that replaces the raw table_subquery in the PIT join.
"""

from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from feast.batch_feature_view import BatchFeatureView
from feast.feature_view import FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    _apply_bfv_transformations,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.transformation.base import Transformation


@pytest.fixture()
def spark_session():
    mock = MagicMock()
    mock.sql.return_value = MagicMock(name="source_df")
    return mock


@pytest.fixture()
def spark_source():
    source = MagicMock(spec=SparkSource)
    source.get_table_query_string.return_value = "`raw_events`"
    return source


@pytest.fixture()
def base_query_context():
    return offline_utils.FeatureViewQueryContext(
        name="my_bfv",
        ttl=3600,
        entities=["user_id"],
        features=["avg_rating"],
        field_mapping={},
        timestamp_field="event_timestamp",
        created_timestamp_column=None,
        table_subquery="`raw_events`",
        entity_selections=["user_id AS user_id"],
        min_event_timestamp="2023-01-01T00:00:00",
        max_event_timestamp="2024-01-01T00:00:00",
        date_partition_column=None,
        timestamp_field_type=None,
    )


def _make_bfv(name: str, spark_source, has_udf: bool = True):
    """Create a mock BatchFeatureView with optional UDF."""
    fv = MagicMock(spec=BatchFeatureView)
    fv.name = name
    fv.projection = MagicMock()
    fv.projection.name_to_use.return_value = name
    fv.batch_source = spark_source
    fv.source_views = []

    if has_udf:
        transformation = MagicMock(spec=Transformation)
        transformed_df = MagicMock(name="transformed_df")
        transformation.udf = MagicMock(return_value=transformed_df)
        fv.feature_transformation = transformation
        fv.udf = transformation.udf
    else:
        fv.feature_transformation = None
        fv.udf = None

    return fv


def _make_plain_fv(name: str, spark_source):
    """Create a mock plain FeatureView (not a BatchFeatureView)."""
    fv = MagicMock(spec=FeatureView)
    fv.name = name
    fv.projection = MagicMock()
    fv.projection.name_to_use.return_value = name
    fv.batch_source = spark_source
    fv.feature_transformation = None
    fv.udf = None
    return fv


class TestApplyBfvTransformations:
    def test_bfv_with_udf_replaces_table_subquery(
        self, spark_session, spark_source, base_query_context
    ):
        """BFV with a UDF should have its table_subquery replaced with a temp view."""
        bfv = _make_bfv("my_bfv", spark_source)
        contexts = [base_query_context]

        result = _apply_bfv_transformations(spark_session, [bfv], contexts)

        assert len(result) == 1
        assert result[0].table_subquery != "`raw_events`"
        assert result[0].table_subquery.startswith("feast_bfv_")

    def test_bfv_udf_is_invoked_with_source_df(
        self, spark_session, spark_source, base_query_context
    ):
        """The UDF should be called with the DataFrame read from the raw source."""
        bfv = _make_bfv("my_bfv", spark_source)
        contexts = [base_query_context]

        _apply_bfv_transformations(spark_session, [bfv], contexts)

        sql_arg = spark_session.sql.call_args[0][0]
        assert "SELECT * FROM `raw_events`" in sql_arg
        assert "WHERE" in sql_arg
        source_df = spark_session.sql.return_value
        bfv.feature_transformation.udf.assert_called_once_with(source_df)

    def test_transformed_df_registered_as_temp_view(
        self, spark_session, spark_source, base_query_context
    ):
        """The transformed DataFrame should be registered as a temp view."""
        bfv = _make_bfv("my_bfv", spark_source)
        transformed_df = bfv.feature_transformation.udf.return_value
        contexts = [base_query_context]

        result = _apply_bfv_transformations(spark_session, [bfv], contexts)

        transformed_df.createOrReplaceTempView.assert_called_once()
        view_name = transformed_df.createOrReplaceTempView.call_args[0][0]
        assert view_name == result[0].table_subquery

    def test_plain_feature_view_unchanged(
        self, spark_session, spark_source, base_query_context
    ):
        """Plain FeatureViews (not BFV) should pass through without modification."""
        fv = _make_plain_fv("my_bfv", spark_source)
        contexts = [base_query_context]

        result = _apply_bfv_transformations(spark_session, [fv], contexts)

        assert result[0].table_subquery == "`raw_events`"
        spark_session.sql.assert_not_called()

    def test_bfv_without_udf_unchanged(
        self, spark_session, spark_source, base_query_context
    ):
        """BFV without a UDF should pass through without modification."""
        bfv = _make_bfv("my_bfv", spark_source, has_udf=False)
        contexts = [base_query_context]

        result = _apply_bfv_transformations(spark_session, [bfv], contexts)

        assert result[0].table_subquery == "`raw_events`"
        spark_session.sql.assert_not_called()

    def test_mixed_views_only_transforms_bfvs(
        self, spark_session, spark_source, base_query_context
    ):
        """With mixed BFV and plain FVs, only BFVs with UDFs get transformed."""
        bfv = _make_bfv("my_bfv", spark_source)
        plain_fv = _make_plain_fv("plain_fv", spark_source)

        ctx_bfv = base_query_context
        ctx_plain = replace(
            base_query_context,
            name="plain_fv",
            features=["some_feature"],
        )

        result = _apply_bfv_transformations(
            spark_session, [bfv, plain_fv], [ctx_bfv, ctx_plain]
        )

        assert result[0].table_subquery.startswith("feast_bfv_")
        assert result[1].table_subquery == "`raw_events`"

    def test_time_range_filter_applied(
        self, spark_session, spark_source, base_query_context
    ):
        """Source query should include time bounds from the context."""
        bfv = _make_bfv("my_bfv", spark_source)
        contexts = [base_query_context]

        _apply_bfv_transformations(spark_session, [bfv], contexts)

        sql_arg = spark_session.sql.call_args[0][0]
        assert "2023-01-01" in sql_arg
        assert "2024-01-01" in sql_arg
        assert "event_timestamp" in sql_arg

    def test_time_range_filter_with_none_min_timestamp(
        self, spark_session, spark_source, base_query_context
    ):
        """When min_event_timestamp is None (no TTL), query should still work."""
        bfv = _make_bfv("my_bfv", spark_source)
        ctx = replace(base_query_context, min_event_timestamp=None)

        result = _apply_bfv_transformations(spark_session, [bfv], [ctx])

        assert result[0].table_subquery.startswith("feast_bfv_")
        sql_arg = spark_session.sql.call_args[0][0]
        assert "2024-01-01" in sql_arg

    def test_other_context_fields_preserved(
        self, spark_session, spark_source, base_query_context
    ):
        """All fields besides table_subquery should remain unchanged."""
        bfv = _make_bfv("my_bfv", spark_source)
        contexts = [base_query_context]

        result = _apply_bfv_transformations(spark_session, [bfv], contexts)

        assert result[0].name == base_query_context.name
        assert result[0].ttl == base_query_context.ttl
        assert result[0].entities == base_query_context.entities
        assert result[0].features == base_query_context.features
        assert result[0].timestamp_field == base_query_context.timestamp_field
        assert result[0].min_event_timestamp == base_query_context.min_event_timestamp
        assert result[0].max_event_timestamp == base_query_context.max_event_timestamp
