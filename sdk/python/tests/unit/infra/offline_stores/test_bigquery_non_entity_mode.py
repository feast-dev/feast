"""
Unit tests for BigQuery offline store non-entity mode (entity_df=None).

Covers:
- _gather_all_entities helper
- _bq_create_entity_union_table SQL generation
- get_historical_features non-entity mode flow
- Regression: normal entity_df mode still works
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from feast.infra.offline_stores.bigquery import (
    BigQueryOfflineStore,
    BigQueryOfflineStoreConfig,
    BigQueryRetrievalJob,
    _bq_create_entity_union_table,
)
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.offline_stores.offline_utils import (
    FeatureViewQueryContext,
    gather_all_entities,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

START = datetime(2023, 1, 1, tzinfo=timezone.utc)
END = datetime(2024, 1, 1, tzinfo=timezone.utc)
TABLE_NAME = "project.dataset.feast_tmp_abc123"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_repo_config(project_id: str = "my-project") -> RepoConfig:
    return RepoConfig(
        registry="gs://test/registry.db",
        project="feast_test",
        provider="gcp",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=BigQueryOfflineStoreConfig(
            type="bigquery",
            project_id=project_id,
            dataset="feast",
        ),
    )


def _make_fv_context(
    name: str,
    entities: list,
    timestamp_field: str = "event_timestamp",
    table_subquery: str = "`project.dataset.table`",
) -> FeatureViewQueryContext:
    return FeatureViewQueryContext(
        name=name,
        ttl=86400,
        entities=entities,
        features=["feature_a"],
        field_mapping={},
        timestamp_field=timestamp_field,
        created_timestamp_column=None,
        table_subquery=table_subquery,
        entity_selections=[f"{e} AS {e}" for e in entities],
        min_event_timestamp="2023-01-01T00:00:00",
        max_event_timestamp="2024-01-01T00:00:00",
        date_partition_column=None,
        timestamp_field_type=None,
    )


def _make_bq_source(table: str = "project.dataset.table") -> BigQuerySource:
    return BigQuerySource(
        name="test_source",
        table=table,
        timestamp_field="event_timestamp",
    )


def _make_feature_view_mock(
    name: str, entities: list, table: str = "project.dataset.table"
) -> MagicMock:
    fv = MagicMock()
    fv.name = name
    fv.batch_source = _make_bq_source(table)
    fv.entities = entities
    return fv


# ---------------------------------------------------------------------------
# Tests: _gather_all_entities
# ---------------------------------------------------------------------------


class TestGatherAllEntities:
    def test_single_feature_view(self):
        ctx = _make_fv_context("fv1", ["customer_id", "item_id"])
        assert gather_all_entities([ctx]) == ["customer_id", "item_id"]

    def test_multiple_views_overlapping_entities(self):
        ctx1 = _make_fv_context("fv1", ["customer_id", "item_id"])
        ctx2 = _make_fv_context("fv2", ["customer_id", "store_id"])
        result = gather_all_entities([ctx1, ctx2])
        # customer_id should appear only once; order is first-seen
        assert result == ["customer_id", "item_id", "store_id"]

    def test_multiple_views_disjoint_entities(self):
        ctx1 = _make_fv_context("fv1", ["driver_id"])
        ctx2 = _make_fv_context("fv2", ["customer_id"])
        result = gather_all_entities([ctx1, ctx2])
        assert result == ["driver_id", "customer_id"]

    def test_entityless_feature_view(self):
        ctx = _make_fv_context("fv1", [])
        assert gather_all_entities([ctx]) == []

    def test_empty_list(self):
        assert gather_all_entities([]) == []

    def test_preserves_insertion_order(self):
        ctx1 = _make_fv_context("fv1", ["z_entity", "a_entity"])
        ctx2 = _make_fv_context("fv2", ["a_entity", "m_entity"])
        result = gather_all_entities([ctx1, ctx2])
        assert result == ["z_entity", "a_entity", "m_entity"]


# ---------------------------------------------------------------------------
# Tests: _bq_create_entity_union_table — SQL generation
# ---------------------------------------------------------------------------


class TestBqCreateEntityUnionTable:
    def _make_client(self) -> MagicMock:
        client = MagicMock()
        client.query.return_value = MagicMock()
        client.get_table.return_value = MagicMock()
        return client

    @patch("feast.infra.offline_stores.bigquery.block_until_done")
    @patch("feast.infra.offline_stores.bigquery._utc_now")
    def test_single_view_creates_table_with_correct_sql(self, mock_utc_now, mock_block):
        mock_utc_now.return_value = END
        client = self._make_client()
        fv = _make_feature_view_mock("fv1", ["customer_id"])
        ctx = _make_fv_context(
            "fv1", ["customer_id"], table_subquery="`project.dataset.orders`"
        )

        _bq_create_entity_union_table(
            client=client,
            table_name=TABLE_NAME,
            feature_views=[fv],
            fv_query_contexts=[ctx],
            start_date=START,
            end_date=END,
            all_entities=["customer_id"],
            event_timestamp_col="entity_ts",
        )

        sql = client.query.call_args[0][0]
        assert f"CREATE TABLE `{TABLE_NAME}` AS" in sql
        assert "SELECT DISTINCT `customer_id`" in sql
        assert "UNION DISTINCT" not in sql  # single view → no union
        assert "entity_ts" in sql

    @patch("feast.infra.offline_stores.bigquery.block_until_done")
    @patch("feast.infra.offline_stores.bigquery._utc_now")
    def test_entity_column_not_cast_to_string(self, mock_utc_now, mock_block):
        """INT64 entity columns must NOT be cast to STRING — that breaks the PIT join."""
        mock_utc_now.return_value = END
        client = self._make_client()
        fv = _make_feature_view_mock("fv1", ["user_id"])
        ctx = _make_fv_context("fv1", ["user_id"])

        _bq_create_entity_union_table(
            client=client,
            table_name=TABLE_NAME,
            feature_views=[fv],
            fv_query_contexts=[ctx],
            start_date=START,
            end_date=END,
            all_entities=["user_id"],
            event_timestamp_col="entity_ts",
        )

        sql = client.query.call_args[0][0]
        assert "CAST(`user_id` AS STRING)" not in sql
        assert "`user_id`" in sql

    @patch("feast.infra.offline_stores.bigquery.block_until_done")
    @patch("feast.infra.offline_stores.bigquery._utc_now")
    def test_missing_entity_filled_with_null_not_cast_string(
        self, mock_utc_now, mock_block
    ):
        """When a feature view doesn't have an entity column, fill it with NULL."""
        mock_utc_now.return_value = END
        client = self._make_client()
        fv1 = _make_feature_view_mock("fv1", ["customer_id", "item_id"])
        fv2 = _make_feature_view_mock("fv2", ["customer_id"])
        ctx1 = _make_fv_context("fv1", ["customer_id", "item_id"])
        ctx2 = _make_fv_context("fv2", ["customer_id"])

        _bq_create_entity_union_table(
            client=client,
            table_name=TABLE_NAME,
            feature_views=[fv1, fv2],
            fv_query_contexts=[ctx1, ctx2],
            start_date=START,
            end_date=END,
            all_entities=["customer_id", "item_id"],
            event_timestamp_col="entity_ts",
        )

        sql = client.query.call_args[0][0]
        # fv2 does not have item_id → must appear as NULL for item_id
        assert "`item_id`" in sql
        assert "NULL" in sql
        # fv2's item_id fill must not appear as a regular column reference
        assert "SELECT DISTINCT `customer_id`" in sql

    @patch("feast.infra.offline_stores.bigquery.block_until_done")
    @patch("feast.infra.offline_stores.bigquery._utc_now")
    def test_multiple_views_produce_union_distinct(self, mock_utc_now, mock_block):
        mock_utc_now.return_value = END
        client = self._make_client()
        fv1 = _make_feature_view_mock("fv1", ["driver_id"])
        fv2 = _make_feature_view_mock("fv2", ["driver_id"])
        ctx1 = _make_fv_context("fv1", ["driver_id"])
        ctx2 = _make_fv_context("fv2", ["driver_id"])

        _bq_create_entity_union_table(
            client=client,
            table_name=TABLE_NAME,
            feature_views=[fv1, fv2],
            fv_query_contexts=[ctx1, ctx2],
            start_date=START,
            end_date=END,
            all_entities=["driver_id"],
            event_timestamp_col="entity_ts",
        )

        sql = client.query.call_args[0][0]
        assert "UNION DISTINCT" in sql

    @patch("feast.infra.offline_stores.bigquery.block_until_done")
    @patch("feast.infra.offline_stores.bigquery._utc_now")
    def test_timestamp_filter_uses_start_and_end(self, mock_utc_now, mock_block):
        mock_utc_now.return_value = END
        client = self._make_client()
        fv = _make_feature_view_mock("fv1", ["entity_id"])
        ctx = _make_fv_context("fv1", ["entity_id"])

        _bq_create_entity_union_table(
            client=client,
            table_name=TABLE_NAME,
            feature_views=[fv],
            fv_query_contexts=[ctx],
            start_date=START,
            end_date=END,
            all_entities=["entity_id"],
            event_timestamp_col="entity_ts",
        )

        sql = client.query.call_args[0][0]
        assert "2023-01-01T00:00:00" in sql  # start_date
        assert (
            "2024-01-01T00:00:00" in sql
        )  # end_date (appears twice: WHERE + entity_ts value)
        assert "BETWEEN TIMESTAMP(" in sql

    @patch("feast.infra.offline_stores.bigquery.block_until_done")
    @patch("feast.infra.offline_stores.bigquery._utc_now")
    def test_entity_ts_column_set_to_end_date(self, mock_utc_now, mock_block):
        """The as-of timestamp in the synthetic left table must equal end_date."""
        mock_utc_now.return_value = END
        client = self._make_client()
        fv = _make_feature_view_mock("fv1", ["entity_id"])
        ctx = _make_fv_context("fv1", ["entity_id"])

        _bq_create_entity_union_table(
            client=client,
            table_name=TABLE_NAME,
            feature_views=[fv],
            fv_query_contexts=[ctx],
            start_date=START,
            end_date=END,
            all_entities=["entity_id"],
            event_timestamp_col="entity_ts",
        )

        sql = client.query.call_args[0][0]
        assert "TIMESTAMP('2024-01-01T00:00:00') AS `entity_ts`" in sql

    @patch("feast.infra.offline_stores.bigquery.block_until_done")
    @patch("feast.infra.offline_stores.bigquery._utc_now")
    def test_sets_table_expiry(self, mock_utc_now, mock_block):
        mock_utc_now.return_value = datetime(2024, 1, 1, tzinfo=timezone.utc)
        client = self._make_client()
        fv = _make_feature_view_mock("fv1", ["entity_id"])
        ctx = _make_fv_context("fv1", ["entity_id"])

        _bq_create_entity_union_table(
            client=client,
            table_name=TABLE_NAME,
            feature_views=[fv],
            fv_query_contexts=[ctx],
            start_date=START,
            end_date=END,
            all_entities=["entity_id"],
            event_timestamp_col="entity_ts",
        )

        client.update_table.assert_called_once()
        updated_table = client.update_table.call_args[0][0]
        # expiry should be 30 minutes after _utc_now()
        assert updated_table.expires == datetime(
            2024, 1, 1, tzinfo=timezone.utc
        ) + timedelta(minutes=30)


# ---------------------------------------------------------------------------
# Tests: get_historical_features — non-entity mode flow
# ---------------------------------------------------------------------------


@pytest.fixture
def repo_config():
    return _make_repo_config()


@pytest.fixture
def mock_bq_client():
    client = MagicMock()
    client.project = "my-project"
    client.query.return_value = MagicMock(
        state="DONE", exception=lambda timeout=None: None
    )
    client.get_table.return_value = MagicMock()
    return client


class TestGetHistoricalFeaturesNonEntityMode:
    @patch("feast.infra.offline_stores.bigquery.block_until_done")
    @patch("feast.infra.offline_stores.bigquery._utc_now")
    @patch("feast.infra.offline_stores.bigquery._bq_create_entity_union_table")
    @patch("feast.infra.offline_stores.bigquery._upload_entity_df")
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.get_feature_view_query_context"
    )
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.build_point_in_time_query"
    )
    @patch("feast.infra.offline_stores.bigquery._get_table_reference_for_new_entity")
    @patch("feast.infra.offline_stores.bigquery._get_bigquery_client")
    def test_non_entity_mode_calls_union_table_not_upload(
        self,
        mock_get_client,
        mock_get_table_ref,
        mock_build_pit,
        mock_get_ctx,
        mock_upload,
        mock_union_table,
        mock_utc_now,
        mock_block,
        repo_config,
        mock_bq_client,
    ):
        mock_get_client.return_value = mock_bq_client
        mock_get_table_ref.return_value = TABLE_NAME
        mock_build_pit.return_value = "SELECT 1"
        mock_utc_now.return_value = END

        fv = MagicMock()
        fv.batch_source = _make_bq_source()
        fv.ttl = timedelta(days=30)

        ctx = _make_fv_context("fv1", ["customer_id"])
        mock_get_ctx.return_value = [ctx]

        registry = MagicMock()
        registry.list_entities.return_value = []

        job = BigQueryOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["fv1:feature_a"],
            entity_df=None,
            registry=registry,
            project="feast_test",
            full_feature_names=False,
            start_date=START,
            end_date=END,
        )

        # Trigger query_generator
        with job._query_generator():
            pass

        mock_union_table.assert_called_once()
        mock_upload.assert_not_called()

    @patch("feast.infra.offline_stores.bigquery.block_until_done")
    @patch("feast.infra.offline_stores.bigquery._utc_now")
    @patch("feast.infra.offline_stores.bigquery._bq_create_entity_union_table")
    @patch("feast.infra.offline_stores.bigquery._upload_entity_df")
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.get_feature_view_query_context"
    )
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.build_point_in_time_query"
    )
    @patch("feast.infra.offline_stores.bigquery._get_table_reference_for_new_entity")
    @patch("feast.infra.offline_stores.bigquery._get_bigquery_client")
    def test_non_entity_mode_uses_entity_ts_as_timestamp_col(
        self,
        mock_get_client,
        mock_get_table_ref,
        mock_build_pit,
        mock_get_ctx,
        mock_upload,
        mock_union_table,
        mock_utc_now,
        mock_block,
        repo_config,
        mock_bq_client,
    ):
        mock_get_client.return_value = mock_bq_client
        mock_get_table_ref.return_value = TABLE_NAME
        mock_build_pit.return_value = "SELECT 1"
        mock_utc_now.return_value = END

        fv = MagicMock()
        fv.batch_source = _make_bq_source()
        fv.ttl = timedelta(days=30)

        ctx = _make_fv_context("fv1", ["customer_id"])
        mock_get_ctx.return_value = [ctx]

        registry = MagicMock()

        job = BigQueryOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["fv1:feature_a"],
            entity_df=None,
            registry=registry,
            project="feast_test",
            start_date=START,
            end_date=END,
        )

        with job._query_generator():
            pass

        build_call_kwargs = mock_build_pit.call_args[1]
        assert build_call_kwargs["entity_df_event_timestamp_col"] == "entity_ts"

    @patch("feast.infra.offline_stores.bigquery.block_until_done")
    @patch("feast.infra.offline_stores.bigquery._utc_now")
    @patch("feast.infra.offline_stores.bigquery._bq_create_entity_union_table")
    @patch("feast.infra.offline_stores.bigquery._upload_entity_df")
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.get_feature_view_query_context"
    )
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.build_point_in_time_query"
    )
    @patch("feast.infra.offline_stores.bigquery._get_table_reference_for_new_entity")
    @patch("feast.infra.offline_stores.bigquery._get_bigquery_client")
    def test_non_entity_mode_passes_start_and_end_to_union_table(
        self,
        mock_get_client,
        mock_get_table_ref,
        mock_build_pit,
        mock_get_ctx,
        mock_upload,
        mock_union_table,
        mock_utc_now,
        mock_block,
        repo_config,
        mock_bq_client,
    ):
        mock_get_client.return_value = mock_bq_client
        mock_get_table_ref.return_value = TABLE_NAME
        mock_build_pit.return_value = "SELECT 1"
        mock_utc_now.return_value = END

        fv = MagicMock()
        fv.batch_source = _make_bq_source()
        fv.ttl = timedelta(days=30)

        ctx = _make_fv_context("fv1", ["customer_id"])
        mock_get_ctx.return_value = [ctx]

        registry = MagicMock()

        job = BigQueryOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["fv1:feature_a"],
            entity_df=None,
            registry=registry,
            project="feast_test",
            start_date=START,
            end_date=END,
        )

        with job._query_generator():
            pass

        call_kwargs = mock_union_table.call_args[1]
        assert call_kwargs["start_date"].replace(tzinfo=timezone.utc) == START
        assert call_kwargs["end_date"].replace(tzinfo=timezone.utc) == END

    @patch("feast.infra.offline_stores.bigquery.block_until_done")
    @patch("feast.infra.offline_stores.bigquery._utc_now")
    @patch("feast.infra.offline_stores.bigquery._bq_create_entity_union_table")
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.get_feature_view_query_context"
    )
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.build_point_in_time_query"
    )
    @patch("feast.infra.offline_stores.bigquery._get_table_reference_for_new_entity")
    @patch("feast.infra.offline_stores.bigquery._get_bigquery_client")
    def test_non_entity_mode_returns_retrieval_job(
        self,
        mock_get_client,
        mock_get_table_ref,
        mock_build_pit,
        mock_get_ctx,
        mock_union_table,
        mock_utc_now,
        mock_block,
        repo_config,
        mock_bq_client,
    ):
        mock_get_client.return_value = mock_bq_client
        mock_get_table_ref.return_value = TABLE_NAME
        mock_build_pit.return_value = "SELECT 1"
        mock_utc_now.return_value = END

        fv = MagicMock()
        fv.batch_source = _make_bq_source()
        fv.ttl = timedelta(days=30)

        ctx = _make_fv_context("fv1", ["customer_id"])
        mock_get_ctx.return_value = [ctx]

        registry = MagicMock()

        job = BigQueryOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["fv1:feature_a"],
            entity_df=None,
            registry=registry,
            project="feast_test",
            start_date=START,
            end_date=END,
        )

        assert isinstance(job, BigQueryRetrievalJob)

    @patch("feast.infra.offline_stores.bigquery.block_until_done")
    @patch("feast.infra.offline_stores.bigquery._utc_now")
    @patch("feast.infra.offline_stores.bigquery._bq_create_entity_union_table")
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.get_feature_view_query_context"
    )
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.build_point_in_time_query"
    )
    @patch("feast.infra.offline_stores.bigquery._get_table_reference_for_new_entity")
    @patch("feast.infra.offline_stores.bigquery._get_bigquery_client")
    def test_non_entity_mode_metadata_excludes_timestamp_col_from_keys(
        self,
        mock_get_client,
        mock_get_table_ref,
        mock_build_pit,
        mock_get_ctx,
        mock_union_table,
        mock_utc_now,
        mock_block,
        repo_config,
        mock_bq_client,
    ):
        mock_get_client.return_value = mock_bq_client
        mock_get_table_ref.return_value = TABLE_NAME
        mock_build_pit.return_value = "SELECT 1"
        mock_utc_now.return_value = END

        fv = MagicMock()
        fv.batch_source = _make_bq_source()
        fv.ttl = timedelta(days=30)

        ctx = _make_fv_context("fv1", ["customer_id"])
        mock_get_ctx.return_value = [ctx]

        registry = MagicMock()

        job = BigQueryOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["fv1:feature_a"],
            entity_df=None,
            registry=registry,
            project="feast_test",
            start_date=START,
            end_date=END,
        )

        # entity_ts (the as-of timestamp) must NOT appear in the returned keys
        assert "entity_ts" not in job.metadata.keys
        assert "customer_id" in job.metadata.keys


# ---------------------------------------------------------------------------
# Tests: get_historical_features — entity_df mode regression
# ---------------------------------------------------------------------------


class TestGetHistoricalFeaturesEntityDfMode:
    @patch("feast.infra.offline_stores.bigquery._bq_create_entity_union_table")
    @patch("feast.infra.offline_stores.bigquery._upload_entity_df")
    @patch("feast.infra.offline_stores.bigquery._get_entity_df_event_timestamp_range")
    @patch("feast.infra.offline_stores.bigquery._get_entity_schema")
    @patch("feast.infra.offline_stores.bigquery.offline_utils.get_expected_join_keys")
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.assert_expected_columns_in_entity_df"
    )
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.get_feature_view_query_context"
    )
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.build_point_in_time_query"
    )
    @patch("feast.infra.offline_stores.bigquery._get_table_reference_for_new_entity")
    @patch("feast.infra.offline_stores.bigquery._get_bigquery_client")
    def test_entity_df_mode_calls_upload_not_union_table(
        self,
        mock_get_client,
        mock_get_table_ref,
        mock_build_pit,
        mock_get_ctx,
        mock_assert_cols,
        mock_join_keys,
        mock_entity_schema,
        mock_ts_range,
        mock_upload,
        mock_union_table,
    ):
        import pandas as pd

        mock_bq_client = MagicMock()
        mock_bq_client.project = "my-project"
        mock_get_client.return_value = mock_bq_client
        mock_get_table_ref.return_value = TABLE_NAME
        mock_build_pit.return_value = "SELECT 1"
        mock_entity_schema.return_value = {
            "customer_id": "int64",
            "event_timestamp": "datetime64[ns, UTC]",
        }
        mock_ts_range.return_value = (START, END)
        mock_get_ctx.return_value = [_make_fv_context("fv1", ["customer_id"])]
        mock_join_keys.return_value = ["customer_id"]

        entity_df = pd.DataFrame(
            {"customer_id": [1, 2], "event_timestamp": [START, END]}
        )

        repo_config = _make_repo_config()
        fv = MagicMock()
        fv.batch_source = _make_bq_source()

        job = BigQueryOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["fv1:feature_a"],
            entity_df=entity_df,
            registry=MagicMock(),
            project="feast_test",
        )

        with job._query_generator():
            pass

        mock_upload.assert_called_once()
        mock_union_table.assert_not_called()

    @patch("feast.infra.offline_stores.bigquery._bq_create_entity_union_table")
    @patch("feast.infra.offline_stores.bigquery._upload_entity_df")
    @patch("feast.infra.offline_stores.bigquery._get_entity_df_event_timestamp_range")
    @patch("feast.infra.offline_stores.bigquery._get_entity_schema")
    @patch("feast.infra.offline_stores.bigquery.offline_utils.get_expected_join_keys")
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.assert_expected_columns_in_entity_df"
    )
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.get_feature_view_query_context"
    )
    @patch(
        "feast.infra.offline_stores.bigquery.offline_utils.build_point_in_time_query"
    )
    @patch("feast.infra.offline_stores.bigquery._get_table_reference_for_new_entity")
    @patch("feast.infra.offline_stores.bigquery._get_bigquery_client")
    def test_entity_df_sql_string_mode_works(
        self,
        mock_get_client,
        mock_get_table_ref,
        mock_build_pit,
        mock_get_ctx,
        mock_assert_cols,
        mock_join_keys,
        mock_entity_schema,
        mock_ts_range,
        mock_upload,
        mock_union_table,
    ):
        mock_bq_client = MagicMock()
        mock_bq_client.project = "my-project"
        mock_get_client.return_value = mock_bq_client
        mock_get_table_ref.return_value = TABLE_NAME
        mock_build_pit.return_value = "SELECT 1"
        mock_entity_schema.return_value = {
            "customer_id": "int64",
            "event_timestamp": "datetime64[ns, UTC]",
        }
        mock_ts_range.return_value = (START, END)
        mock_get_ctx.return_value = [_make_fv_context("fv1", ["customer_id"])]
        mock_join_keys.return_value = ["customer_id"]

        repo_config = _make_repo_config()
        fv = MagicMock()
        fv.batch_source = _make_bq_source()

        job = BigQueryOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["fv1:feature_a"],
            entity_df="SELECT customer_id, event_timestamp FROM `project.dataset.entities`",
            registry=MagicMock(),
            project="feast_test",
        )

        with job._query_generator():
            pass

        mock_upload.assert_called_once()
        mock_union_table.assert_not_called()
