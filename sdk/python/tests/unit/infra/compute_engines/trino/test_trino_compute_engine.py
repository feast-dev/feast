from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from trino.exceptions import TrinoConnectionError, TrinoQueryError

from feast.aggregation import Aggregation
from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
)
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.trino.compute import (
    TrinoComputeEngine,
    TrinoComputeEngineConfig,
)
from feast.infra.compute_engines.trino.job import (
    TrinoDAGRetrievalJob,
)
from feast.infra.compute_engines.trino.nodes import (
    TrinoAggregationNode,
    TrinoDedupNode,
    TrinoFilterNode,
    TrinoJoinNode,
    TrinoReadNode,
    TrinoTransformationNode,
    TrinoValidationNode,
    TrinoWriteNode,
)
from feast.infra.compute_engines.trino.sql_builder import TrinoQueryPlan
from feast.infra.compute_engines.trino.utils import (
    _has_unquoted_semicolon,
    _trino_sql_literal,
    from_feast_to_trino_type,
    quote_identifier,
    unique_ordered,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino import (
    TrinoOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import (
    Results,
)
from feast.repo_config import get_batch_engine_config_from_type
from feast.transformation.mode import TransformationMode
from feast.transformation.trino_transformation import TrinoTransformation
from feast.types import Array, Float32, Float64, Int64, String


class TestTrinoComputeEngineConfig:
    def test_default_config(self):
        config = TrinoComputeEngineConfig()
        assert config.type in ("trino.engine", "trino")
        assert config.dataset == "feast"
        assert config.port == 8080
        assert config.http_scheme == "http"
        assert config.verify is True
        assert config.batch_size == 10000
        assert config.write_concurrency == 4
        assert config.offline_write_mode == "append"

    def test_custom_config(self):
        config = TrinoComputeEngineConfig(
            host="trino.internal.net",
            port=8443,
            catalog="iceberg",
            user="feast_user",
            http_scheme="https",
            verify=False,
            dataset="feast_staging",
            batch_size=5000,
            write_concurrency=8,
        )
        assert config.host == "trino.internal.net"
        assert config.port == 8443
        assert config.catalog == "iceberg"
        assert config.user == "feast_user"
        assert config.http_scheme == "https"
        assert config.verify is False
        assert config.dataset == "feast_staging"
        assert config.batch_size == 5000
        assert config.write_concurrency == 8

    def test_get_batch_engine_config_from_type(self):
        cls1 = get_batch_engine_config_from_type("trino.engine")
        cls2 = get_batch_engine_config_from_type("trino")
        assert cls1 is TrinoComputeEngineConfig
        assert cls2 is TrinoComputeEngineConfig


class TestTrinoComputeEngine:
    def test_engine_inherits_from_offline_store(self):
        repo_config = MagicMock()
        repo_config.batch_engine = TrinoComputeEngineConfig()  # host=None
        repo_config.offline_store = TrinoOfflineStoreConfig(
            host="offline-trino.internal",
            port=8443,
            catalog="hive",
            user="offline_user",
            dataset="offline_dataset",
            connector={"type": "memory"},
            auth=None,
        )

        engine = TrinoComputeEngine(
            repo_config=repo_config,
            offline_store=MagicMock(),
            online_store=MagicMock(),
        )

        assert engine.config.host == "offline-trino.internal"
        assert engine.config.port == 8443
        assert engine.config.catalog == "hive"
        assert engine.config.user == "offline_user"
        assert engine.config.dataset == "offline_dataset"

    def test_materialize_job_captures_connection_error(self):
        mock_client = MagicMock()
        mock_client._get_cursor.side_effect = TrinoConnectionError("Trino unreachable")

        repo_config = MagicMock()
        repo_config.batch_engine = TrinoComputeEngineConfig(
            host="localhost", catalog="iceberg", user="test"
        )
        repo_config.offline_store = MagicMock()

        engine = TrinoComputeEngine(
            repo_config=repo_config,
            offline_store=MagicMock(),
            online_store=MagicMock(),
        )
        engine.client = mock_client

        task = MagicMock()
        task.feature_view.name = "test_fv"
        task.feature_view.online = True
        task.feature_view.offline = False
        task.feature_view.entities = []

        with patch(
            "feast.infra.compute_engines.trino.compute.TrinoFeatureBuilder"
        ) as mock_builder_cls:
            mock_builder = MagicMock()
            mock_plan = MagicMock()
            mock_plan.execute.side_effect = TrinoConnectionError("Trino unreachable")
            mock_builder.build.return_value = mock_plan
            mock_builder_cls.return_value = mock_builder

            mat_job = engine._materialize_one(MagicMock(), task)
            assert mat_job.status() == MaterializationJobStatus.ERROR
            assert isinstance(mat_job.error(), TrinoConnectionError)


class TestTrinoDAGRetrievalJob:
    def test_retrieval_job_propagates_connection_error(self):
        mock_client = MagicMock()
        mock_client.execute_query.side_effect = TrinoConnectionError(
            "Network disconnected"
        )

        job = TrinoDAGRetrievalJob(
            client=mock_client,
            plan=None,
            context=None,
            full_feature_names=False,
            config=MagicMock(),
            query="SELECT 1",
        )

        with pytest.raises(TrinoConnectionError, match="Network disconnected"):
            job._to_arrow_internal()


class TestTrinoSqlFormattingAndTypes:
    def test_quote_identifier(self):
        assert quote_identifier("user_id") == '"user_id"'
        assert quote_identifier('col"with"quote') == '"col""with""quote"'

    def test_has_unquoted_semicolon(self):
        assert not _has_unquoted_semicolon('"conv_rate" > 0.5')
        assert not _has_unquoted_semicolon("status = 'active;pending'")
        assert not _has_unquoted_semicolon('status = "active;pending"')
        assert not _has_unquoted_semicolon("status = 'escaped\\'quote;still_in_quotes'")
        assert _has_unquoted_semicolon('"conv_rate" > 0.5;')
        assert _has_unquoted_semicolon('"conv_rate" > 0.5;   ')
        assert _has_unquoted_semicolon('"conv_rate" > 0.5; DROP TABLE foo')
        assert _has_unquoted_semicolon(";")

    def test_from_feast_to_trino_type(self):
        assert from_feast_to_trino_type(Int64) == "BIGINT"
        assert from_feast_to_trino_type(Float64) == "DOUBLE"
        assert from_feast_to_trino_type(Float32) == "REAL"
        assert from_feast_to_trino_type(String) == "VARCHAR"
        assert from_feast_to_trino_type(Array(String)) == "ARRAY(VARCHAR)"

    def test_sql_literal_datetime_utc_normalization(self):
        # 1. Naive datetime
        dt = datetime(2025, 1, 15, 12, 0, 0)
        assert _trino_sql_literal(dt) == "TIMESTAMP '2025-01-15 12:00:00.000000'"

        # 2. Timezone-aware datetime with +05:30 offset -> must convert to 06:30:00 UTC
        dt_tz = datetime(
            2025, 1, 15, 12, 0, 0, tzinfo=timezone(timedelta(hours=5, minutes=30))
        )
        assert _trino_sql_literal(dt_tz) == "TIMESTAMP '2025-01-15 06:30:00.000000'"

        # 3. pd.Timestamp with timezone
        ts_tz = pd.Timestamp("2025-01-15 12:00:00+05:30")
        assert _trino_sql_literal(ts_tz) == "TIMESTAMP '2025-01-15 06:30:00.000000'"

        # 4. np.datetime64
        np_dt = np.datetime64("2025-01-15T12:00:00")
        assert _trino_sql_literal(np_dt) == "TIMESTAMP '2025-01-15 12:00:00.000000'"

        # 5. pd.NaT and None
        assert _trino_sql_literal(pd.NaT) == "NULL"
        assert _trino_sql_literal(None) == "NULL"

        # 6. Date
        d = date(2025, 1, 15)
        assert _trino_sql_literal(d) == "DATE '2025-01-15'"

    def test_sql_literal_containers_and_escaping(self):
        # Container arrays should not raise ValueError when checked for nan
        assert _trino_sql_literal([1, 2, 3]) == "ARRAY[1, 2, 3]"
        assert _trino_sql_literal((4, 5)) == "ARRAY[4, 5]"
        assert _trino_sql_literal(np.array([6, 7])) == "ARRAY[6, 7]"

        # Strings with single quotes must be escaped
        assert _trino_sql_literal("O'Connor") == "'O''Connor'"

        # Booleans
        assert _trino_sql_literal(True) == "TRUE"
        assert _trino_sql_literal(False) == "FALSE"
        assert _trino_sql_literal(np.bool_(True)) == "TRUE"


class TestTrinoWriteNode:
    def test_offline_write_staging_swap_failure_preserves_target(self):
        mock_client = MagicMock()
        executed_queries = []

        def mock_exec(query):
            executed_queries.append(query)
            if "CREATE TABLE" in query:
                raise TrinoQueryError(
                    {"message": "Disk full", "errorName": "GENERIC_INTERNAL_ERROR"}
                )
            return Results(data=[], columns=[])

        mock_client.execute_query.side_effect = mock_exec

        fv = MagicMock()
        fv.name = "my_fv"
        fv.online = False
        fv.offline = True
        fv.batch_source.get_table_query_string.return_value = (
            "iceberg.feast.my_fv_offline"
        )

        node = TrinoWriteNode(
            name="output",
            feature_view=fv,
            client=mock_client,
        )

        plan = TrinoQueryPlan(
            ctes=[("_source", "SELECT 1")],
            current_from="_source",
        )
        context = MagicMock()
        context.repo_config.batch_engine.offline_write_mode = "overwrite"
        context.node_outputs = {}

        input_node = MagicMock()
        input_node.name = "upstream"
        node.inputs = [input_node]
        context.node_outputs["upstream"] = DAGValue(data=plan, format=DAGFormat.TRINO)

        with pytest.raises(TrinoQueryError):
            node.execute(context)

        # Target table must NEVER have been dropped
        assert not any(
            "DROP TABLE IF EXISTS iceberg.feast.my_fv_offline\n" in q
            for q in executed_queries
        )
        assert not any(
            q.strip() == "DROP TABLE IF EXISTS iceberg.feast.my_fv_offline"
            for q in executed_queries
        )
        # Staging table cleanup must have been called
        assert any(
            'DROP TABLE IF EXISTS iceberg.feast."my_fv_offline__staging"' in q
            for q in executed_queries
        )


class TestTrinoDAGCompilation:
    """Verifies that all DAG nodes compile into Trino SQL CTEs."""

    def test_trino_read_node_generates_sql(self):
        mock_source = MagicMock()
        mock_source.get_table_query_string.return_value = "iceberg.feast.driver_stats"

        col_info = ColumnInfo(
            join_keys=["driver_id"],
            feature_cols=["conv_rate", "acc_rate"],
            ts_col="event_timestamp",
            created_ts_col=None,
            field_mapping={"conv_rate": "conversion_rate"},
        )

        start_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

        node = TrinoReadNode(
            name="test_read",
            source=mock_source,
            column_info=col_info,
            client=MagicMock(),
            start_time=start_time,
            end_time=end_time,
        )

        context = MagicMock()
        val = node.execute(context)
        assert val.format == DAGFormat.TRINO
        plan: TrinoQueryPlan = val.data

        sql = plan.to_sql()
        assert "WITH\n_source_test_read AS (" in sql
        assert '"driver_id"' in sql
        assert '"conv_rate" AS "conversion_rate"' in sql
        assert "TIMESTAMP '2025-01-01 00:00:00.000000'" in sql
        assert "TIMESTAMP '2025-01-02 00:00:00.000000'" in sql
        assert "FROM iceberg.feast.driver_stats" in sql

    def test_trino_filter_node_generates_sql(self):
        plan = TrinoQueryPlan(
            ctes=[("_source", 'SELECT * FROM "iceberg"."feast"."driver_stats"')],
            current_from="_source",
            columns=("driver_id", "event_timestamp", "__entity_event_timestamp"),
            timestamp_col="event_timestamp",
            metadata={"entity_joined": True},
        )
        input_node = MagicMock()
        input_node.name = "source"

        col_info = ColumnInfo(
            join_keys=["driver_id"],
            feature_cols=[],
            ts_col="event_timestamp",
            created_ts_col=None,
        )
        node = TrinoFilterNode(
            name="filter",
            column_info=col_info,
            client=MagicMock(),
            ttl=timedelta(days=1),
            filter_condition='"conv_rate" > 0.5',
            inputs=[input_node],
        )

        context = MagicMock()
        context.node_outputs = {"source": DAGValue(data=plan, format=DAGFormat.TRINO)}

        val = node.execute(context)
        sql = val.data.to_sql()

        assert "_filter_filter AS (" in sql
        assert '"event_timestamp" <= "__entity_event_timestamp"' in sql
        assert "INTERVAL '86400' SECOND" in sql
        assert '("conv_rate" > 0.5)' in sql

    def test_trino_filter_node_semicolon_handling(self):
        plan = TrinoQueryPlan(
            ctes=[("_source", 'SELECT * FROM "iceberg"."feast"."driver_stats"')],
            current_from="_source",
            columns=("driver_id", "conv_rate"),
        )
        input_node = MagicMock()
        input_node.name = "source"

        col_info = ColumnInfo(
            join_keys=["driver_id"],
            feature_cols=["conv_rate"],
            ts_col=None,
            created_ts_col=None,
        )

        # 1. Unquoted semicolon must raise ValueError
        node_invalid = TrinoFilterNode(
            name="filter",
            column_info=col_info,
            client=MagicMock(),
            filter_condition='"conv_rate" > 0.5;',
            inputs=[input_node],
        )
        context = MagicMock()
        context.node_outputs = {"source": DAGValue(data=plan, format=DAGFormat.TRINO)}

        with pytest.raises(ValueError, match="prohibited semicolon"):
            node_invalid.execute(context)

        # 2. Semicolon inside string literal is permitted
        node_valid = TrinoFilterNode(
            name="filter",
            column_info=col_info,
            client=MagicMock(),
            filter_condition="status = 'foo;bar'",
            inputs=[input_node],
        )
        val = node_valid.execute(context)
        sql = val.data.to_sql()

        assert "WHERE (status = 'foo;bar')" in sql

    def test_trino_dedup_node_generates_row_number_sql(self):
        plan = TrinoQueryPlan(
            ctes=[("_source", 'SELECT * FROM "iceberg"."feast"."driver_stats"')],
            current_from="_source",
            join_keys=["driver_id"],
            timestamp_col="event_timestamp",
            created_timestamp_col="created_timestamp",
        )
        input_node = MagicMock()
        input_node.name = "source"

        col_info = ColumnInfo(
            join_keys=["driver_id"],
            feature_cols=[],
            ts_col="event_timestamp",
            created_ts_col="created_timestamp",
        )
        node = TrinoDedupNode(
            name="dedup",
            column_info=col_info,
            client=MagicMock(),
            inputs=[input_node],
        )

        context = MagicMock()
        context.node_outputs = {"source": DAGValue(data=plan, format=DAGFormat.TRINO)}

        val = node.execute(context)
        sql = val.data.to_sql()

        assert "ROW_NUMBER() OVER (" in sql
        assert 'PARTITION BY "driver_id"' in sql
        assert 'ORDER BY "event_timestamp" DESC, "created_timestamp" DESC' in sql
        assert "_feast_rn = 1" in sql
        # Assert _feast_rn column does NOT leak into output projection
        assert "SELECT * FROM (" not in sql
        assert (
            'SELECT "driver_id", "event_timestamp", "created_timestamp" FROM (' in sql
        )
        assert "_feast_rn" not in val.data.columns

    def test_trino_dedup_node_uses_upstream_plan_columns(self):
        plan = TrinoQueryPlan(
            ctes=[
                (
                    "_source",
                    'SELECT "driver_id", "event_timestamp", "conv_rate" FROM "driver_stats"',
                )
            ],
            current_from="_source",
            columns=("driver_id", "event_timestamp", "conv_rate"),
            join_keys=["driver_id"],
            timestamp_col="event_timestamp",
        )
        input_node = MagicMock()
        input_node.name = "source"

        col_info = ColumnInfo(
            join_keys=["driver_id"],
            feature_cols=["conv_rate"],
            ts_col="event_timestamp",
            created_ts_col=None,
        )
        node = TrinoDedupNode(
            name="dedup",
            column_info=col_info,
            client=MagicMock(),
            inputs=[input_node],
        )

        context = MagicMock()
        context.node_outputs = {"source": DAGValue(data=plan, format=DAGFormat.TRINO)}

        val = node.execute(context)
        sql = val.data.to_sql()

        assert 'SELECT "driver_id", "event_timestamp", "conv_rate" FROM (' in sql
        assert "_feast_rn" not in val.data.columns
        assert val.data.columns == ("driver_id", "event_timestamp", "conv_rate")

    def test_trino_aggregation_node_generates_group_by_sql(self):
        plan = TrinoQueryPlan(
            ctes=[("_source", 'SELECT * FROM "iceberg"."feast"."driver_stats"')],
            current_from="_source",
            join_keys=["driver_id"],
            timestamp_col="event_timestamp",
        )
        input_node = MagicMock()
        input_node.name = "source"

        aggs = [
            Aggregation(column="conv_rate", function="sum"),
            Aggregation(column="driver_id", function="count_distinct"),
        ]

        node = TrinoAggregationNode(
            name="agg",
            aggregations=aggs,
            group_by_keys=["driver_id"],
            timestamp_col="event_timestamp",
            client=MagicMock(),
            inputs=[input_node],
        )

        context = MagicMock()
        context.node_outputs = {"source": DAGValue(data=plan, format=DAGFormat.TRINO)}

        val = node.execute(context)
        sql = val.data.to_sql()

        assert "GROUP BY" in sql
        assert 'SUM("conv_rate") AS "sum_conv_rate"' in sql
        assert 'approx_distinct("driver_id") AS "count_distinct_driver_id"' in sql

    def test_trino_transformation_node_generates_sql(self):
        plan = TrinoQueryPlan(
            ctes=[("_source", 'SELECT * FROM "iceberg"."feast"."driver_stats"')],
            current_from="_source",
            join_keys=["driver_id"],
        )
        input_node = MagicMock()
        input_node.name = "source"

        def double_sql(src_table):
            return (
                f'SELECT "driver_id", "conv_rate" * 2.0 AS "conv_rate" FROM {src_table}'
            )

        node = TrinoTransformationNode(
            name="transform",
            udf=double_sql,
            inputs=[input_node],
        )

        context = MagicMock()
        context.node_outputs = {"source": DAGValue(data=plan, format=DAGFormat.TRINO)}

        val = node.execute(context)
        sql = val.data.to_sql()

        assert "_transform_transform AS (" in sql
        assert (
            'SELECT "driver_id", "conv_rate" * 2.0 AS "conv_rate" FROM _source' in sql
        )

    def test_trino_transformation_class(self):
        tf = TrinoTransformation(
            mode=TransformationMode.TRINO_SQL,
            udf='SELECT "driver_id", "conv_rate" * 2.0 AS "conv_rate" FROM {}',
            udf_string='SELECT "driver_id", "conv_rate" * 2.0 AS "conv_rate" FROM {}',
        )
        res = tf.transform("upstream_step")
        assert (
            'SELECT "driver_id", "conv_rate" * 2.0 AS "conv_rate" FROM upstream_step'
            in res
        )


class TestTrinoQueryPlanImmutabilityAndFallbacks:
    def test_empty_query_plan_and_fallbacks(self):
        empty_plan = TrinoQueryPlan()
        assert empty_plan.to_sql() == "SELECT 1"

        plan_from = TrinoQueryPlan(current_from="my_table", columns=("id", "val"))
        assert plan_from.to_sql() == "SELECT id, val FROM my_table"
        assert plan_from.get_latest_cte_name() == "my_table"

    def test_query_plan_is_frozen(self):
        from dataclasses import FrozenInstanceError

        plan = TrinoQueryPlan(current_from="t1")
        with pytest.raises(FrozenInstanceError):
            plan.current_from = "t2"  # type: ignore


class TestTrinoJoinNodeExecution:
    def test_join_multiple_plans_with_keys(self):
        plan1 = TrinoQueryPlan(ctes=(("_p1", "SELECT 1"),), current_from="_p1")
        plan2 = TrinoQueryPlan(ctes=(("_p2", "SELECT 2"),), current_from="_p2")
        col_info = ColumnInfo(
            join_keys=["entity_id"], feature_cols=[], ts_col="ts", created_ts_col=None
        )

        n1 = MagicMock()
        n1.name = "n1"
        n2 = MagicMock()
        n2.name = "n2"

        join_node = TrinoJoinNode(
            "join_test", col_info, client=MagicMock(), inputs=[n1, n2]
        )
        ctx = ExecutionContext(
            repo_config=MagicMock(),
            project="p",
            offline_store=MagicMock(),
            online_store=MagicMock(),
            entity_defs=[],
        )
        ctx.node_outputs = {
            "n1": DAGValue(data=plan1, format=DAGFormat.TRINO),
            "n2": DAGValue(data=plan2, format=DAGFormat.TRINO),
        }

        val = join_node.execute(ctx)
        sql = val.data.to_sql()
        assert 'LEFT JOIN _p2 ON _p1."entity_id" = _p2."entity_id"' in sql

    def test_join_without_keys_emits_cross_join(self):
        plan1 = TrinoQueryPlan(ctes=(("_p1", "SELECT 1"),), current_from="_p1")
        plan2 = TrinoQueryPlan(ctes=(("_p2", "SELECT 2"),), current_from="_p2")
        col_info = ColumnInfo(
            join_keys=[], feature_cols=[], ts_col=None, created_ts_col=None
        )

        n1 = MagicMock()
        n1.name = "n1"
        n2 = MagicMock()
        n2.name = "n2"

        join_node = TrinoJoinNode(
            "join_test", col_info, client=MagicMock(), inputs=[n1, n2]
        )
        ctx = ExecutionContext(
            repo_config=MagicMock(),
            project="p",
            offline_store=MagicMock(),
            online_store=MagicMock(),
            entity_defs=[],
        )
        ctx.node_outputs = {
            "n1": DAGValue(data=plan1, format=DAGFormat.TRINO),
            "n2": DAGValue(data=plan2, format=DAGFormat.TRINO),
        }

        val = join_node.execute(ctx)
        sql = val.data.to_sql()
        assert "CROSS JOIN _p2" in sql

    def test_join_with_entity_df_pandas(self):
        plan1 = TrinoQueryPlan(
            ctes=(("_p1", "SELECT 1"),), current_from="_p1", timestamp_col="ts"
        )
        col_info = ColumnInfo(
            join_keys=["user_id"], feature_cols=[], ts_col="ts", created_ts_col=None
        )
        n1 = MagicMock()
        n1.name = "n1"

        client = MagicMock()
        join_node = TrinoJoinNode("join_entity", col_info, client=client, inputs=[n1])

        df_entity = pd.DataFrame(
            {"user_id": [1, 2], "event_timestamp": [datetime.now(), datetime.now()]}
        )
        ctx = ExecutionContext(
            repo_config=MagicMock(),
            project="p",
            offline_store=MagicMock(),
            online_store=MagicMock(),
            entity_defs=[],
        )
        ctx.entity_df = df_entity
        ctx.node_outputs = {"n1": DAGValue(data=plan1, format=DAGFormat.TRINO)}

        with patch(
            "feast.infra.compute_engines.trino.nodes.upload_pandas_dataframe_to_trino"
        ) as mock_upload:
            val = join_node.execute(ctx)
            mock_upload.assert_called_once()
            sql = val.data.to_sql()
            assert (
                'LEFT JOIN _join_join_entity ON _join_join_entity."user_id" = _entity."user_id"'
                in sql
            )
            assert '_join_join_entity."ts" <= _entity."__entity_event_timestamp"' in sql


class TestTrinoTransformationNodeVariations:
    def test_string_template_udf(self):
        plan = TrinoQueryPlan(ctes=(("_src", "SELECT 1"),), current_from="_src")
        n1 = MagicMock()
        n1.name = "n1"

        node = TrinoTransformationNode(
            "tf_str", udf="SELECT a * 2 FROM {}", inputs=[n1]
        )
        ctx = ExecutionContext(
            repo_config=MagicMock(),
            project="p",
            offline_store=MagicMock(),
            online_store=MagicMock(),
            entity_defs=[],
        )
        ctx.node_outputs = {"n1": DAGValue(data=plan, format=DAGFormat.TRINO)}

        val = node.execute(ctx)
        assert "SELECT a * 2 FROM _src" in val.data.to_sql()

    def test_udf_string_fallback(self):
        plan = TrinoQueryPlan(ctes=(("_src", "SELECT 1"),), current_from="_src")
        n1 = MagicMock()
        n1.name = "n1"

        node = TrinoTransformationNode(
            "tf_fallback", udf=None, inputs=[n1], udf_string="SELECT x FROM {}"
        )
        ctx = ExecutionContext(
            repo_config=MagicMock(),
            project="p",
            offline_store=MagicMock(),
            online_store=MagicMock(),
            entity_defs=[],
        )
        ctx.node_outputs = {"n1": DAGValue(data=plan, format=DAGFormat.TRINO)}

        val = node.execute(ctx)
        assert "SELECT x FROM _src" in val.data.to_sql()

    def test_missing_udf_raises(self):
        plan = TrinoQueryPlan(ctes=(("_src", "SELECT 1"),), current_from="_src")
        n1 = MagicMock()
        n1.name = "n1"

        node = TrinoTransformationNode("tf_empty", udf=None, inputs=[n1], udf_string="")
        ctx = ExecutionContext(
            repo_config=MagicMock(),
            project="p",
            offline_store=MagicMock(),
            online_store=MagicMock(),
            entity_defs=[],
        )
        ctx.node_outputs = {"n1": DAGValue(data=plan, format=DAGFormat.TRINO)}

        with pytest.raises(ValueError, match="requires SQL UDF string or callable"):
            node.execute(ctx)


class TestTrinoValidationNode:
    def test_validation_node_execute(self):
        plan = TrinoQueryPlan(ctes=(("_src", "SELECT 1"),), current_from="_src")
        n1 = MagicMock()
        n1.name = "n1"

        node = TrinoValidationNode(
            "val_node",
            expected_columns={"a": "VARCHAR"},
            json_columns={"a"},
            inputs=[n1],
        )
        ctx = ExecutionContext(
            repo_config=MagicMock(),
            project="p",
            offline_store=MagicMock(),
            online_store=MagicMock(),
            entity_defs=[],
        )
        ctx.node_outputs = {"n1": DAGValue(data=plan, format=DAGFormat.TRINO)}

        val = node.execute(ctx)
        assert ctx.node_outputs["val_node"] == val
        assert val.data == plan


class TestTrinoAggregationNodeVariations:
    def test_aggregation_without_time_window(self):
        plan = TrinoQueryPlan(ctes=(("_src", "SELECT 1"),), current_from="_src")
        n1 = MagicMock()
        n1.name = "n1"

        aggs = [
            Aggregation(column="val", function="avg"),
            Aggregation(column="val", function="min"),
            Aggregation(column="val", function="max"),
            Aggregation(column="val", function="count"),
            Aggregation(column="val", function="custom_func"),
        ]
        node = TrinoAggregationNode(
            name="agg_no_window",
            aggregations=aggs,
            group_by_keys=["user_id"],
            timestamp_col="ts",
            client=MagicMock(),
            inputs=[n1],
        )
        ctx = ExecutionContext(
            repo_config=MagicMock(),
            project="p",
            offline_store=MagicMock(),
            online_store=MagicMock(),
            entity_defs=[],
        )
        ctx.node_outputs = {"n1": DAGValue(data=plan, format=DAGFormat.TRINO)}

        val = node.execute(ctx)
        sql = val.data.to_sql()
        assert 'MAX("ts") AS "ts"' in sql
        assert 'AVG("val")' in sql
        assert 'MIN("val")' in sql
        assert 'MAX("val")' in sql
        assert 'COUNT("val")' in sql
        assert 'CUSTOM_FUNC("val")' in sql


class TestTrinoTypeMappingComprehensive:
    def test_complex_types(self):
        from feast.types import PrimitiveFeastType, Set, Struct

        assert from_feast_to_trino_type(PrimitiveFeastType.BYTES) == "VARBINARY"
        assert from_feast_to_trino_type(PrimitiveFeastType.BOOL) == "BOOLEAN"
        assert (
            from_feast_to_trino_type(PrimitiveFeastType.UNIX_TIMESTAMP) == "TIMESTAMP"
        )
        assert (
            from_feast_to_trino_type(PrimitiveFeastType.MAP) == "MAP(VARCHAR, VARCHAR)"
        )

        struct_t = Struct(
            {"x": PrimitiveFeastType.INT32, "y": PrimitiveFeastType.STRING}
        )
        assert from_feast_to_trino_type(struct_t) == 'ROW("x" INTEGER, "y" VARCHAR)'

        arr_struct = Array(struct_t)
        assert (
            from_feast_to_trino_type(arr_struct)
            == 'ARRAY(ROW("x" INTEGER, "y" VARCHAR))'
        )

        arr_map = Array(PrimitiveFeastType.MAP)
        assert from_feast_to_trino_type(arr_map) == "ARRAY(MAP(VARCHAR, VARCHAR))"

        set_str = Set(PrimitiveFeastType.STRING)
        assert from_feast_to_trino_type(set_str) == "ARRAY(VARCHAR)"


class TestTrinoSqlLiteralComprehensive:
    def test_literals(self):
        assert _trino_sql_literal(None) == "NULL"
        assert _trino_sql_literal(True) == "TRUE"
        assert _trino_sql_literal(False) == "FALSE"
        assert _trino_sql_literal(b"hello") == "b'hello'"
        assert _trino_sql_literal(bytearray(b"abc")) == "bytearray(b'abc')"
        assert _trino_sql_literal(date(2025, 5, 10)) == "DATE '2025-05-10'"
        assert _trino_sql_literal(float("nan")) == "NULL"
        assert _trino_sql_literal([1, 2, 3]) == "ARRAY[1, 2, 3]"
        assert _trino_sql_literal({"k": "v"}) == "{'k': 'v'}"


class TestTrinoFeatureBuilderDirect:
    def test_builder_methods(self):
        from feast.infra.compute_engines.trino.feature_builder import (
            TrinoFeatureBuilder,
        )
        from feast.types import PrimitiveFeastType

        client = MagicMock()
        registry = MagicMock()
        task = MagicMock()
        task.start_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
        task.end_time = datetime(2025, 1, 2, tzinfo=timezone.utc)
        task.project = "test_p"

        view = MagicMock()
        view.name = "v1"
        view.batch_source = MagicMock()
        view.batch_source.timestamp_field = "ts"
        view.batch_source.field_mapping = {}
        view.stream_source = None
        view.entities = []
        view.features = [MagicMock(name="f1", dtype=PrimitiveFeastType.STRING)]
        view.features[0].name = "f1"
        view.aggregations = []
        view.filter = None
        view.ttl = None

        with patch(
            "feast.infra.compute_engines.feature_builder._get_column_names",
            return_value=([], ["f1"], "ts", None),
        ):
            builder = TrinoFeatureBuilder(registry, client, task)
            src_node = builder.build_source_node(view)
            assert src_node.name == "v1:source"

            filt_node = builder.build_filter_node(view, src_node)
            assert filt_node.name == "v1:filter"

            dedup_node = builder.build_dedup_node(view, filt_node)
            assert dedup_node.name == "v1:dedup"

            join_node = builder.build_join_node(view, [src_node])
            assert join_node.name == "v1:join"

            val_node = builder.build_validation_node(view, src_node)
            assert val_node.name == "v1:validate"

    def test_should_join_entity_df(self):
        from feast.infra.common.materialization_job import MaterializationTask
        from feast.infra.common.retrieval_task import HistoricalRetrievalTask
        from feast.infra.compute_engines.trino.feature_builder import (
            TrinoFeatureBuilder,
        )

        client = MagicMock()
        registry = MagicMock()

        # 1. MaterializationTask -> False
        mat_task = MagicMock(spec=MaterializationTask)
        mat_task.feature_view = MagicMock()
        with patch(
            "feast.infra.compute_engines.feature_builder._get_column_names",
            return_value=([], [], "ts", None),
        ):
            b_mat = TrinoFeatureBuilder(registry, client, mat_task)
            assert b_mat._should_join_entity_df() is False

        # 2. HistoricalRetrievalTask with DataFrame -> True
        ret_task_df = MagicMock(spec=HistoricalRetrievalTask)
        ret_task_df.feature_view = MagicMock()
        ret_task_df.entity_df = pd.DataFrame({"id": [1]})
        with patch(
            "feast.infra.compute_engines.feature_builder._get_column_names",
            return_value=([], [], "ts", None),
        ):
            b_df = TrinoFeatureBuilder(registry, client, ret_task_df)
            assert b_df._should_join_entity_df() is True

        # 3. HistoricalRetrievalTask with SQL query string -> True
        ret_task_sql = MagicMock(spec=HistoricalRetrievalTask)
        ret_task_sql.feature_view = MagicMock()
        ret_task_sql.entity_df = "SELECT * FROM entities"
        with patch(
            "feast.infra.compute_engines.feature_builder._get_column_names",
            return_value=([], [], "ts", None),
        ):
            b_sql = TrinoFeatureBuilder(registry, client, ret_task_sql)
            assert b_sql._should_join_entity_df() is True

        # 4. HistoricalRetrievalTask with empty / whitespace string -> False
        ret_task_empty = MagicMock(spec=HistoricalRetrievalTask)
        ret_task_empty.feature_view = MagicMock()
        ret_task_empty.entity_df = "   "
        with patch(
            "feast.infra.compute_engines.feature_builder._get_column_names",
            return_value=([], [], "ts", None),
        ):
            b_empty = TrinoFeatureBuilder(registry, client, ret_task_empty)
            assert b_empty._should_join_entity_df() is False

        # 5. HistoricalRetrievalTask with None -> False
        ret_task_none = MagicMock(spec=HistoricalRetrievalTask)
        ret_task_none.feature_view = MagicMock()
        ret_task_none.entity_df = None
        with patch(
            "feast.infra.compute_engines.feature_builder._get_column_names",
            return_value=([], [], "ts", None),
        ):
            b_none = TrinoFeatureBuilder(registry, client, ret_task_none)
            assert b_none._should_join_entity_df() is False

    def test_build_single_feature_view_with_entity_df_injects_join_node(self):
        from feast.infra.common.retrieval_task import HistoricalRetrievalTask
        from feast.infra.compute_engines.trino.feature_builder import (
            TrinoFeatureBuilder,
        )
        from feast.infra.compute_engines.trino.nodes import (
            TrinoDedupNode,
            TrinoFilterNode,
            TrinoJoinNode,
            TrinoReadNode,
        )

        client = MagicMock()
        registry = MagicMock()

        view = MagicMock()
        view.name = "single_fv"
        view.batch_source = MagicMock()
        view.batch_source.timestamp_field = "ts"
        view.batch_source.field_mapping = {}
        view.stream_source = None
        view.entities = ["id"]
        view.features = []
        view.aggregations = []
        view.filter = None
        view.ttl = None
        view.feature_transformation = None
        view.enable_validation = False

        # When entity_df is provided for HistoricalRetrievalTask:
        task = MagicMock(spec=HistoricalRetrievalTask)
        task.feature_view = view
        task.entity_df = pd.DataFrame({"id": [1], "ts": [datetime.now()]})
        task.project = "test_p"
        task.start_time = None
        task.end_time = None

        with patch(
            "feast.infra.compute_engines.feature_builder._get_column_names",
            return_value=(["id"], [], "ts", None),
        ):
            builder = TrinoFeatureBuilder(registry, client, task)
            last_node = builder._build(view, input_nodes=None)

            # Traverse the DAG inputs backwards from the final dedup node
            assert isinstance(last_node, TrinoDedupNode)
            filter_node = last_node.inputs[0]
            assert isinstance(filter_node, TrinoFilterNode)
            join_node = filter_node.inputs[0]
            assert isinstance(join_node, TrinoJoinNode)
            source_node = join_node.inputs[0]
            assert isinstance(source_node, TrinoReadNode)


class TestTrinoJobsAndEngineMethods:
    def test_materialization_job_properties(self):
        from feast.infra.compute_engines.trino.job import (
            TrinoMaterializationJob,
        )

        job = TrinoMaterializationJob(
            job_id="mat_1", status=MaterializationJobStatus.SUCCEEDED
        )
        assert job.job_id() == "mat_1"
        assert job.url() is None
        assert job.status() == MaterializationJobStatus.SUCCEEDED
        assert job.error() is None
        assert job.should_be_retried() is False

    def test_retrieval_job_properties(self):
        job = TrinoDAGRetrievalJob(
            client=MagicMock(),
            plan=MagicMock(),
            context=MagicMock(),
            full_feature_names=False,
            config=MagicMock(),
        )
        assert job.error() is None
        assert job.full_feature_names is False
        assert job.on_demand_feature_views == []
        assert job.metadata is None

    def test_engine_infrastructure_no_ops(self):
        engine = TrinoComputeEngine(
            repo_config=MagicMock(),
            offline_store=MagicMock(),
            online_store=MagicMock(),
        )
        engine.update("p", [], [], [], [])
        engine.teardown_infra("p", [], [])


class TestTrinoTransformationComprehensive:
    def test_mode_as_string(self):
        tf = TrinoTransformation(
            mode="trino_sql",
            udf="SELECT * FROM {}",
        )
        assert tf.mode == TransformationMode.TRINO_SQL
        assert tf.transform("source_tbl") == "SELECT * FROM source_tbl"

    def test_callable_udf(self):
        def my_udf(t):
            return f"SELECT id FROM {t}"

        tf = TrinoTransformation(udf=my_udf)
        assert tf.transform("t1") == "SELECT id FROM t1"

    def test_static_sql_udf_without_template(self):
        tf = TrinoTransformation(udf="SELECT 1")
        assert tf.transform("t1") == "SELECT 1"

    def test_udf_string_fallback(self):
        tf = TrinoTransformation(udf=None, udf_string="SELECT * FROM {}")
        assert tf.transform("tbl") == "SELECT * FROM tbl"

        tf_static = TrinoTransformation(udf=None, udf_string="SELECT 42")
        assert tf_static.transform("tbl") == "SELECT 42"

    def test_invalid_transformation_raises(self):
        tf = TrinoTransformation(udf=None, udf_string="")
        with pytest.raises(ValueError, match="Invalid TrinoTransformation"):
            tf.transform("tbl")

    def test_infer_features_noop(self):
        tf = TrinoTransformation(udf="SELECT 1")
        assert tf.infer_features() is None


class TestTrinoDAGRetrievalJobComprehensive:
    def test_to_sql_with_string_result(self):
        mock_plan = MagicMock()
        mock_plan.execute.return_value = MagicMock(data="SELECT 123")
        job = TrinoDAGRetrievalJob(
            client=MagicMock(),
            plan=mock_plan,
            context=MagicMock(),
            full_feature_names=False,
            config=MagicMock(),
        )
        assert job.to_sql() == "SELECT 123"

    def test_ensure_executed_with_preexisting_error(self):
        err = RuntimeError("Pre-existing retrieval error")
        job = TrinoDAGRetrievalJob(
            client=MagicMock(),
            plan=None,
            context=None,
            full_feature_names=False,
            config=MagicMock(),
            error=err,
        )
        with pytest.raises(RuntimeError, match="Pre-existing retrieval error"):
            job.to_arrow()

    def test_ensure_executed_caching(self):
        mock_client = MagicMock()
        df = pd.DataFrame({"x": [1, 2, 3]})
        mock_results = MagicMock()
        mock_results.to_dataframe.return_value = df
        mock_client.execute_query.return_value = mock_results

        job = TrinoDAGRetrievalJob(
            client=mock_client,
            plan=None,
            context=None,
            full_feature_names=False,
            config=MagicMock(),
            query="SELECT x FROM tbl",
        )
        t1 = job.to_arrow()
        t2 = job.to_arrow()
        assert t1 == t2
        assert mock_client.execute_query.call_count == 1

    def test_ensure_executed_no_client_raises(self):
        job = TrinoDAGRetrievalJob(
            client=None,
            plan=None,
            context=None,
            full_feature_names=False,
            config=MagicMock(),
            query="SELECT 1",
        )
        with pytest.raises(
            RuntimeError, match="No Trino client or SQL query available"
        ):
            job.to_arrow()

    def test_ensure_executed_trino_query_error(self):
        mock_client = MagicMock()
        mock_client.execute_query.side_effect = TrinoQueryError(
            {"message": "syntax error"}
        )
        job = TrinoDAGRetrievalJob(
            client=mock_client,
            plan=None,
            context=None,
            full_feature_names=False,
            config=MagicMock(),
            query="SELECT syntax error",
        )
        with pytest.raises(TrinoQueryError):
            job.to_arrow()

    def test_to_df(self):
        mock_client = MagicMock()
        df = pd.DataFrame({"a": [10, 20]})
        mock_results = MagicMock()
        mock_results.to_dataframe.return_value = df
        mock_client.execute_query.return_value = mock_results

        job = TrinoDAGRetrievalJob(
            client=mock_client,
            plan=None,
            context=None,
            full_feature_names=False,
            config=MagicMock(),
            query="SELECT a FROM tbl",
        )
        result_df = job.to_df()
        assert list(result_df["a"]) == [10, 20]

    def test_persist(self):
        from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import (
            SavedDatasetTrinoStorage,
        )

        mock_client = MagicMock()
        job = TrinoDAGRetrievalJob(
            client=mock_client,
            plan=None,
            context=None,
            full_feature_names=False,
            config=MagicMock(),
            query="SELECT 1 AS a",
        )
        with pytest.raises(ValueError, match="Expected `SavedDatasetTrinoStorage`"):
            job.persist(MagicMock())

        storage = SavedDatasetTrinoStorage(table="catalog.schema.persisted_tbl")
        job.persist(storage, allow_overwrite=True)
        assert mock_client.execute_query.call_count == 2


class TestTrinoComputeEngineAdditional:
    def test_engine_init_with_dict_and_str_config(self):
        repo_conf_dict = MagicMock()
        repo_conf_dict.batch_engine = {"type": "trino.engine", "port": 9090}
        repo_conf_dict.offline_store = MagicMock()
        engine_dict = TrinoComputeEngine(
            repo_config=repo_conf_dict,
            offline_store=MagicMock(),
            online_store=MagicMock(),
        )
        assert engine_dict.config.port == 9090

        repo_conf_str = MagicMock()
        repo_conf_str.batch_engine = "trino"
        repo_conf_str.offline_store = MagicMock()
        engine_str = TrinoComputeEngine(
            repo_config=repo_conf_str,
            offline_store=MagicMock(),
            online_store=MagicMock(),
        )
        assert engine_str.config.port == 8080

    def test_materialize_one_trino_clint_not_configured(self):
        engine = TrinoComputeEngine(
            repo_config=MagicMock(),
            offline_store=MagicMock(),
            online_store=MagicMock(),
        )
        task = MagicMock()
        task.feature_view.name = "err_fv"
        task.start_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
        task.end_time = datetime(2025, 1, 2, tzinfo=timezone.utc)
        registry = MagicMock()

        job = engine._materialize_one(registry, task)
        assert job.status() == MaterializationJobStatus.ERROR
        assert "Trino client is not configured." in str(job.error())

    def test_materialize_from_offline_store(self):
        repo_cfg = MagicMock()
        repo_cfg.batch_engine = TrinoComputeEngineConfig()
        repo_cfg.offline_store = MagicMock()
        mock_offline = MagicMock()
        mock_online = MagicMock()

        engine = TrinoComputeEngine(
            repo_config=repo_cfg,
            offline_store=mock_offline,
            online_store=mock_online,
        )
        engine.client = MagicMock()

        registry = MagicMock()
        fv = MagicMock()
        fv.name = "legacy_fv"
        fv.entities = []
        fv.batch_source = MagicMock()

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 2, tzinfo=timezone.utc)

        with (
            patch(
                "feast.infra.compute_engines.trino.compute._get_column_names",
                return_value=([], ["f1"], "ts", None),
            ),
            patch(
                "feast.infra.compute_engines.trino.compute.stream_trino_arrow_batches",
                return_value=[],
            ),
            patch(
                "feast.infra.compute_engines.trino.compute.write_arrow_batches_to_online_store",
            ),
        ):
            job = engine._materialize_from_offline_store(
                registry=registry,
                feature_view=fv,
                start_date=start,
                end_date=end,
                project="proj",
            )
            assert job.status() == MaterializationJobStatus.SUCCEEDED

    def test_materialize_from_offline_store_error(self):
        repo_cfg = MagicMock()
        repo_cfg.batch_engine = TrinoComputeEngineConfig()
        repo_cfg.offline_store = MagicMock()

        engine = TrinoComputeEngine(
            repo_config=repo_cfg,
            offline_store=MagicMock(),
            online_store=MagicMock(),
        )
        engine.client = None

        registry = MagicMock()
        fv = MagicMock()
        fv.name = "legacy_fv_err"
        fv.entities = []
        fv.batch_source = MagicMock()

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 2, tzinfo=timezone.utc)

        with patch(
            "feast.infra.compute_engines.trino.compute._get_column_names",
            return_value=([], ["f1"], "ts", None),
        ):
            job = engine._materialize_from_offline_store(
                registry=registry,
                feature_view=fv,
                start_date=start,
                end_date=end,
                project="proj",
            )
            assert job.status() == MaterializationJobStatus.ERROR

    def test_get_historical_features_exception(self):
        engine = TrinoComputeEngine(
            repo_config=MagicMock(),
            offline_store=MagicMock(),
            online_store=MagicMock(),
        )
        task = MagicMock()
        task.full_feature_name = False
        with patch(
            "feast.infra.compute_engines.trino.compute.TrinoFeatureBuilder",
            side_effect=ValueError("DAG build error"),
        ):
            job = engine.get_historical_features(MagicMock(), task)
            assert job.error() is not None
            with pytest.raises(ValueError, match="DAG build error"):
                job.to_arrow()


class TestTrinoNodesEdgeCases:
    def test_read_node_without_columns_emits_wildcard(self):
        src = MagicMock()
        src.get_table_query_string.return_value = '"catalog"."schema"."tbl"'
        src.field_mapping = {}

        node = TrinoReadNode(
            name="read_all",
            source=src,
            column_info=ColumnInfo([], [], None, None),
            client=MagicMock(),
        )
        val = node.execute(MagicMock())
        assert "SELECT *" in val.data.to_sql()
        assert 'FROM "catalog"."schema"."tbl"' in val.data.to_sql()

    def test_dedup_node_missing_keys_passthrough(self):
        input_node = MagicMock()
        input_node.name = "source"
        plan = TrinoQueryPlan(current_from="tbl")

        col_info = ColumnInfo(
            join_keys=[], feature_cols=[], ts_col=None, created_ts_col=None
        )
        node = TrinoDedupNode(
            name="dedup",
            column_info=col_info,
            client=MagicMock(),
            inputs=[input_node],
        )
        context = MagicMock()
        context.node_outputs = {"source": DAGValue(data=plan, format=DAGFormat.TRINO)}

        val = node.execute(context)
        assert val.data == plan

    def test_aggregation_node_with_time_window(self):
        plan = TrinoQueryPlan(
            ctes=[("_source", 'SELECT * FROM "driver_stats"')],
            current_from="_source",
            join_keys=["driver_id"],
            timestamp_col="event_timestamp",
        )
        input_node = MagicMock()
        input_node.name = "source"

        aggs = [
            Aggregation(
                column="conv_rate", function="avg", time_window=timedelta(hours=1)
            ),
        ]
        node = TrinoAggregationNode(
            name="agg_window",
            aggregations=aggs,
            group_by_keys=["driver_id"],
            timestamp_col="event_timestamp",
            client=MagicMock(),
            inputs=[input_node],
        )
        context = MagicMock()
        context.node_outputs = {"source": DAGValue(data=plan, format=DAGFormat.TRINO)}

        val = node.execute(context)
        sql = val.data.to_sql()
        assert "from_unixtime(floor(to_unixtime(" in sql
        assert "AVG(" in sql

    def test_join_node_with_string_entity_df(self):
        plan = TrinoQueryPlan(
            ctes=[("_source", 'SELECT * FROM "fv"')],
            current_from="_source",
            join_keys=["id"],
            timestamp_col="ts",
        )
        input_node = MagicMock()
        input_node.name = "upstream"

        col_info = ColumnInfo(
            join_keys=["id"], feature_cols=[], ts_col="ts", created_ts_col=None
        )
        node = TrinoJoinNode(
            name="join_str",
            column_info=col_info,
            client=MagicMock(),
            inputs=[input_node],
        )
        context = MagicMock()
        context.entity_df = (
            "SELECT 1 AS id, TIMESTAMP '2025-01-01 00:00:00' AS event_timestamp"
        )
        context.node_outputs = {"upstream": DAGValue(data=plan, format=DAGFormat.TRINO)}

        val = node.execute(context)
        sql = val.data.to_sql()
        assert (
            "FROM (SELECT 1 AS id, TIMESTAMP '2025-01-01 00:00:00' AS event_timestamp) AS _entity"
            in sql
        )
        assert (
            'LEFT JOIN _join_join_str ON _join_join_str."id" = _entity."id" '
            'AND _join_join_str."ts" <= _entity."__entity_event_timestamp"' in sql
        )

    def test_write_node_no_batch_source(self):
        fv = MagicMock()
        fv.online = False
        fv.offline = True
        fv.batch_source = None
        node = TrinoWriteNode(name="out", feature_view=fv, client=MagicMock())
        input_node = MagicMock()
        input_node.name = "src"
        node.inputs = [input_node]
        context = MagicMock()
        context.node_outputs = {
            "src": DAGValue(data=TrinoQueryPlan(), format=DAGFormat.TRINO)
        }
        # Should return silently without raising
        node.execute(context)

    def test_write_node_offline_append_existing_table(self):
        client = MagicMock()
        client.execute_query.return_value = MagicMock()

        fv = MagicMock()
        fv.online = False
        fv.offline = True
        batch_source = MagicMock()
        batch_source.get_table_query_string.return_value = "feast.driver_features"
        fv.batch_source = batch_source

        node = TrinoWriteNode(name="out", feature_view=fv, client=client)
        input_node = MagicMock()
        input_node.name = "src"
        node.inputs = [input_node]

        plan = TrinoQueryPlan(
            ctes=[("_src", "SELECT 1")],
            current_from="_src",
            columns=("driver_id", "feature_val"),
        )
        context = MagicMock()
        context.repo_config.batch_engine.offline_write_mode = "append"
        context.node_outputs = {"src": DAGValue(data=plan, format=DAGFormat.TRINO)}

        node.execute(context)

        executed_queries = [call[0][0] for call in client.execute_query.call_args_list]
        assert any(
            "SELECT 1 FROM feast.driver_features WHERE 1 = 0" in q
            for q in executed_queries
        )
        assert any(
            'INSERT INTO feast.driver_features ("driver_id", "feature_val")' in q
            for q in executed_queries
        )
        assert not any("DROP TABLE" in q for q in executed_queries)

    def test_write_node_offline_append_new_table(self):
        client = MagicMock()

        def mock_query(q):
            if "SELECT 1 FROM" in q:
                raise TrinoQueryError(
                    {"errorName": "TABLE_NOT_FOUND", "message": "Table not found"}
                )
            return MagicMock()

        client.execute_query.side_effect = mock_query

        fv = MagicMock()
        fv.online = False
        fv.offline = True
        batch_source = MagicMock()
        batch_source.get_table_query_string.return_value = "feast.new_features"
        fv.batch_source = batch_source

        node = TrinoWriteNode(name="out", feature_view=fv, client=client)
        input_node = MagicMock()
        input_node.name = "src"
        node.inputs = [input_node]

        plan = TrinoQueryPlan(
            ctes=[("_src", "SELECT 1")],
            current_from="_src",
            columns=("driver_id", "feature_val"),
        )
        context = MagicMock()
        context.repo_config.batch_engine.offline_write_mode = "append"
        context.node_outputs = {"src": DAGValue(data=plan, format=DAGFormat.TRINO)}

        node.execute(context)

        executed_queries = [call[0][0] for call in client.execute_query.call_args_list]
        assert any("CREATE TABLE feast.new_features AS" in q for q in executed_queries)
        assert not any("INSERT INTO" in q for q in executed_queries)

    def test_write_node_offline_overwrite_safe_swap(self):
        client = MagicMock()
        client.execute_query.return_value = MagicMock()

        fv = MagicMock()
        fv.online = False
        fv.offline = True
        batch_source = MagicMock()
        batch_source.get_table_query_string.return_value = "feast.driver_features"
        fv.batch_source = batch_source

        node = TrinoWriteNode(name="out", feature_view=fv, client=client)
        input_node = MagicMock()
        input_node.name = "src"
        node.inputs = [input_node]

        plan = TrinoQueryPlan(
            ctes=[("_src", "SELECT 1")],
            current_from="_src",
        )
        context = MagicMock()
        context.repo_config.batch_engine.offline_write_mode = "overwrite"
        context.node_outputs = {"src": DAGValue(data=plan, format=DAGFormat.TRINO)}

        node.execute(context)

        executed_queries = [call[0][0] for call in client.execute_query.call_args_list]
        assert any(
            'CREATE TABLE feast."driver_features__staging" AS' in q
            for q in executed_queries
        )
        assert any(
            'ALTER TABLE feast.driver_features RENAME TO "driver_features__backup"' in q
            for q in executed_queries
        )
        assert any(
            'ALTER TABLE feast."driver_features__staging" RENAME TO driver_features'
            in q
            for q in executed_queries
        )
        assert any(
            'DROP TABLE IF EXISTS feast."driver_features__backup"' in q
            for q in executed_queries
        )
        assert not any(
            "DROP TABLE IF EXISTS feast.driver_features" == q for q in executed_queries
        )

    def test_write_node_offline_overwrite_rollback_on_failure(self):
        client = MagicMock()

        def mock_query(q):
            if 'ALTER TABLE feast."driver_features__staging" RENAME' in q:
                raise TrinoQueryError(
                    {"errorName": "GENERIC_INTERNAL_ERROR", "message": "Rename failed"}
                )
            return MagicMock()

        client.execute_query.side_effect = mock_query

        fv = MagicMock()
        fv.online = False
        fv.offline = True
        batch_source = MagicMock()
        batch_source.get_table_query_string.return_value = "feast.driver_features"
        fv.batch_source = batch_source

        node = TrinoWriteNode(name="out", feature_view=fv, client=client)
        input_node = MagicMock()
        input_node.name = "src"
        node.inputs = [input_node]

        plan = TrinoQueryPlan(
            ctes=[("_src", "SELECT 1")],
            current_from="_src",
        )
        context = MagicMock()
        context.repo_config.batch_engine.offline_write_mode = "overwrite"
        context.node_outputs = {"src": DAGValue(data=plan, format=DAGFormat.TRINO)}

        with pytest.raises(TrinoQueryError):
            node.execute(context)

        executed_queries = [call[0][0] for call in client.execute_query.call_args_list]
        assert any(
            'ALTER TABLE feast."driver_features__backup" RENAME TO driver_features' in q
            for q in executed_queries
        )
        assert any(
            'DROP TABLE IF EXISTS feast."driver_features__staging"' in q
            for q in executed_queries
        )

    def test_write_node_offline_merge_existing_table(self):
        client = MagicMock()
        client.execute_query.return_value = MagicMock()

        fv = MagicMock()
        fv.online = False
        fv.offline = True
        batch_source = MagicMock()
        batch_source.get_table_query_string.return_value = (
            "iceberg.feast.driver_features"
        )
        fv.batch_source = batch_source

        node = TrinoWriteNode(name="out", feature_view=fv, client=client)
        input_node = MagicMock()
        input_node.name = "src"
        node.inputs = [input_node]

        plan = TrinoQueryPlan(
            ctes=[("_src", "SELECT 1")],
            current_from="_src",
            join_keys=("driver_id",),
            timestamp_col="event_timestamp",
            columns=("driver_id", "event_timestamp", "feature_val"),
        )
        context = MagicMock()
        context.repo_config.batch_engine.offline_write_mode = "merge"
        context.node_outputs = {"src": DAGValue(data=plan, format=DAGFormat.TRINO)}

        node.execute(context)

        executed_queries = [call[0][0] for call in client.execute_query.call_args_list]
        assert any(
            "MERGE INTO iceberg.feast.driver_features AS target" in q
            for q in executed_queries
        )
        assert any("USING (\nWITH\n_src AS (" in q for q in executed_queries)
        assert any(
            'ON target."driver_id" = stage."driver_id" AND target."event_timestamp" = stage."event_timestamp"'
            in q
            for q in executed_queries
        )
        assert any(
            'UPDATE SET "feature_val" = stage."feature_val"' in q
            for q in executed_queries
        )
        assert any(
            'INSERT ("driver_id", "event_timestamp", "feature_val")' in q
            for q in executed_queries
        )
        assert not any("CREATE TABLE" in q for q in executed_queries)
        assert not any("DROP TABLE" in q for q in executed_queries)

    def test_write_node_offline_merge_new_table(self):
        client = MagicMock()

        def mock_query(q):
            if "SELECT 1 FROM" in q:
                raise TrinoQueryError(
                    {"errorName": "TABLE_NOT_FOUND", "message": "Table not found"}
                )
            return MagicMock()

        client.execute_query.side_effect = mock_query

        fv = MagicMock()
        fv.online = False
        fv.offline = True
        batch_source = MagicMock()
        batch_source.get_table_query_string.return_value = "iceberg.feast.new_features"
        fv.batch_source = batch_source

        node = TrinoWriteNode(name="out", feature_view=fv, client=client)
        input_node = MagicMock()
        input_node.name = "src"
        node.inputs = [input_node]

        plan = TrinoQueryPlan(
            ctes=[("_src", "SELECT 1")],
            current_from="_src",
            join_keys=("driver_id",),
            timestamp_col="event_timestamp",
            columns=("driver_id", "event_timestamp", "feature_val"),
        )
        context = MagicMock()
        context.repo_config.batch_engine.offline_write_mode = "merge"
        context.node_outputs = {"src": DAGValue(data=plan, format=DAGFormat.TRINO)}

        node.execute(context)

        executed_queries = [call[0][0] for call in client.execute_query.call_args_list]
        assert any(
            "CREATE TABLE iceberg.feast.new_features AS" in q for q in executed_queries
        )
        assert not any("MERGE INTO" in q for q in executed_queries)

    def test_write_node_offline_merge_fallback_on_missing_keys(self):
        client = MagicMock()
        client.execute_query.return_value = MagicMock()

        fv = MagicMock()
        fv.online = False
        fv.offline = True
        batch_source = MagicMock()
        batch_source.get_table_query_string.return_value = (
            "iceberg.feast.driver_features"
        )
        fv.batch_source = batch_source

        node = TrinoWriteNode(name="out", feature_view=fv, client=client)
        input_node = MagicMock()
        input_node.name = "src"
        node.inputs = [input_node]

        plan = TrinoQueryPlan(
            ctes=[("_src", "SELECT 1")],
            current_from="_src",
            join_keys=(),
            timestamp_col=None,
            columns=("val1",),
        )
        context = MagicMock()
        context.repo_config.batch_engine.offline_write_mode = "merge"
        context.node_outputs = {"src": DAGValue(data=plan, format=DAGFormat.TRINO)}

        node.execute(context)

        executed_queries = [call[0][0] for call in client.execute_query.call_args_list]
        assert any(
            'INSERT INTO iceberg.feast.driver_features ("val1")' in q
            for q in executed_queries
        )
        assert not any("MERGE INTO" in q for q in executed_queries)


class TestTrinoUtilsStreamingAndOnlineWrite:
    def test_stream_trino_arrow_batches_no_columns(self):
        from feast.infra.compute_engines.trino.utils import stream_trino_arrow_batches

        mock_client = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor._query = None
        mock_client._get_cursor.return_value = mock_cursor

        batches = list(stream_trino_arrow_batches(mock_client, "SELECT 1"))
        assert len(batches) == 0

    def test_stream_trino_arrow_batches_fetch(self):
        from feast.infra.compute_engines.trino.utils import stream_trino_arrow_batches

        mock_client = MagicMock()
        mock_cursor = MagicMock()
        mock_query = MagicMock()
        mock_query.columns = [{"name": "id", "type": "bigint"}]
        mock_cursor._query = mock_query
        mock_cursor.fetchmany.side_effect = [[(1,), (2,)], []]
        mock_client._get_cursor.return_value = mock_cursor

        batches = list(
            stream_trino_arrow_batches(mock_client, "SELECT id FROM tbl", batch_size=2)
        )
        assert len(batches) == 1
        assert batches[0].num_rows == 2

    def test_write_arrow_batches_to_online_store(self):
        from feast.infra.compute_engines.trino.utils import (
            write_arrow_batches_to_online_store,
        )

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2], type=pa.int64())],
            names=["entity_id"],
        )
        fv = MagicMock()
        fv.entity_columns = []
        fv.batch_source = None

        repo_cfg = MagicMock()
        repo_cfg.materialization_config.online_write_batch_size = 10

        online_store = MagicMock()
        progress = MagicMock()

        write_arrow_batches_to_online_store(
            batches=[batch],
            feature_view=fv,
            online_store=online_store,
            repo_config=repo_cfg,
            concurrency=2,
            progress_callback=progress,
        )
        assert online_store.online_write_batch.called
        assert progress.called


def test_unique_ordered():
    assert unique_ordered([1, 2, 2, 3, 1, 4]) == (1, 2, 3, 4)
    assert unique_ordered(("a", "b", "a", "c")) == ("a", "b", "c")
    assert unique_ordered([]) == ()
