import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from feast.infra.offline_stores.contrib.trino_offline_store.connectors.upload import (
    format_pandas_row,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino import (
    TrinoOfflineStore,
    TrinoOfflineStoreConfig,
    _trino_monitoring_table_name,
    _trino_normalize_histogram_column,
    _trino_pandas_upsert,
    _trino_sql_literal,
    _trino_sql_numeric_histogram,
    _trino_table_with_clause,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import (
    Results,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import (
    TrinoSource,
)
from feast.monitoring.monitoring_utils import (
    MON_TABLE_FEATURE,
    MON_TABLE_FEATURE_SERVICE,
    MON_TABLE_FEATURE_VIEW,
    MON_TABLE_JOB,
)
from feast.repo_config import RepoConfig


@pytest.fixture
def repo_config():
    return RepoConfig(
        project="test_project",
        registry="data/registry.db",
        provider="local",
        offline_store=TrinoOfflineStoreConfig(
            host="localhost",
            port=8080,
            catalog="memory",
            dataset="feast_test",
            connector={"type": "memory"},
            user="test_user",
        ),
    )


@pytest.fixture
def data_source():
    return TrinoSource(
        name="test_source",
        table="memory.feast_test.driver_stats",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )


def test_sql_literal_formatting():
    assert _trino_sql_literal(None) == "NULL"
    assert _trino_sql_literal(True) == "TRUE"
    assert _trino_sql_literal(False) == "FALSE"
    assert _trino_sql_literal(42) == "42"
    assert _trino_sql_literal(3.14) == "3.14"
    assert _trino_sql_literal("simple") == "'simple'"
    assert _trino_sql_literal("O'Reilly") == "'O''Reilly'"
    assert _trino_sql_literal(date(2025, 1, 15)) == "DATE '2025-01-15'"
    dt = datetime(2025, 1, 15, 12, 0, 0)
    assert _trino_sql_literal(dt) == "TIMESTAMP '2025-01-15 12:00:00.000000'"


def test_monitoring_table_name_and_with_clause(repo_config):
    table_name = _trino_monitoring_table_name(repo_config, MON_TABLE_FEATURE)
    assert table_name == f"memory.feast_test.{MON_TABLE_FEATURE}"

    with_clause = _trino_table_with_clause(repo_config)
    assert with_clause == ""

    # Hive connector with parquet format
    hive_config = RepoConfig(
        project="test_project",
        registry="data/registry.db",
        provider="local",
        offline_store=TrinoOfflineStoreConfig(
            host="localhost",
            port=8080,
            catalog="hive",
            dataset="default",
            connector={"type": "hive", "file_format": "parquet"},
            user="test_user",
        ),
    )
    assert _trino_table_with_clause(hive_config) == "WITH (format = 'parquet')"


def test_normalize_histogram_column():
    pdf = pd.DataFrame(
        [
            {"feature_name": "f1", "histogram": {"bins": [1, 2], "counts": [10]}},
            {"feature_name": "f2", "histogram": None},
            {"feature_name": "f3", "histogram": '{"already": "string"}'},
        ]
    )
    normalized = _trino_normalize_histogram_column(pdf)
    assert isinstance(normalized["histogram"].iloc[0], str)
    assert json.loads(normalized["histogram"].iloc[0]) == {
        "bins": [1, 2],
        "counts": [10],
    }
    assert normalized["histogram"].iloc[1] is None
    assert normalized["histogram"].iloc[2] == '{"already": "string"}'


def test_pandas_upsert():
    old_df = pd.DataFrame(
        [
            {"project_id": "p1", "feature_name": "f1", "value": 10},
            {"project_id": "p1", "feature_name": "f2", "value": 20},
        ]
    )
    new_df = pd.DataFrame(
        [
            {"project_id": "p1", "feature_name": "f2", "value": 99},
            {"project_id": "p1", "feature_name": "f3", "value": 30},
        ]
    )
    merged = _trino_pandas_upsert(old_df, new_df, ["project_id", "feature_name"])
    assert len(merged) == 3
    row_f2 = merged[merged["feature_name"] == "f2"].iloc[0]
    assert row_f2["value"] == 99


def test_compute_monitoring_metrics(repo_config, data_source):
    mock_client = MagicMock()
    executed_queries = []

    def mock_execute(query_text):
        executed_queries.append(query_text)
        if "APPROX_PERCENTILE" in query_text:
            return Results(
                data=[[100, 90, 50.0, 10.0, 1.0, 100.0, 45.0, 75.0, 90.0, 95.0, 99.0]],
                columns=[{"name": "col", "type": "double"}],
            )
        elif "GROUP BY bucket" in query_text:
            return Results(
                data=[[1, 50], [2, 40]],
                columns=[
                    {"name": "bucket", "type": "bigint"},
                    {"name": "cnt", "type": "bigint"},
                ],
            )
        elif "COUNT(DISTINCT" in query_text:
            return Results(
                data=[[100, 0, 3]],
                columns=[
                    {"name": "row_count", "type": "bigint"},
                    {"name": "null_count", "type": "bigint"},
                    {"name": "unique_count", "type": "bigint"},
                ],
            )
        elif 'GROUP BY "status"' in query_text:
            return Results(
                data=[["val_a", 60], ["val_b", 40]],
                columns=[
                    {"name": "value", "type": "varchar"},
                    {"name": "cnt", "type": "bigint"},
                ],
            )
        return Results(data=[], columns=[])

    mock_client.execute_query.side_effect = mock_execute

    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._get_trino_client",
        return_value=mock_client,
    ):
        results = TrinoOfflineStore.compute_monitoring_metrics(
            config=repo_config,
            data_source=data_source,
            feature_columns=[("trip_cost", "numeric"), ("status", "categorical")],
            timestamp_field="event_timestamp",
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 1, 2, tzinfo=timezone.utc),
            histogram_bins=5,
            top_n=10,
        )

    assert len(results) == 2
    assert results[0]["feature_name"] == "trip_cost"
    assert results[0]["feature_type"] == "numeric"
    assert results[0]["mean"] == 50.0
    assert results[0]["histogram"] is not None

    assert results[1]["feature_name"] == "status"
    assert results[1]["feature_type"] == "categorical"
    assert results[1]["histogram"]["unique_count"] == 3
    assert len(results[1]["histogram"]["values"]) == 2

    # Check query patterns
    assert any("APPROX_PERCENTILE" in q for q in executed_queries)
    assert any("STDDEV_SAMP" in q for q in executed_queries)
    assert any("COUNT(DISTINCT" in q for q in executed_queries)
    assert any('GROUP BY "status"' in q for q in executed_queries)


def test_get_monitoring_max_timestamp(repo_config, data_source):
    mock_client = MagicMock()
    mock_client.execute_query.return_value = Results(
        data=[[datetime(2025, 1, 15, 10, 30, 0)]],
        columns=[{"name": "max_ts", "type": "timestamp"}],
    )

    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._get_trino_client",
        return_value=mock_client,
    ):
        max_ts = TrinoOfflineStore.get_monitoring_max_timestamp(
            config=repo_config,
            data_source=data_source,
            timestamp_field="event_timestamp",
        )

    assert max_ts == datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)


def test_ensure_monitoring_tables(repo_config):
    mock_client = MagicMock()
    executed_queries = []
    mock_client.execute_query.side_effect = lambda q: executed_queries.append(q)

    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._get_trino_client",
        return_value=mock_client,
    ):
        TrinoOfflineStore.ensure_monitoring_tables(config=repo_config)

    assert any(
        "CREATE SCHEMA IF NOT EXISTS memory.feast_test" in q for q in executed_queries
    )
    assert any(
        f"CREATE TABLE IF NOT EXISTS memory.feast_test.{MON_TABLE_FEATURE}" in q
        for q in executed_queries
    )
    assert any(
        f"CREATE TABLE IF NOT EXISTS memory.feast_test.{MON_TABLE_FEATURE_VIEW}" in q
        for q in executed_queries
    )
    assert any(
        f"CREATE TABLE IF NOT EXISTS memory.feast_test.{MON_TABLE_FEATURE_SERVICE}" in q
        for q in executed_queries
    )
    assert any(
        f"CREATE TABLE IF NOT EXISTS memory.feast_test.{MON_TABLE_JOB}" in q
        for q in executed_queries
    )


def test_save_and_query_monitoring_metrics(repo_config):
    mock_client = MagicMock()
    executed_queries = []
    mock_client.execute_query.side_effect = lambda q: (
        executed_queries.append(q) or Results(data=[], columns=[])
    )

    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._get_trino_client",
        return_value=mock_client,
    ):
        metrics = [
            {
                "project_id": "test_project",
                "feature_view_name": "fv1",
                "feature_name": "f1",
                "metric_date": date(2025, 1, 1),
                "granularity": "daily",
                "data_source_type": "batch",
                "computed_at": datetime(2025, 1, 1, 12, 0, 0),
                "max_event_timestamp": datetime(2025, 1, 1, 12, 0, 0),
                "is_baseline": False,
                "feature_type": "numeric",
                "row_count": 100,
                "null_count": 0,
                "null_rate": 0.0,
                "mean": 10.5,
                "stddev": 2.1,
                "min_val": 1.0,
                "max_val": 20.0,
                "p50": 10.0,
                "p75": 15.0,
                "p90": 18.0,
                "p95": 19.0,
                "p99": 20.0,
                "histogram": {"bins": [1, 20], "counts": [100]},
            }
        ]
        TrinoOfflineStore.save_monitoring_metrics(
            config=repo_config,
            metric_type="feature",
            metrics=metrics,
        )

    full_table = f"memory.feast_test.{MON_TABLE_FEATURE}"
    assert any(f"DROP TABLE IF EXISTS {full_table}" in q for q in executed_queries)
    assert any(
        f"CREATE TABLE IF NOT EXISTS {full_table}" in q for q in executed_queries
    )
    assert any("histogram         VARCHAR" in q for q in executed_queries)
    assert any(f"INSERT INTO {full_table}" in q for q in executed_queries)
    insert_q = [q for q in executed_queries if "INSERT INTO" in q][0]
    assert "'f1'" in insert_q


def test_save_monitoring_metrics_with_all_null_optional_columns(repo_config):
    mock_client = MagicMock()
    executed_queries = []
    mock_client.execute_query.side_effect = lambda q: (
        executed_queries.append(q) or Results(data=[], columns=[])
    )

    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._get_trino_client",
        return_value=mock_client,
    ):
        metrics = [
            {
                "project_id": "test_project",
                "feature_view_name": "fv1",
                "feature_name": "f1",
                "metric_date": date(2025, 1, 1),
                "granularity": "daily",
                "data_source_type": "batch",
                "computed_at": datetime(2025, 1, 1, 12, 0, 0),
                "max_event_timestamp": None,
                "is_baseline": False,
                "feature_type": "categorical",
                "row_count": 100,
                "null_count": 100,
                "null_rate": 1.0,
                "mean": None,
                "stddev": None,
                "min_val": None,
                "max_val": None,
                "p50": None,
                "p75": None,
                "p90": None,
                "p95": None,
                "p99": None,
                "histogram": None,
            }
        ]
        TrinoOfflineStore.save_monitoring_metrics(
            config=repo_config,
            metric_type="feature",
            metrics=metrics,
        )

    # Recreated table must preserve explicit monitoring schema, never inferring "histogram null"
    for q in executed_queries:
        assert "null null" not in q
        assert "histogram null" not in q
        assert "max_event_timestamp null" not in q
    assert any("histogram         VARCHAR" in q for q in executed_queries)
    assert any("max_event_timestamp TIMESTAMP" in q for q in executed_queries)
    insert_q = [q for q in executed_queries if "INSERT INTO" in q][0]
    assert "NULL" in insert_q


def test_clear_monitoring_baseline(repo_config):
    mock_client = MagicMock()
    existing_df = pd.DataFrame(
        [
            {
                "project_id": "test_project",
                "feature_view_name": "fv1",
                "feature_name": "f1",
                "data_source_type": "batch",
                "is_baseline": True,
            },
            {
                "project_id": "other_project",
                "feature_view_name": "fv1",
                "feature_name": "f1",
                "data_source_type": "batch",
                "is_baseline": True,
            },
        ]
    )

    executed_queries = []

    def mock_execute(q):
        executed_queries.append(q)
        if "SELECT *" in q:
            return Results(
                data=existing_df.values.tolist(),
                columns=[
                    {"name": col, "type": "varchar"} for col in existing_df.columns
                ],
            )
        return Results(data=[], columns=[])

    mock_client.execute_query.side_effect = mock_execute

    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._get_trino_client",
        return_value=mock_client,
    ):
        TrinoOfflineStore.clear_monitoring_baseline(
            config=repo_config,
            project="test_project",
            feature_view_name="fv1",
            feature_name="f1",
        )

    full_table = f"memory.feast_test.{MON_TABLE_FEATURE}"
    assert any(f"DROP TABLE IF EXISTS {full_table}" in q for q in executed_queries)
    assert any(
        f"CREATE TABLE IF NOT EXISTS {full_table}" in q for q in executed_queries
    )
    insert_queries = [q for q in executed_queries if "INSERT INTO" in q]
    assert len(insert_queries) == 1
    insert_q = insert_queries[0]
    # Check that test_project has is_baseline set to FALSE and other_project has TRUE
    assert "FALSE" in insert_q
    assert "TRUE" in insert_q


def test_numeric_histogram_single_value():
    mock_client = MagicMock()
    mock_client.execute_query.return_value = Results(
        data=[[42]],
        columns=[{"name": "cnt", "type": "bigint"}],
    )

    hist = _trino_sql_numeric_histogram(
        client=mock_client,
        from_expression="test_table",
        col_name="val",
        ts_clause="1=1",
        bins=5,
        min_val=10.0,
        max_val=10.0,
    )
    assert hist["bins"] == [10.0, 10.0]
    assert hist["counts"] == [42]
    assert hist["bin_width"] == 0.0


def test_query_backed_trino_source_monitoring(repo_config):
    query_source = TrinoSource(
        name="query_source",
        query="SELECT trip_cost, status, event_timestamp FROM memory.feast_test.driver_stats",
        timestamp_field="event_timestamp",
    )
    mock_client = MagicMock()
    executed_queries = []

    def mock_execute(query_text):
        executed_queries.append(query_text)
        if "APPROX_PERCENTILE" in query_text:
            return Results(
                data=[[100, 90, 50.0, 10.0, 1.0, 100.0, 45.0, 75.0, 90.0, 95.0, 99.0]],
                columns=[{"name": "col", "type": "double"}],
            )
        elif "GROUP BY bucket" in query_text:
            return Results(
                data=[[1, 50], [2, 40]],
                columns=[
                    {"name": "bucket", "type": "bigint"},
                    {"name": "cnt", "type": "bigint"},
                ],
            )
        elif "COUNT(DISTINCT" in query_text:
            return Results(
                data=[[100, 0, 3]],
                columns=[
                    {"name": "row_count", "type": "bigint"},
                    {"name": "null_count", "type": "bigint"},
                    {"name": "unique_count", "type": "bigint"},
                ],
            )
        elif 'GROUP BY "status"' in query_text:
            return Results(
                data=[["val_a", 60], ["val_b", 40]],
                columns=[
                    {"name": "value", "type": "varchar"},
                    {"name": "cnt", "type": "bigint"},
                ],
            )
        elif "SELECT MAX(" in query_text:
            return Results(
                data=[[datetime(2025, 1, 15, 10, 30, 0)]],
                columns=[{"name": "max_ts", "type": "timestamp"}],
            )
        return Results(data=[], columns=[])

    mock_client.execute_query.side_effect = mock_execute

    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._get_trino_client",
        return_value=mock_client,
    ):
        TrinoOfflineStore.get_monitoring_max_timestamp(
            config=repo_config,
            data_source=query_source,
            timestamp_field="event_timestamp",
        )
        TrinoOfflineStore.compute_monitoring_metrics(
            config=repo_config,
            data_source=query_source,
            feature_columns=[("trip_cost", "numeric"), ("status", "categorical")],
            timestamp_field="event_timestamp",
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 1, 2, tzinfo=timezone.utc),
            histogram_bins=5,
            top_n=10,
        )

    expected_from = f"FROM ({query_source.query}) AS _src"
    assert len(executed_queries) == 5
    for q in executed_queries:
        assert expected_from in q
        assert f"FROM {query_source.query} AS _src" not in q


def test_categorical_all_nulls_monitoring(repo_config, data_source):
    mock_client = MagicMock()
    executed_queries = []

    def mock_execute(query_text):
        executed_queries.append(query_text)
        if "COUNT(DISTINCT" in query_text:
            # 100 rows, all 100 null, 0 distinct values
            return Results(
                data=[[100, 100, 0]],
                columns=[
                    {"name": "row_count", "type": "bigint"},
                    {"name": "null_count", "type": "bigint"},
                    {"name": "unique_count", "type": "bigint"},
                ],
            )
        return Results(data=[], columns=[])

    mock_client.execute_query.side_effect = mock_execute

    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._get_trino_client",
        return_value=mock_client,
    ):
        results = TrinoOfflineStore.compute_monitoring_metrics(
            config=repo_config,
            data_source=data_source,
            feature_columns=[("all_null_col", "categorical")],
            timestamp_field="event_timestamp",
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 1, 2, tzinfo=timezone.utc),
        )

    assert len(results) == 1
    m = results[0]
    assert m["feature_name"] == "all_null_col"
    assert m["feature_type"] == "categorical"
    assert m["row_count"] == 100
    assert m["null_count"] == 100
    assert m["null_rate"] == 1.0
    assert m["histogram"]["unique_count"] == 0
    assert m["histogram"]["values"] == []
    assert m["histogram"]["other_count"] == 0
    # Should only execute the counts query, not the top_n query
    assert len(executed_queries) == 1
    assert "GROUP BY" not in executed_queries[0]


def test_format_pandas_row_with_containers_and_nulls():
    df = pd.DataFrame(
        [
            {
                "int_col": 1,
                "list_col": [1, 2],
                "array_col": np.array([3, 4]),
                "tuple_col": (5, 6),
                "null_col": None,
                "bool_col": True,
                "str_col": "O'Connor",
                "ts_col": datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                "date_col": date(2025, 1, 1),
            },
            {
                "int_col": 2,
                "list_col": None,
                "array_col": None,
                "tuple_col": None,
                "null_col": np.nan,
                "bool_col": False,
                "str_col": "normal",
                "ts_col": None,
                "date_col": None,
            },
        ]
    )

    formatted = format_pandas_row(df)
    rows = formatted.split("),(")
    assert len(rows) == 2

    # First row should have array formatting and escaped string
    assert "ARRAY[1, 2]" in rows[0]
    assert "ARRAY[3, 4]" in rows[0]
    assert "ARRAY[5, 6]" in rows[0]
    assert "'O''Connor'" in rows[0]
    assert "TRUE" in rows[0]
    assert "TIMESTAMP '2025-01-01 12:00:00.000000'" in rows[0]
    assert "DATE '2025-01-01'" in rows[0]

    # Second row with nulls should have NULL for missing containers and timestamps
    assert "NULL" in rows[1]
    assert "FALSE" in rows[1]
    assert "'normal'" in rows[1]
