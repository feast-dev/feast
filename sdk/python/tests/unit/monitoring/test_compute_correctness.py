"""
Compute correctness tests for monitoring metric calculations.

Verifies that each offline store backend's SQL/compute helpers produce
mathematically correct results for a known golden dataset.

- DuckDB and Dask tests run fully in-memory with zero external dependencies.
- PostgreSQL tests require a live Postgres instance (skipped if unavailable).
- Snowflake, BigQuery, Redshift, Spark, Oracle tests require their respective
  backends (skipped if unavailable).
"""

import statistics
from datetime import datetime, timezone
from typing import Any, Dict

import pyarrow as pa
import pytest

# ---------------------------------------------------------------------------
# Golden dataset: known values with hand-computable statistics
# ---------------------------------------------------------------------------

NUMERIC_VALUES = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
NUMERIC_WITH_NULLS = [1.0, None, 3.0, None, 5.0, None, 7.0, None, 9.0, None]
CATEGORICAL_VALUES = ["a", "b", "a", "c", "a", "b", "d", "a", "b", "c"]

ROW_COUNT = len(NUMERIC_VALUES)
NON_NULL_VALUES = [v for v in NUMERIC_WITH_NULLS if v is not None]


def _expected_numeric_stats():
    """Hand-computed expected values for NUMERIC_VALUES = [1..10]."""
    vals = NUMERIC_VALUES
    return {
        "row_count": 10,
        "null_count": 0,
        "null_rate": 0.0,
        "mean": 5.5,
        "stddev": statistics.stdev(vals),  # sample stddev ≈ 3.0277
        "min_val": 1.0,
        "max_val": 10.0,
        "p50": 5.5,
        "p75": 7.75,
        "p90": 9.1,
        "p95": 9.55,
        "p99": 9.91,
    }


def _expected_numeric_with_nulls_stats():
    """Hand-computed expected values for NUMERIC_WITH_NULLS."""
    vals = NON_NULL_VALUES  # [1, 3, 5, 7, 9]
    return {
        "row_count": 10,
        "null_count": 5,
        "null_rate": 0.5,
        "mean": 5.0,
        "stddev": statistics.stdev(vals),  # sample stddev ≈ 3.1623
        "min_val": 1.0,
        "max_val": 9.0,
    }


def _expected_categorical_stats():
    """Expected values for CATEGORICAL_VALUES."""
    return {
        "row_count": 10,
        "null_count": 0,
        "null_rate": 0.0,
        "unique_count": 4,
        "top_value": "a",
        "top_count": 4,
    }


# ---------------------------------------------------------------------------
# Shared assertions: validate any backend's numeric/categorical result
# ---------------------------------------------------------------------------


def assert_numeric_correctness(
    result: Dict[str, Any], expected: Dict, label: str, approx_percentiles: bool = False
):
    """Assert that a numeric result from any backend matches expected values."""
    assert result["feature_type"] == "numeric", f"{label}: wrong feature_type"
    assert result["row_count"] == expected["row_count"], f"{label}: wrong row_count"
    assert result["null_count"] == expected["null_count"], f"{label}: wrong null_count"
    assert result["null_rate"] == pytest.approx(expected["null_rate"], abs=1e-6), (
        f"{label}: wrong null_rate"
    )
    assert result["mean"] == pytest.approx(expected["mean"], abs=1e-4), (
        f"{label}: wrong mean"
    )
    assert result["stddev"] == pytest.approx(expected["stddev"], abs=0.05), (
        f"{label}: wrong stddev"
    )
    assert result["min_val"] == pytest.approx(expected["min_val"], abs=1e-6), (
        f"{label}: wrong min_val"
    )
    assert result["max_val"] == pytest.approx(expected["max_val"], abs=1e-6), (
        f"{label}: wrong max_val"
    )

    if "p50" in expected and not approx_percentiles:
        assert result["p50"] == pytest.approx(expected["p50"], abs=0.5), (
            f"{label}: wrong p50"
        )
        assert result["p75"] == pytest.approx(expected["p75"], abs=0.5), (
            f"{label}: wrong p75"
        )
        assert result["p90"] == pytest.approx(expected["p90"], abs=0.5), (
            f"{label}: wrong p90"
        )

    # Percentile ordering is always enforced
    assert result["p50"] <= result["p75"], f"{label}: p50 > p75"
    assert result["p75"] <= result["p90"], f"{label}: p75 > p90"
    assert result["p90"] <= result["p95"], f"{label}: p90 > p95"
    assert result["p95"] <= result["p99"], f"{label}: p95 > p99"
    assert result["p50"] >= result["min_val"], f"{label}: p50 < min"
    assert result["p99"] <= result["max_val"], f"{label}: p99 > max"


def assert_histogram_correctness(
    result: Dict[str, Any], label: str, expected_bins: int = 5
):
    """Assert that a numeric histogram has consistent structure and totals."""
    hist = result.get("histogram")
    assert hist is not None, f"{label}: histogram is None"
    assert len(hist["counts"]) == expected_bins, f"{label}: wrong number of bins"
    assert len(hist["bins"]) == expected_bins + 1, f"{label}: wrong number of bin edges"
    total = sum(hist["counts"])
    non_null = result["row_count"] - result["null_count"]
    assert total == non_null, f"{label}: histogram total {total} != non_null {non_null}"
    assert hist["bins"][0] <= result["min_val"], f"{label}: first bin edge > min_val"
    assert hist["bins"][-1] >= result["max_val"], f"{label}: last bin edge < max_val"


def assert_categorical_correctness(result: Dict[str, Any], expected: Dict, label: str):
    """Assert that a categorical result from any backend matches expected values."""
    assert result["feature_type"] == "categorical", f"{label}: wrong feature_type"
    assert result["row_count"] == expected["row_count"], f"{label}: wrong row_count"
    assert result["null_count"] == expected["null_count"], f"{label}: wrong null_count"
    assert result["null_rate"] == pytest.approx(expected["null_rate"], abs=1e-6), (
        f"{label}: wrong null_rate"
    )

    hist = result["histogram"]
    assert hist is not None, f"{label}: histogram is None"
    assert hist["unique_count"] == expected["unique_count"], (
        f"{label}: wrong unique_count"
    )

    top_entry = hist["values"][0]
    assert top_entry["value"] == expected["top_value"], f"{label}: wrong top value"
    assert top_entry["count"] == expected["top_count"], f"{label}: wrong top count"

    total = sum(e["count"] for e in hist["values"]) + hist["other_count"]
    expected_total = expected["row_count"] - expected["null_count"]
    assert total == expected_total, (
        f"{label}: categorical total {total} != expected {expected_total}"
    )


# ===================================================================
# DuckDB compute correctness tests (fully in-memory, no external deps)
# ===================================================================


class TestDuckDBComputeCorrectness:
    """Test DuckDB SQL helper functions produce correct metric values."""

    @pytest.fixture(autouse=True)
    def setup_duckdb(self):
        duckdb = pytest.importorskip("duckdb")
        self.conn = duckdb.connect()

        self.conn.execute("""
            CREATE TABLE test_data (
                event_timestamp TIMESTAMP,
                numeric_col DOUBLE,
                numeric_with_nulls DOUBLE,
                categorical_col VARCHAR
            )
        """)

        ts = datetime(2025, 1, 15, 12, 0, 0)
        for i in range(ROW_COUNT):
            n_val = NUMERIC_VALUES[i]
            n_null = NUMERIC_WITH_NULLS[i]
            c_val = CATEGORICAL_VALUES[i]
            n_null_sql = f"{n_null}" if n_null is not None else "NULL"
            self.conn.execute(
                f"INSERT INTO test_data VALUES "
                f"(TIMESTAMP '{ts.strftime('%Y-%m-%d %H:%M:%S')}', "
                f"{n_val}, {n_null_sql}, '{c_val}')"
            )
        yield
        self.conn.close()

    def test_numeric_stats_basic(self):
        from feast.infra.offline_stores.duckdb import _duckdb_numeric_stats

        results = _duckdb_numeric_stats(
            self.conn,
            "test_data",
            ["numeric_col"],
            "1=1",
            histogram_bins=5,
        )

        assert len(results) == 1
        result = results[0]
        expected = _expected_numeric_stats()
        assert_numeric_correctness(result, expected, "duckdb_numeric")
        assert_histogram_correctness(result, "duckdb_numeric", expected_bins=5)

    def test_numeric_stats_with_nulls(self):
        from feast.infra.offline_stores.duckdb import _duckdb_numeric_stats

        results = _duckdb_numeric_stats(
            self.conn,
            "test_data",
            ["numeric_with_nulls"],
            "1=1",
            histogram_bins=5,
        )

        assert len(results) == 1
        result = results[0]
        expected = _expected_numeric_with_nulls_stats()
        assert_numeric_correctness(result, expected, "duckdb_numeric_nulls")
        assert_histogram_correctness(result, "duckdb_numeric_nulls", expected_bins=5)

    def test_numeric_multiple_features(self):
        from feast.infra.offline_stores.duckdb import _duckdb_numeric_stats

        results = _duckdb_numeric_stats(
            self.conn,
            "test_data",
            ["numeric_col", "numeric_with_nulls"],
            "1=1",
            histogram_bins=5,
        )

        assert len(results) == 2
        assert results[0]["feature_name"] == "numeric_col"
        assert results[1]["feature_name"] == "numeric_with_nulls"
        assert results[0]["mean"] == pytest.approx(5.5, abs=1e-4)
        assert results[1]["mean"] == pytest.approx(5.0, abs=1e-4)
        assert results[0]["null_count"] == 0
        assert results[1]["null_count"] == 5

    def test_categorical_stats(self):
        from feast.infra.offline_stores.duckdb import _duckdb_categorical_stats

        result = _duckdb_categorical_stats(
            self.conn,
            "test_data",
            "categorical_col",
            "1=1",
            top_n=10,
        )

        expected = _expected_categorical_stats()
        assert_categorical_correctness(result, expected, "duckdb_categorical")

    def test_categorical_top_n_truncation(self):
        from feast.infra.offline_stores.duckdb import _duckdb_categorical_stats

        result = _duckdb_categorical_stats(
            self.conn,
            "test_data",
            "categorical_col",
            "1=1",
            top_n=2,
        )

        assert len(result["histogram"]["values"]) == 2
        assert result["histogram"]["other_count"] > 0
        total = (
            sum(e["count"] for e in result["histogram"]["values"])
            + result["histogram"]["other_count"]
        )
        assert total == 10

    def test_histogram_bin_edges_cover_range(self):
        from feast.infra.offline_stores.duckdb import _duckdb_numeric_histogram

        hist = _duckdb_numeric_histogram(
            self.conn,
            "test_data",
            "numeric_col",
            "1=1",
            bins=5,
            min_val=1.0,
            max_val=10.0,
        )

        assert hist["bins"][0] == pytest.approx(1.0, abs=1e-6)
        assert hist["bins"][-1] == pytest.approx(10.0, abs=0.1)
        assert sum(hist["counts"]) == 10
        assert hist["bin_width"] == pytest.approx(1.8, abs=0.01)

    def test_histogram_single_value(self):
        from feast.infra.offline_stores.duckdb import _duckdb_numeric_histogram

        self.conn.execute("""
            CREATE TABLE single_val (event_timestamp TIMESTAMP, v DOUBLE)
        """)
        self.conn.execute(
            "INSERT INTO single_val VALUES (TIMESTAMP '2025-01-15 12:00:00', 42.0)"
        )

        hist = _duckdb_numeric_histogram(
            self.conn,
            "single_val",
            "v",
            "1=1",
            bins=5,
            min_val=42.0,
            max_val=42.0,
        )

        assert hist["counts"] == [1]
        assert hist["bin_width"] == 0.0

    def test_empty_table(self):
        from feast.infra.offline_stores.duckdb import _duckdb_numeric_stats

        self.conn.execute("""
            CREATE TABLE empty_tbl (event_timestamp TIMESTAMP, v DOUBLE)
        """)
        results = _duckdb_numeric_stats(
            self.conn,
            "empty_tbl",
            ["v"],
            "1=1",
            histogram_bins=5,
        )

        assert len(results) == 1
        assert results[0]["row_count"] == 0
        assert results[0]["mean"] is None
        assert results[0]["histogram"] is None

    def test_stddev_with_single_row(self):
        from feast.infra.offline_stores.duckdb import _duckdb_numeric_stats

        self.conn.execute("""
            CREATE TABLE one_row (event_timestamp TIMESTAMP, v DOUBLE)
        """)
        self.conn.execute(
            "INSERT INTO one_row VALUES (TIMESTAMP '2025-01-15 12:00:00', 7.0)"
        )
        results = _duckdb_numeric_stats(
            self.conn,
            "one_row",
            ["v"],
            "1=1",
            histogram_bins=5,
        )

        assert results[0]["mean"] == pytest.approx(7.0)
        assert results[0]["min_val"] == 7.0
        assert results[0]["max_val"] == 7.0
        # STDDEV_SAMP of a single value is NULL
        assert results[0]["stddev"] is None

    def test_large_dataset_percentiles(self):
        from feast.infra.offline_stores.duckdb import _duckdb_numeric_stats

        self.conn.execute("""
            CREATE TABLE large_tbl (event_timestamp TIMESTAMP, v DOUBLE)
        """)
        for i in range(1, 1001):
            self.conn.execute(
                f"INSERT INTO large_tbl VALUES "
                f"(TIMESTAMP '2025-01-15 12:00:00', {float(i)})"
            )

        results = _duckdb_numeric_stats(
            self.conn,
            "large_tbl",
            ["v"],
            "1=1",
            histogram_bins=10,
        )

        r = results[0]
        assert r["mean"] == pytest.approx(500.5, abs=0.1)
        assert r["min_val"] == 1.0
        assert r["max_val"] == 1000.0
        assert r["p50"] == pytest.approx(500.5, abs=5.0)
        assert r["p90"] == pytest.approx(900.0, abs=10.0)
        assert r["p99"] == pytest.approx(990.0, abs=10.0)
        assert_histogram_correctness(r, "duckdb_large", expected_bins=10)


# ===================================================================
# Dask (PyArrow) compute correctness tests (no external deps)
# ===================================================================


class TestDaskComputeCorrectness:
    """Test Dask/PyArrow compute helpers produce correct metric values."""

    def test_numeric_stats_basic(self):
        from feast.infra.offline_stores.dask import _dask_compute_numeric_metrics

        col = pa.chunked_array([pa.array(NUMERIC_VALUES, type=pa.float64())])
        result = _dask_compute_numeric_metrics(col, histogram_bins=5)

        expected = _expected_numeric_stats()
        result["feature_name"] = "test"
        assert_numeric_correctness(result, expected, "dask_numeric")
        assert_histogram_correctness(result, "dask_numeric", expected_bins=5)

    def test_numeric_stats_with_nulls(self):
        from feast.infra.offline_stores.dask import _dask_compute_numeric_metrics

        col = pa.chunked_array([pa.array(NUMERIC_WITH_NULLS, type=pa.float64())])
        result = _dask_compute_numeric_metrics(col, histogram_bins=5)
        result["feature_name"] = "test"

        expected = _expected_numeric_with_nulls_stats()
        assert_numeric_correctness(result, expected, "dask_numeric_nulls")
        assert_histogram_correctness(result, "dask_numeric_nulls", expected_bins=5)

    def test_numeric_all_nulls(self):
        from feast.infra.offline_stores.dask import _dask_compute_numeric_metrics

        col = pa.chunked_array([pa.array([None, None, None], type=pa.float64())])
        result = _dask_compute_numeric_metrics(col, histogram_bins=5)

        assert result["row_count"] == 3
        assert result["null_count"] == 3
        assert result["mean"] is None
        assert result["histogram"] is None

    def test_numeric_empty(self):
        from feast.infra.offline_stores.dask import _dask_compute_numeric_metrics

        col = pa.chunked_array([pa.array([], type=pa.float64())])
        result = _dask_compute_numeric_metrics(col, histogram_bins=5)

        assert result["row_count"] == 0
        assert result["mean"] is None

    def test_numeric_single_value(self):
        from feast.infra.offline_stores.dask import _dask_compute_numeric_metrics

        col = pa.chunked_array([pa.array([42.0], type=pa.float64())])
        result = _dask_compute_numeric_metrics(col, histogram_bins=5)

        assert result["mean"] == pytest.approx(42.0)
        assert result["min_val"] == 42.0
        assert result["max_val"] == 42.0
        assert result["stddev"] is None  # STDDEV_SAMP of single value

    def test_numeric_large_dataset_percentiles(self):
        from feast.infra.offline_stores.dask import _dask_compute_numeric_metrics

        vals = list(range(1, 1001))
        col = pa.chunked_array([pa.array(vals, type=pa.float64())])
        result = _dask_compute_numeric_metrics(col, histogram_bins=10)

        assert result["mean"] == pytest.approx(500.5, abs=0.1)
        assert result["p50"] == pytest.approx(500.5, abs=5.0)
        assert result["p90"] == pytest.approx(900.0, abs=10.0)
        assert result["p99"] == pytest.approx(990.0, abs=10.0)
        result["feature_name"] = "test"
        assert_histogram_correctness(result, "dask_large", expected_bins=10)

    def test_categorical_stats_basic(self):
        from feast.infra.offline_stores.dask import _dask_compute_categorical_metrics

        col = pa.chunked_array([pa.array(CATEGORICAL_VALUES, type=pa.string())])
        result = _dask_compute_categorical_metrics(col, top_n=10)
        result["feature_name"] = "test"

        expected = _expected_categorical_stats()
        assert_categorical_correctness(result, expected, "dask_categorical")

    def test_categorical_with_nulls(self):
        from feast.infra.offline_stores.dask import _dask_compute_categorical_metrics

        vals = ["a", None, "b", None, "a", "c"]
        col = pa.chunked_array([pa.array(vals, type=pa.string())])
        result = _dask_compute_categorical_metrics(col, top_n=10)

        assert result["row_count"] == 6
        assert result["null_count"] == 2
        assert result["null_rate"] == pytest.approx(1 / 3, abs=1e-4)
        assert result["histogram"]["unique_count"] == 3

    def test_categorical_top_n_truncation(self):
        from feast.infra.offline_stores.dask import _dask_compute_categorical_metrics

        col = pa.chunked_array([pa.array(CATEGORICAL_VALUES, type=pa.string())])
        result = _dask_compute_categorical_metrics(col, top_n=2)

        assert len(result["histogram"]["values"]) == 2
        assert result["histogram"]["other_count"] > 0
        total = (
            sum(e["count"] for e in result["histogram"]["values"])
            + result["histogram"]["other_count"]
        )
        assert total == 10

    def test_categorical_all_nulls(self):
        from feast.infra.offline_stores.dask import _dask_compute_categorical_metrics

        col = pa.chunked_array([pa.array([None, None], type=pa.string())])
        result = _dask_compute_categorical_metrics(col, top_n=10)

        assert result["null_count"] == 2
        assert result["histogram"] is None


# ===================================================================
# PostgreSQL compute correctness tests (requires live Postgres)
# ===================================================================


def _pg_available():
    try:
        import psycopg  # noqa: F401

        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _pg_available(), reason="psycopg not installed")
class TestPostgresComputeCorrectness:
    """Test PostgreSQL SQL helpers produce correct metric values.

    Requires env vars: FEAST_PG_HOST, FEAST_PG_PORT, FEAST_PG_DB,
    FEAST_PG_USER, FEAST_PG_PASS (or a local Postgres at localhost:5432).
    """

    @pytest.fixture(autouse=True)
    def setup_pg(self):
        import os

        import psycopg

        host = os.environ.get("FEAST_PG_HOST", "localhost")
        port = os.environ.get("FEAST_PG_PORT", "5432")
        db = os.environ.get("FEAST_PG_DB", "feast")
        user = os.environ.get("FEAST_PG_USER", "feast")
        password = os.environ.get("FEAST_PG_PASS", "feast")

        try:
            self.conn = psycopg.connect(
                f"host={host} port={port} dbname={db} user={user} password={password}",
                autocommit=True,
            )
        except psycopg.OperationalError:
            pytest.skip("PostgreSQL not reachable")

        self.conn.execute("DROP TABLE IF EXISTS feast_test_monitoring_correctness")
        self.conn.execute("""
            CREATE TABLE feast_test_monitoring_correctness (
                event_timestamp TIMESTAMPTZ,
                numeric_col DOUBLE PRECISION,
                numeric_with_nulls DOUBLE PRECISION,
                categorical_col TEXT
            )
        """)

        ts = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        for i in range(ROW_COUNT):
            n_val = NUMERIC_VALUES[i]
            n_null = NUMERIC_WITH_NULLS[i]
            c_val = CATEGORICAL_VALUES[i]
            self.conn.execute(
                "INSERT INTO feast_test_monitoring_correctness VALUES (%s, %s, %s, %s)",
                (ts, n_val, n_null, c_val),
            )
        yield
        self.conn.execute("DROP TABLE IF EXISTS feast_test_monitoring_correctness")
        self.conn.close()

    def test_numeric_stats(self):
        from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
            _sql_numeric_stats,
        )

        results = _sql_numeric_stats(
            self.conn,
            "feast_test_monitoring_correctness",
            ["numeric_col"],
            "1=1",
            histogram_bins=5,
        )

        assert len(results) == 1
        expected = _expected_numeric_stats()
        assert_numeric_correctness(results[0], expected, "pg_numeric")
        assert_histogram_correctness(results[0], "pg_numeric", expected_bins=5)

    def test_numeric_stats_with_nulls(self):
        from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
            _sql_numeric_stats,
        )

        results = _sql_numeric_stats(
            self.conn,
            "feast_test_monitoring_correctness",
            ["numeric_with_nulls"],
            "1=1",
            histogram_bins=5,
        )

        expected = _expected_numeric_with_nulls_stats()
        assert_numeric_correctness(results[0], expected, "pg_numeric_nulls")

    def test_numeric_multiple_features(self):
        from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
            _sql_numeric_stats,
        )

        results = _sql_numeric_stats(
            self.conn,
            "feast_test_monitoring_correctness",
            ["numeric_col", "numeric_with_nulls"],
            "1=1",
            histogram_bins=5,
        )

        assert len(results) == 2
        assert results[0]["mean"] == pytest.approx(5.5, abs=1e-4)
        assert results[1]["mean"] == pytest.approx(5.0, abs=1e-4)

    def test_categorical_stats(self):
        from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
            _sql_categorical_stats,
        )

        result = _sql_categorical_stats(
            self.conn,
            "feast_test_monitoring_correctness",
            "categorical_col",
            "1=1",
            top_n=10,
        )

        expected = _expected_categorical_stats()
        assert_categorical_correctness(result, expected, "pg_categorical")


# ===================================================================
# Snowflake compute correctness (mocked connection, real parsing)
# ===================================================================


def _snowflake_importable():
    try:
        from feast.infra.offline_stores.snowflake import (
            _snowflake_sql_numeric_stats,  # noqa: F401
        )

        return True
    except (ImportError, Exception):
        return False


@pytest.mark.skipif(not _snowflake_importable(), reason="Snowflake deps not installed")
class TestSnowflakeComputeCorrectness:
    """Tests Snowflake result parsing with mocked cursor.

    The cursor returns exactly the row format Snowflake would produce.
    This validates column indexing, opt_float, null_count math, and
    histogram assembly without needing a live Snowflake account.
    """

    def _make_mock_cursor(self, fetchone_val=None, fetchall_val=None):
        from unittest.mock import MagicMock

        cursor = MagicMock()
        cursor.fetchone.return_value = fetchone_val
        cursor.fetchall.return_value = fetchall_val or []
        return cursor

    def test_numeric_stats(self):
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.snowflake import (
            _snowflake_sql_numeric_stats,
        )

        vals = NUMERIC_VALUES
        mean_v = statistics.mean(vals)
        stddev_v = statistics.stdev(vals)
        # row: COUNT(*), then per-feature: nn, avg, stddev, min, max, p50..p99
        stats_row = (
            10,
            10,
            mean_v,
            stddev_v,
            1.0,
            10.0,
            5.5,
            7.75,
            9.1,
            9.55,
            9.91,
        )
        stats_cursor = self._make_mock_cursor(fetchone_val=stats_row)
        hist_row_data = [(1, 2), (2, 2), (3, 2), (4, 2), (5, 2)]
        hist_cursor = self._make_mock_cursor(fetchall_val=hist_row_data)

        call_count = [0]

        def mock_execute(conn, query):
            call_count[0] += 1
            return stats_cursor if call_count[0] == 1 else hist_cursor

        with patch(
            "feast.infra.offline_stores.snowflake.execute_snowflake_statement",
            side_effect=mock_execute,
        ):
            results = _snowflake_sql_numeric_stats(
                MagicMock(),
                "test_table",
                ["numeric_col"],
                "1=1",
                histogram_bins=5,
            )

        assert len(results) == 1
        r = results[0]
        expected = _expected_numeric_stats()
        assert_numeric_correctness(r, expected, "snowflake_numeric")
        assert r["histogram"] is not None
        assert sum(r["histogram"]["counts"]) == 10

    def test_numeric_stats_with_nulls(self):
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.snowflake import (
            _snowflake_sql_numeric_stats,
        )

        vals = NON_NULL_VALUES
        stats_row = (
            10,
            5,
            statistics.mean(vals),
            statistics.stdev(vals),
            1.0,
            9.0,
            5.0,
            7.0,
            8.6,
            8.8,
            8.96,
        )
        stats_cursor = self._make_mock_cursor(fetchone_val=stats_row)
        hist_cursor = self._make_mock_cursor(
            fetchall_val=[(1, 1), (2, 1), (3, 1), (4, 1), (5, 1)]
        )

        call_count = [0]

        def mock_execute(conn, query):
            call_count[0] += 1
            return stats_cursor if call_count[0] == 1 else hist_cursor

        with patch(
            "feast.infra.offline_stores.snowflake.execute_snowflake_statement",
            side_effect=mock_execute,
        ):
            results = _snowflake_sql_numeric_stats(
                MagicMock(),
                "t",
                ["col"],
                "1=1",
                histogram_bins=5,
            )

        r = results[0]
        assert r["null_count"] == 5
        assert r["null_rate"] == pytest.approx(0.5)
        assert r["mean"] == pytest.approx(5.0, abs=1e-4)

    def test_categorical_stats(self):
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.snowflake import (
            _snowflake_sql_categorical_stats,
        )

        rows = [
            (10, 0, 4, "a", 4),
            (10, 0, 4, "b", 3),
            (10, 0, 4, "c", 2),
            (10, 0, 4, "d", 1),
        ]
        cursor = self._make_mock_cursor(fetchall_val=rows)

        with patch(
            "feast.infra.offline_stores.snowflake.execute_snowflake_statement",
            return_value=cursor,
        ):
            result = _snowflake_sql_categorical_stats(
                MagicMock(),
                "t",
                "cat_col",
                "1=1",
                top_n=10,
            )

        expected = _expected_categorical_stats()
        assert_categorical_correctness(result, expected, "snowflake_categorical")

    def test_empty_result(self):
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.snowflake import (
            _snowflake_sql_numeric_stats,
        )

        cursor = self._make_mock_cursor(fetchone_val=None)
        with patch(
            "feast.infra.offline_stores.snowflake.execute_snowflake_statement",
            return_value=cursor,
        ):
            results = _snowflake_sql_numeric_stats(
                MagicMock(),
                "t",
                ["col"],
                "1=1",
                histogram_bins=5,
            )

        assert len(results) == 1
        assert results[0]["mean"] is None
        assert results[0]["row_count"] == 0


# ===================================================================
# BigQuery compute correctness (mocked client, real parsing)
# ===================================================================


class TestBigQueryComputeCorrectness:
    """Tests BigQuery result parsing with mocked client.

    BigQuery results use dict-like row access (row["column_name"]).
    """

    def _make_mock_bq_row(self, data: dict):
        """Create an object supporting both dict-key and index access."""

        class BQRow:
            def __init__(self, d):
                self._data = d
                self._keys = list(d.keys())

            def __getitem__(self, key):
                if isinstance(key, int):
                    return self._data[self._keys[key]]
                return self._data[key]

        return BQRow(data)

    def _make_mock_job(self, rows):
        from unittest.mock import MagicMock

        job = MagicMock()
        job.result.return_value = None
        job.__iter__ = lambda self_: iter(rows)
        return job

    def test_numeric_stats(self):
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.bigquery import _bq_numeric_stats

        vals = NUMERIC_VALUES
        row_data = {
            "_row_count": 10,
            "c0_nn": 10,
            "c0_avg": statistics.mean(vals),
            "c0_stddev": statistics.stdev(vals),
            "c0_min": 1.0,
            "c0_max": 10.0,
            "c0_p50": 5.5,
            "c0_p75": 7.75,
            "c0_p90": 9.1,
            "c0_p95": 9.55,
            "c0_p99": 9.91,
        }
        stats_row = self._make_mock_bq_row(row_data)
        stats_job = self._make_mock_job([stats_row])

        hist_rows = [
            self._make_mock_bq_row({"bucket": i + 1, "cnt": 2}) for i in range(5)
        ]
        hist_job = self._make_mock_job(hist_rows)

        call_count = [0]

        def mock_query(sql, *args, **kwargs):
            call_count[0] += 1
            return stats_job if call_count[0] == 1 else hist_job

        mock_config = MagicMock()
        mock_config.offline_store.billing_project_id = "proj"
        mock_config.offline_store.project_id = "proj"
        mock_config.offline_store.location = "US"

        with patch(
            "feast.infra.offline_stores.bigquery._get_bigquery_client"
        ) as mock_client:
            mock_client.return_value.query = mock_query
            results = _bq_numeric_stats(
                mock_config,
                "test_table",
                ["numeric_col"],
                "1=1",
                histogram_bins=5,
            )

        assert len(results) == 1
        r = results[0]
        expected = _expected_numeric_stats()
        assert_numeric_correctness(r, expected, "bq_numeric")
        assert r["histogram"] is not None
        assert sum(r["histogram"]["counts"]) == 10

    def test_numeric_stats_with_nulls(self):
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.bigquery import _bq_numeric_stats

        vals = NON_NULL_VALUES
        row_data = {
            "_row_count": 10,
            "c0_nn": 5,
            "c0_avg": statistics.mean(vals),
            "c0_stddev": statistics.stdev(vals),
            "c0_min": 1.0,
            "c0_max": 9.0,
            "c0_p50": 5.0,
            "c0_p75": 7.0,
            "c0_p90": 8.6,
            "c0_p95": 8.8,
            "c0_p99": 8.96,
        }
        stats_row = self._make_mock_bq_row(row_data)
        stats_job = self._make_mock_job([stats_row])
        hist_job = self._make_mock_job(
            [self._make_mock_bq_row({"bucket": i + 1, "cnt": 1}) for i in range(5)]
        )

        call_count = [0]

        def mock_query(sql, *args, **kwargs):
            call_count[0] += 1
            return stats_job if call_count[0] == 1 else hist_job

        mock_config = MagicMock()
        mock_config.offline_store.billing_project_id = "proj"
        mock_config.offline_store.project_id = "proj"
        mock_config.offline_store.location = "US"

        with patch(
            "feast.infra.offline_stores.bigquery._get_bigquery_client"
        ) as mock_client:
            mock_client.return_value.query = mock_query
            results = _bq_numeric_stats(
                mock_config,
                "t",
                ["col"],
                "1=1",
                histogram_bins=5,
            )

        r = results[0]
        assert r["null_count"] == 5
        assert r["null_rate"] == pytest.approx(0.5)
        assert r["mean"] == pytest.approx(5.0, abs=1e-4)

    def test_categorical_stats(self):
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.bigquery import _bq_categorical_stats

        rows = [
            self._make_mock_bq_row(
                {
                    "row_count": 10,
                    "null_count": 0,
                    "unique_count": 4,
                    "value": "a",
                    "cnt": 4,
                }
            ),
            self._make_mock_bq_row(
                {
                    "row_count": 10,
                    "null_count": 0,
                    "unique_count": 4,
                    "value": "b",
                    "cnt": 3,
                }
            ),
            self._make_mock_bq_row(
                {
                    "row_count": 10,
                    "null_count": 0,
                    "unique_count": 4,
                    "value": "c",
                    "cnt": 2,
                }
            ),
            self._make_mock_bq_row(
                {
                    "row_count": 10,
                    "null_count": 0,
                    "unique_count": 4,
                    "value": "d",
                    "cnt": 1,
                }
            ),
        ]
        job = self._make_mock_job(rows)

        mock_config = MagicMock()
        mock_config.offline_store.billing_project_id = "proj"
        mock_config.offline_store.project_id = "proj"
        mock_config.offline_store.location = "US"

        with patch(
            "feast.infra.offline_stores.bigquery._get_bigquery_client"
        ) as mock_client:
            mock_client.return_value.query.return_value = job
            result = _bq_categorical_stats(
                mock_config,
                "t",
                "cat_col",
                "1=1",
                top_n=10,
            )

        expected = _expected_categorical_stats()
        assert_categorical_correctness(result, expected, "bq_categorical")

    def test_multiple_features(self):
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.bigquery import _bq_numeric_stats

        row_data = {
            "_row_count": 10,
            "c0_nn": 10,
            "c0_avg": 5.5,
            "c0_stddev": 3.03,
            "c0_min": 1.0,
            "c0_max": 10.0,
            "c0_p50": 5.5,
            "c0_p75": 7.75,
            "c0_p90": 9.1,
            "c0_p95": 9.55,
            "c0_p99": 9.91,
            "c1_nn": 5,
            "c1_avg": 5.0,
            "c1_stddev": 3.16,
            "c1_min": 1.0,
            "c1_max": 9.0,
            "c1_p50": 5.0,
            "c1_p75": 7.0,
            "c1_p90": 8.6,
            "c1_p95": 8.8,
            "c1_p99": 8.96,
        }
        stats_job = self._make_mock_job([self._make_mock_bq_row(row_data)])
        hist_job = self._make_mock_job(
            [self._make_mock_bq_row({"bucket": i + 1, "cnt": 2}) for i in range(5)]
        )

        call_count = [0]

        def mock_query(sql, *args, **kwargs):
            call_count[0] += 1
            return stats_job if call_count[0] == 1 else hist_job

        mock_config = MagicMock()
        mock_config.offline_store.billing_project_id = "p"
        mock_config.offline_store.project_id = "p"
        mock_config.offline_store.location = "US"

        with patch(
            "feast.infra.offline_stores.bigquery._get_bigquery_client"
        ) as mock_client:
            mock_client.return_value.query = mock_query
            results = _bq_numeric_stats(
                mock_config,
                "t",
                ["col_a", "col_b"],
                "1=1",
                histogram_bins=5,
            )

        assert len(results) == 2
        assert results[0]["mean"] == pytest.approx(5.5, abs=1e-2)
        assert results[1]["mean"] == pytest.approx(5.0, abs=1e-2)
        assert results[0]["null_count"] == 0
        assert results[1]["null_count"] == 5


# ===================================================================
# Redshift compute correctness (mocked Data API, real parsing)
# ===================================================================


class TestRedshiftComputeCorrectness:
    """Tests Redshift result parsing with mocked _redshift_execute_fetch_rows.

    Redshift Data API returns rows as lists of field dicts, e.g.
    [{"longValue": 10}, {"doubleValue": 5.5}, ...].
    """

    def _long(self, v):
        return {"longValue": v}

    def _double(self, v):
        return {"doubleValue": v}

    def _string(self, v):
        return {"stringValue": v}

    def _null(self):
        return {"isNull": True}

    def test_numeric_stats(self):
        from unittest.mock import patch

        from feast.infra.offline_stores.redshift import (
            _redshift_sql_numeric_stats,
        )

        vals = NUMERIC_VALUES
        row = [
            self._long(10),  # COUNT(*)
            self._long(10),  # COUNT(col)
            self._double(statistics.mean(vals)),  # AVG
            self._double(statistics.stdev(vals)),  # STDDEV_SAMP
            self._double(1.0),  # MIN
            self._double(10.0),  # MAX
            self._double(5.5),  # p50
            self._double(7.75),  # p75
            self._double(9.1),  # p90
            self._double(9.55),  # p95
            self._double(9.91),  # p99
        ]
        hist_rows = [[self._long(i + 1), self._long(2)] for i in range(5)]

        call_count = [0]

        def mock_fetch(config, sql):
            call_count[0] += 1
            return [row] if call_count[0] == 1 else hist_rows

        with patch(
            "feast.infra.offline_stores.redshift._redshift_execute_fetch_rows",
            side_effect=mock_fetch,
        ):
            from unittest.mock import MagicMock

            results = _redshift_sql_numeric_stats(
                MagicMock(),
                "test_table",
                ["numeric_col"],
                "1=1",
                histogram_bins=5,
            )

        assert len(results) == 1
        r = results[0]
        expected = _expected_numeric_stats()
        assert_numeric_correctness(r, expected, "redshift_numeric")
        assert r["histogram"] is not None
        assert sum(r["histogram"]["counts"]) == 10

    def test_numeric_stats_with_nulls(self):
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.redshift import (
            _redshift_sql_numeric_stats,
        )

        vals = NON_NULL_VALUES
        row = [
            self._long(10),
            self._long(5),
            self._double(statistics.mean(vals)),
            self._double(statistics.stdev(vals)),
            self._double(1.0),
            self._double(9.0),
            self._double(5.0),
            self._double(7.0),
            self._double(8.6),
            self._double(8.8),
            self._double(8.96),
        ]
        hist_rows = [[self._long(i + 1), self._long(1)] for i in range(5)]

        call_count = [0]

        def mock_fetch(config, sql):
            call_count[0] += 1
            return [row] if call_count[0] == 1 else hist_rows

        with patch(
            "feast.infra.offline_stores.redshift._redshift_execute_fetch_rows",
            side_effect=mock_fetch,
        ):
            results = _redshift_sql_numeric_stats(
                MagicMock(),
                "t",
                ["col"],
                "1=1",
                histogram_bins=5,
            )

        r = results[0]
        assert r["null_count"] == 5
        assert r["null_rate"] == pytest.approx(0.5)
        assert r["mean"] == pytest.approx(5.0, abs=1e-4)

    def test_categorical_stats(self):
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.redshift import (
            _redshift_sql_categorical_stats,
        )

        rows = [
            [
                self._long(10),
                self._long(0),
                self._long(4),
                self._string("a"),
                self._long(4),
            ],
            [
                self._long(10),
                self._long(0),
                self._long(4),
                self._string("b"),
                self._long(3),
            ],
            [
                self._long(10),
                self._long(0),
                self._long(4),
                self._string("c"),
                self._long(2),
            ],
            [
                self._long(10),
                self._long(0),
                self._long(4),
                self._string("d"),
                self._long(1),
            ],
        ]

        with patch(
            "feast.infra.offline_stores.redshift._redshift_execute_fetch_rows",
            return_value=rows,
        ):
            result = _redshift_sql_categorical_stats(
                MagicMock(),
                "t",
                "cat_col",
                "1=1",
                top_n=10,
            )

        expected = _expected_categorical_stats()
        assert_categorical_correctness(result, expected, "redshift_categorical")

    def test_empty_result(self):
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.redshift import (
            _redshift_sql_numeric_stats,
        )

        with patch(
            "feast.infra.offline_stores.redshift._redshift_execute_fetch_rows",
            return_value=[],
        ):
            results = _redshift_sql_numeric_stats(
                MagicMock(),
                "t",
                ["col"],
                "1=1",
                histogram_bins=5,
            )

        assert len(results) == 1
        assert results[0]["mean"] is None
        assert results[0]["row_count"] == 0


# ===================================================================
# Spark compute correctness tests (requires SparkSession)
# ===================================================================


def _spark_available():
    try:
        from pyspark.sql import SparkSession  # noqa: F401

        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _spark_available(), reason="PySpark not installed")
class TestSparkComputeCorrectness:
    """Test Spark SQL helpers produce correct metric values.

    Uses a local SparkSession — no external cluster required.
    """

    @pytest.fixture(autouse=True)
    def setup_spark(self):
        from pyspark.sql import SparkSession
        from pyspark.sql.types import (
            DoubleType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        try:
            self.spark = (
                SparkSession.builder.master("local[1]")
                .appName("feast_monitoring_test")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate()
            )
        except Exception as e:
            pytest.skip(f"SparkSession unavailable: {e}")

        schema = StructType(
            [
                StructField("event_timestamp", TimestampType(), False),
                StructField("numeric_col", DoubleType(), True),
                StructField("numeric_with_nulls", DoubleType(), True),
                StructField("categorical_col", StringType(), True),
            ]
        )

        ts = datetime(2025, 1, 15, 12, 0, 0)
        rows = [
            (ts, NUMERIC_VALUES[i], NUMERIC_WITH_NULLS[i], CATEGORICAL_VALUES[i])
            for i in range(ROW_COUNT)
        ]
        df = self.spark.createDataFrame(rows, schema)
        df.createOrReplaceTempView("feast_test_monitoring")

        yield
        self.spark.sql("DROP VIEW IF EXISTS feast_test_monitoring")

    def test_numeric_stats(self):
        from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
            _spark_sql_numeric_stats,
        )

        results = _spark_sql_numeric_stats(
            self.spark,
            "feast_test_monitoring",
            ["numeric_col"],
            "1=1",
            histogram_bins=5,
        )

        assert len(results) == 1
        expected = _expected_numeric_stats()
        assert_numeric_correctness(
            results[0],
            expected,
            "spark_numeric",
            approx_percentiles=True,
        )
        assert results[0]["histogram"] is not None
        assert sum(results[0]["histogram"]["counts"]) == 10

    def test_numeric_stats_with_nulls(self):
        from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
            _spark_sql_numeric_stats,
        )

        results = _spark_sql_numeric_stats(
            self.spark,
            "feast_test_monitoring",
            ["numeric_with_nulls"],
            "1=1",
            histogram_bins=5,
        )

        expected = _expected_numeric_with_nulls_stats()
        assert_numeric_correctness(
            results[0],
            expected,
            "spark_numeric_nulls",
            approx_percentiles=True,
        )

    def test_categorical_stats(self):
        from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
            _spark_sql_categorical_stats,
        )

        result = _spark_sql_categorical_stats(
            self.spark,
            "feast_test_monitoring",
            "categorical_col",
            "1=1",
            top_n=10,
        )

        expected = _expected_categorical_stats()
        assert_categorical_correctness(result, expected, "spark_categorical")

    def test_numeric_multiple_features(self):
        from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
            _spark_sql_numeric_stats,
        )

        results = _spark_sql_numeric_stats(
            self.spark,
            "feast_test_monitoring",
            ["numeric_col", "numeric_with_nulls"],
            "1=1",
            histogram_bins=5,
        )

        assert len(results) == 2
        assert results[0]["mean"] == pytest.approx(5.5, abs=1e-4)
        assert results[1]["mean"] == pytest.approx(5.0, abs=1e-4)

    def test_categorical_top_n_truncation(self):
        from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
            _spark_sql_categorical_stats,
        )

        result = _spark_sql_categorical_stats(
            self.spark,
            "feast_test_monitoring",
            "categorical_col",
            "1=1",
            top_n=2,
        )

        assert len(result["histogram"]["values"]) == 2
        assert result["histogram"]["other_count"] > 0
        total = (
            sum(e["count"] for e in result["histogram"]["values"])
            + result["histogram"]["other_count"]
        )
        assert total == 10


# ===================================================================
# Oracle compute correctness (mocked Ibis connection, real parsing)
# ===================================================================


def _oracle_importable():
    try:
        from feast.infra.offline_stores.contrib.oracle_offline_store.oracle import (
            _oracle_numeric_stats,  # noqa: F401
        )

        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _oracle_importable(), reason="Oracle deps not installed")
class TestOracleComputeCorrectness:
    """Tests Oracle result parsing with mocked Ibis connection.

    _oracle_fetchall returns list of tuples (positional indexing).
    """

    def test_numeric_stats(self):
        from unittest.mock import patch

        from feast.infra.offline_stores.contrib.oracle_offline_store.oracle import (
            _oracle_numeric_stats,
        )

        vals = NUMERIC_VALUES
        row = (
            10,
            10,
            statistics.mean(vals),
            statistics.stdev(vals),
            1.0,
            10.0,
            5.5,
            7.75,
            9.1,
            9.55,
            9.91,
        )
        hist_rows = [(i + 1, 2) for i in range(5)]

        call_count = [0]

        def mock_fetchall(con, sql):
            call_count[0] += 1
            return [row] if call_count[0] == 1 else hist_rows

        with patch(
            "feast.infra.offline_stores.contrib.oracle_offline_store.oracle._oracle_fetchall",
            side_effect=mock_fetchall,
        ):
            results = _oracle_numeric_stats(
                None,
                "test_table",
                ["numeric_col"],
                "1=1",
                histogram_bins=5,
            )

        assert len(results) == 1
        r = results[0]
        expected = _expected_numeric_stats()
        assert_numeric_correctness(r, expected, "oracle_numeric")
        assert r["histogram"] is not None
        assert sum(r["histogram"]["counts"]) == 10

    def test_numeric_stats_with_nulls(self):
        from unittest.mock import patch

        from feast.infra.offline_stores.contrib.oracle_offline_store.oracle import (
            _oracle_numeric_stats,
        )

        vals = NON_NULL_VALUES
        row = (
            10,
            5,
            statistics.mean(vals),
            statistics.stdev(vals),
            1.0,
            9.0,
            5.0,
            7.0,
            8.6,
            8.8,
            8.96,
        )
        hist_rows = [(i + 1, 1) for i in range(5)]

        call_count = [0]

        def mock_fetchall(con, sql):
            call_count[0] += 1
            return [row] if call_count[0] == 1 else hist_rows

        with patch(
            "feast.infra.offline_stores.contrib.oracle_offline_store.oracle._oracle_fetchall",
            side_effect=mock_fetchall,
        ):
            results = _oracle_numeric_stats(
                None,
                "t",
                ["col"],
                "1=1",
                histogram_bins=5,
            )

        r = results[0]
        assert r["null_count"] == 5
        assert r["null_rate"] == pytest.approx(0.5)
        assert r["mean"] == pytest.approx(5.0, abs=1e-4)

    def test_categorical_stats(self):
        from unittest.mock import patch

        from feast.infra.offline_stores.contrib.oracle_offline_store.oracle import (
            _oracle_categorical_stats,
        )

        rows = [
            (10, 0, 4, "a", 4),
            (10, 0, 4, "b", 3),
            (10, 0, 4, "c", 2),
            (10, 0, 4, "d", 1),
        ]

        with patch(
            "feast.infra.offline_stores.contrib.oracle_offline_store.oracle._oracle_fetchall",
            return_value=rows,
        ):
            result = _oracle_categorical_stats(
                None,
                "t",
                "cat_col",
                "1=1",
                top_n=10,
            )

        expected = _expected_categorical_stats()
        assert_categorical_correctness(result, expected, "oracle_categorical")

    def test_empty_result(self):
        from unittest.mock import patch

        from feast.infra.offline_stores.contrib.oracle_offline_store.oracle import (
            _oracle_numeric_stats,
        )

        with patch(
            "feast.infra.offline_stores.contrib.oracle_offline_store.oracle._oracle_fetchall",
            return_value=[None],
        ):
            results = _oracle_numeric_stats(
                None,
                "t",
                ["col"],
                "1=1",
                histogram_bins=5,
            )

        assert len(results) == 1
        assert results[0]["mean"] is None
        assert results[0]["row_count"] == 0

    def test_multiple_features(self):
        from unittest.mock import patch

        from feast.infra.offline_stores.contrib.oracle_offline_store.oracle import (
            _oracle_numeric_stats,
        )

        row = (
            10,
            # Feature 0: numeric_col
            10,
            5.5,
            3.03,
            1.0,
            10.0,
            5.5,
            7.75,
            9.1,
            9.55,
            9.91,
            # Feature 1: numeric_with_nulls
            5,
            5.0,
            3.16,
            1.0,
            9.0,
            5.0,
            7.0,
            8.6,
            8.8,
            8.96,
        )
        hist_rows = [(i + 1, 2) for i in range(5)]

        call_count = [0]

        def mock_fetchall(con, sql):
            call_count[0] += 1
            return [row] if call_count[0] == 1 else hist_rows

        with patch(
            "feast.infra.offline_stores.contrib.oracle_offline_store.oracle._oracle_fetchall",
            side_effect=mock_fetchall,
        ):
            results = _oracle_numeric_stats(
                None,
                "t",
                ["col_a", "col_b"],
                "1=1",
                histogram_bins=5,
            )

        assert len(results) == 2
        assert results[0]["mean"] == pytest.approx(5.5, abs=1e-2)
        assert results[1]["mean"] == pytest.approx(5.0, abs=1e-2)
        assert results[0]["null_count"] == 0
        assert results[1]["null_count"] == 5


# ===================================================================
# Cross-backend consistency: MetricsCalculator vs DuckDB vs Dask
# ===================================================================


class TestCrossBackendConsistency:
    """Verify that DuckDB, Dask, and MetricsCalculator produce
    consistent results for the same dataset."""

    def test_numeric_mean_matches_across_backends(self):
        duckdb = pytest.importorskip("duckdb")
        from feast.infra.offline_stores.dask import _dask_compute_numeric_metrics
        from feast.infra.offline_stores.duckdb import _duckdb_numeric_stats
        from feast.monitoring.metrics_calculator import MetricsCalculator

        calc = MetricsCalculator(histogram_bins=5, top_n=10)
        arr = pa.array(NUMERIC_VALUES, type=pa.float64())
        pyarrow_result = calc.compute_numeric(arr)

        col = pa.chunked_array([arr])
        dask_result = _dask_compute_numeric_metrics(col, histogram_bins=5)

        conn = duckdb.connect()
        conn.execute("CREATE TABLE consistency_test (v DOUBLE)")
        for v in NUMERIC_VALUES:
            conn.execute(f"INSERT INTO consistency_test VALUES ({v})")

        duckdb_results = _duckdb_numeric_stats(
            conn,
            "consistency_test",
            ["v"],
            "1=1",
            histogram_bins=5,
        )
        conn.close()

        duckdb_result = duckdb_results[0]

        assert pyarrow_result["mean"] == pytest.approx(dask_result["mean"], abs=1e-6)
        assert pyarrow_result["mean"] == pytest.approx(duckdb_result["mean"], abs=1e-6)
        assert dask_result["mean"] == pytest.approx(duckdb_result["mean"], abs=1e-6)

        assert pyarrow_result["stddev"] == pytest.approx(
            dask_result["stddev"], abs=0.01
        )
        assert pyarrow_result["stddev"] == pytest.approx(
            duckdb_result["stddev"], abs=0.01
        )

        assert pyarrow_result["min_val"] == dask_result["min_val"]
        assert pyarrow_result["min_val"] == duckdb_result["min_val"]
        assert pyarrow_result["max_val"] == dask_result["max_val"]
        assert pyarrow_result["max_val"] == duckdb_result["max_val"]

    def test_categorical_unique_count_matches(self):
        duckdb = pytest.importorskip("duckdb")
        from feast.infra.offline_stores.dask import (
            _dask_compute_categorical_metrics,
        )
        from feast.infra.offline_stores.duckdb import _duckdb_categorical_stats
        from feast.monitoring.metrics_calculator import MetricsCalculator

        calc = MetricsCalculator(histogram_bins=5, top_n=10)
        arr = pa.array(CATEGORICAL_VALUES, type=pa.string())
        pyarrow_result = calc.compute_categorical(arr)

        col = pa.chunked_array([arr])
        dask_result = _dask_compute_categorical_metrics(col, top_n=10)

        conn = duckdb.connect()
        conn.execute("CREATE TABLE cat_consistency (v VARCHAR)")
        for v in CATEGORICAL_VALUES:
            conn.execute(f"INSERT INTO cat_consistency VALUES ('{v}')")

        duckdb_result = _duckdb_categorical_stats(
            conn,
            "cat_consistency",
            "v",
            "1=1",
            top_n=10,
        )
        conn.close()

        assert (
            pyarrow_result["histogram"]["unique_count"]
            == dask_result["histogram"]["unique_count"]
            == duckdb_result["histogram"]["unique_count"]
            == 4
        )

        pyarrow_top = pyarrow_result["histogram"]["values"][0]
        dask_top = dask_result["histogram"]["values"][0]
        duckdb_top = duckdb_result["histogram"]["values"][0]
        assert pyarrow_top["value"] == dask_top["value"] == duckdb_top["value"] == "a"
        assert pyarrow_top["count"] == dask_top["count"] == duckdb_top["count"] == 4

    def test_null_rate_matches_across_backends(self):
        duckdb = pytest.importorskip("duckdb")
        from feast.infra.offline_stores.dask import _dask_compute_numeric_metrics
        from feast.infra.offline_stores.duckdb import _duckdb_numeric_stats
        from feast.monitoring.metrics_calculator import MetricsCalculator

        calc = MetricsCalculator(histogram_bins=5, top_n=10)
        arr = pa.array(NUMERIC_WITH_NULLS, type=pa.float64())
        pyarrow_result = calc.compute_numeric(arr)

        col = pa.chunked_array([arr])
        dask_result = _dask_compute_numeric_metrics(col, histogram_bins=5)

        conn = duckdb.connect()
        conn.execute("CREATE TABLE null_consistency (v DOUBLE)")
        for v in NUMERIC_WITH_NULLS:
            val = f"{v}" if v is not None else "NULL"
            conn.execute(f"INSERT INTO null_consistency VALUES ({val})")

        duckdb_results = _duckdb_numeric_stats(
            conn,
            "null_consistency",
            ["v"],
            "1=1",
            histogram_bins=5,
        )
        conn.close()

        assert pyarrow_result["null_rate"] == pytest.approx(0.5, abs=1e-6)
        assert dask_result["null_rate"] == pytest.approx(0.5, abs=1e-6)
        assert duckdb_results[0]["null_rate"] == pytest.approx(0.5, abs=1e-6)
