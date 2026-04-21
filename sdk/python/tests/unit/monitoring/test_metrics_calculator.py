import pyarrow as pa
import pytest

from feast.monitoring.metrics_calculator import MetricsCalculator
from feast.types import PrimitiveFeastType


def _make_calc(bins=20, top_n=10):
    return MetricsCalculator(histogram_bins=bins, top_n=top_n)


class TestClassifyFeature:
    @pytest.mark.parametrize(
        "dtype, expected",
        [
            (PrimitiveFeastType.INT32, "numeric"),
            (PrimitiveFeastType.INT64, "numeric"),
            (PrimitiveFeastType.FLOAT32, "numeric"),
            (PrimitiveFeastType.FLOAT64, "numeric"),
            (PrimitiveFeastType.STRING, "categorical"),
            (PrimitiveFeastType.BOOL, "categorical"),
            (PrimitiveFeastType.BYTES, None),
            (PrimitiveFeastType.UNIX_TIMESTAMP, None),
        ],
    )
    def test_classification(self, dtype, expected):
        assert MetricsCalculator.classify_feature(dtype) == expected


class TestComputeNumeric:
    def test_basic_stats(self):
        calc = _make_calc()
        arr = pa.array([1.0, 2.0, 3.0, 4.0, 5.0])
        result = calc.compute_numeric(arr)

        assert result["feature_type"] == "numeric"
        assert result["row_count"] == 5
        assert result["null_count"] == 0
        assert result["null_rate"] == 0.0
        assert result["mean"] == pytest.approx(3.0)
        assert result["min_val"] == 1.0
        assert result["max_val"] == 5.0
        assert result["p50"] is not None
        assert result["histogram"] is not None
        assert "bins" in result["histogram"]
        assert "counts" in result["histogram"]

    def test_with_nulls(self):
        calc = _make_calc()
        arr = pa.array([1.0, None, 3.0, None, 5.0])
        result = calc.compute_numeric(arr)

        assert result["row_count"] == 5
        assert result["null_count"] == 2
        assert result["null_rate"] == pytest.approx(0.4)
        assert result["mean"] == pytest.approx(3.0)

    def test_all_nulls(self):
        calc = _make_calc()
        arr = pa.array([None, None, None], type=pa.float64())
        result = calc.compute_numeric(arr)

        assert result["null_count"] == 3
        assert result["mean"] is None
        assert result["histogram"] is None

    def test_empty_array(self):
        calc = _make_calc()
        arr = pa.array([], type=pa.float64())
        result = calc.compute_numeric(arr)

        assert result["row_count"] == 0
        assert result["null_rate"] == 0.0

    def test_single_value(self):
        calc = _make_calc()
        arr = pa.array([42.0])
        result = calc.compute_numeric(arr)

        assert result["mean"] == 42.0
        assert result["min_val"] == 42.0
        assert result["max_val"] == 42.0

    def test_histogram_bin_count(self):
        calc = _make_calc(bins=5)
        arr = pa.array(list(range(100)), type=pa.float64())
        result = calc.compute_numeric(arr)

        assert len(result["histogram"]["counts"]) == 5
        assert len(result["histogram"]["bins"]) == 6

    def test_percentiles_order(self):
        calc = _make_calc()
        arr = pa.array(list(range(1000)), type=pa.float64())
        result = calc.compute_numeric(arr)

        assert result["p50"] <= result["p75"]
        assert result["p75"] <= result["p90"]
        assert result["p90"] <= result["p95"]
        assert result["p95"] <= result["p99"]


class TestComputeCategorical:
    def test_basic(self):
        calc = _make_calc()
        arr = pa.array(["a", "b", "a", "c", "a", "b"])
        result = calc.compute_categorical(arr)

        assert result["feature_type"] == "categorical"
        assert result["row_count"] == 6
        assert result["null_count"] == 0
        assert result["histogram"] is not None
        assert result["histogram"]["unique_count"] == 3

        top_values = {v["value"] for v in result["histogram"]["values"]}
        assert "a" in top_values

    def test_with_nulls(self):
        calc = _make_calc()
        arr = pa.array(["a", None, "b", None])
        result = calc.compute_categorical(arr)

        assert result["null_count"] == 2
        assert result["null_rate"] == 0.5

    def test_high_cardinality(self):
        calc = _make_calc(top_n=3)
        arr = pa.array([f"val_{i}" for i in range(100)])
        result = calc.compute_categorical(arr)

        assert len(result["histogram"]["values"]) == 3
        assert result["histogram"]["unique_count"] == 100
        assert result["histogram"]["other_count"] == 97

    def test_all_nulls(self):
        calc = _make_calc()
        arr = pa.array([None, None], type=pa.string())
        result = calc.compute_categorical(arr)

        assert result["null_count"] == 2
        assert result["histogram"] is None


class TestComputeAll:
    def test_mixed_features(self):
        calc = _make_calc()
        table = pa.table(
            {
                "age": [25, 30, 35, 40],
                "city": ["NYC", "LA", "NYC", "SF"],
            }
        )
        fields = [("age", "numeric"), ("city", "categorical")]
        results = calc.compute_all(table, fields)

        assert len(results) == 2
        assert results[0]["feature_name"] == "age"
        assert results[0]["feature_type"] == "numeric"
        assert results[1]["feature_name"] == "city"
        assert results[1]["feature_type"] == "categorical"

    def test_missing_column_skipped(self):
        calc = _make_calc()
        table = pa.table({"age": [25, 30]})
        fields = [("age", "numeric"), ("missing_col", "numeric")]
        results = calc.compute_all(table, fields)

        assert len(results) == 1
        assert results[0]["feature_name"] == "age"
