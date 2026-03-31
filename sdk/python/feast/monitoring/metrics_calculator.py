import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from feast.types import PrimitiveFeastType

logger = logging.getLogger(__name__)

_NUMERIC_TYPES = {
    PrimitiveFeastType.INT32,
    PrimitiveFeastType.INT64,
    PrimitiveFeastType.FLOAT32,
    PrimitiveFeastType.FLOAT64,
    PrimitiveFeastType.DECIMAL,
}

_CATEGORICAL_TYPES = {
    PrimitiveFeastType.STRING,
    PrimitiveFeastType.BOOL,
}


class MetricsCalculator:
    def __init__(self, histogram_bins: int = 20, top_n: int = 10):
        self.histogram_bins = histogram_bins
        self.top_n = top_n

    @staticmethod
    def classify_feature(dtype) -> Optional[str]:
        primitive = dtype
        if hasattr(dtype, "base_type"):
            primitive = dtype.base_type if dtype.base_type else dtype

        if isinstance(primitive, PrimitiveFeastType):
            if primitive in _NUMERIC_TYPES:
                return "numeric"
            if primitive in _CATEGORICAL_TYPES:
                return "categorical"
        return None

    def compute_numeric(self, array: pa.Array) -> Dict:
        total = len(array)
        null_count = array.null_count
        result = {
            "feature_type": "numeric",
            "row_count": total,
            "null_count": null_count,
            "null_rate": null_count / total if total > 0 else 0.0,
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

        valid = pc.drop_null(array)
        if len(valid) == 0:
            return result

        float_array = pc.cast(valid, pa.float64())
        result["mean"] = pc.mean(float_array).as_py()
        result["stddev"] = pc.stddev(float_array, ddof=1).as_py()

        min_max = pc.min_max(float_array)
        result["min_val"] = min_max["min"].as_py()
        result["max_val"] = min_max["max"].as_py()

        quantiles = pc.quantile(float_array, q=[0.50, 0.75, 0.90, 0.95, 0.99])
        q_values = quantiles.to_pylist()
        result["p50"] = q_values[0]
        result["p75"] = q_values[1]
        result["p90"] = q_values[2]
        result["p95"] = q_values[3]
        result["p99"] = q_values[4]

        np_array = float_array.to_numpy()
        counts, bin_edges = np.histogram(np_array, bins=self.histogram_bins)
        result["histogram"] = {
            "bins": bin_edges.tolist(),
            "counts": counts.tolist(),
            "bin_width": float(bin_edges[1] - bin_edges[0])
            if len(bin_edges) > 1
            else 0,
        }

        return result

    def compute_categorical(self, array: pa.Array) -> Dict:
        total = len(array)
        null_count = array.null_count
        result = {
            "feature_type": "categorical",
            "row_count": total,
            "null_count": null_count,
            "null_rate": null_count / total if total > 0 else 0.0,
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

        valid = pc.drop_null(array)
        if len(valid) == 0:
            return result

        value_counts = pc.value_counts(valid)
        entries = [
            {"value": vc["values"].as_py(), "count": vc["counts"].as_py()}
            for vc in value_counts
        ]
        entries.sort(key=lambda x: x["count"], reverse=True)

        unique_count = len(entries)
        top_entries = entries[: self.top_n]
        other_count = sum(e["count"] for e in entries[self.top_n :])

        result["histogram"] = {
            "values": top_entries,
            "other_count": other_count,
            "unique_count": unique_count,
        }

        return result

    def compute_all(
        self,
        table: pa.Table,
        feature_fields: List[Tuple[str, str]],
    ) -> List[Dict]:
        results = []
        for name, ftype in feature_fields:
            if name not in table.column_names:
                logger.warning("Column '%s' not found in arrow table, skipping", name)
                continue
            column = table.column(name)
            if ftype == "numeric":
                metrics = self.compute_numeric(column)
            elif ftype == "categorical":
                metrics = self.compute_categorical(column)
            else:
                continue
            metrics["feature_name"] = name
            results.append(metrics)
        return results
