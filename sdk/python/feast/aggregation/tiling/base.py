"""
Base utilities for tiling.

- Orchestrator works in pure pandas
- Engine-specific nodes do direct DataFrame conversions.
"""

from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from feast.aggregation import Aggregation


@dataclass
class IRMetadata:
    """
    Metadata about intermediate representations for an aggregation.

    Attributes:
        type: "algebraic" (sum, count, max, min) or "holistic" (avg, std, var)
        ir_columns: List of IR column names (e.g., ["sum", "count"] for avg)
        computation: String describing how to compute final value from IRs
    """

    type: str  # "algebraic" or "holistic"
    ir_columns: Optional[List[str]] = None
    computation: Optional[str] = None


def get_ir_metadata_for_aggregation(
    agg: Aggregation, feature_name: str
) -> Tuple[List[Tuple[str, Any]], IRMetadata]:
    """
    Get intermediate representation metadata for an aggregation.

    This function determines what intermediate values need to be stored
    to correctly compute holistic aggregations when merging tiles.

    For example:
    - avg: store sum and count (not just final average)
    - std: store sum, count, and sum_of_squares
    - sum: just store sum (algebraic, no IRs needed)

    Args:
        agg: Aggregation specification
        feature_name: Name of the feature being aggregated

    Returns:
        Tuple of (expression_list, metadata)
    """
    agg_type = agg.function.lower()

    # Algebraic aggregations
    if agg_type in ["sum", "count", "max", "min"]:
        return ([], IRMetadata(type="algebraic"))

    # Holistic aggregations (need intermediate representations)
    elif agg_type in ["avg", "mean"]:
        # Average needs sum and count
        ir_cols = [
            f"_tail_{feature_name}_sum",
            f"_tail_{feature_name}_count",
        ]
        return (
            [],
            IRMetadata(
                type="avg",  # Mark as avg for special handling
                ir_columns=ir_cols,
                computation="sum / count",
            ),
        )

    elif agg_type in ["std", "stddev", "var", "variance"]:
        # Std/Var need sum, count, and sum of squares
        ir_cols = [
            f"_tail_{feature_name}_sum",
            f"_tail_{feature_name}_count",
            f"_tail_{feature_name}_sum_sq",
        ]
        return (
            [],
            IRMetadata(
                type="holistic",
                ir_columns=ir_cols,
                computation="sqrt((sum_sq - sum^2/count) / (count-1))",
            ),
        )

    else:
        # Unknown aggregation: treat as algebraic
        return ([], IRMetadata(type="algebraic"))
