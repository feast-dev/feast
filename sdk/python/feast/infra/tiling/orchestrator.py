"""
Tiling orchestrator.

This module provides the core tiling logic using pure pandas operations.
Engines (Spark, Ray, etc.) just need to convert to/from pandas.
"""

from datetime import timedelta
from typing import Any, Callable, Dict, List, Tuple, Union

import numpy as np
import pandas as pd

from feast.aggregation import Aggregation
from feast.infra.tiling.base import get_ir_metadata_for_aggregation


def apply_sawtooth_window_tiling(
    df: pd.DataFrame,
    aggregations: List[Aggregation],
    group_by_keys: List[str],
    timestamp_col: str,
    window_size: timedelta,
    hop_size: timedelta,
) -> pd.DataFrame:
    """
    Generate cumulative tiles.

    This function creates cumulative "tail" aggregations that can be efficiently
    merged to compute sliding window aggregations. For sawtooth windows, tiles
    at time T contain the aggregation from the start of the window to T.

    Args:
        df: Pandas DataFrame with input data
        aggregations: List of aggregation specifications
        group_by_keys: Entity column names for grouping
        timestamp_col: Timestamp column name
        window_size: Size of the time window
        hop_size: Size of hop intervals for tiling

    Returns:
        Pandas DataFrame with cumulative tiles
    """
    if df.empty:
        return df

    # Step 1: Add hop interval column
    hop_size_ms = int(hop_size.total_seconds() * 1000)

    # Convert timestamp to milliseconds
    if pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
        timestamp_ms = df[timestamp_col].astype("int64") // 10**6
    else:
        timestamp_ms = pd.to_datetime(df[timestamp_col]).astype("int64") // 10**6

    # Compute hop interval (inclusive lower boundaries)
    df["_hop_interval"] = (timestamp_ms // hop_size_ms) * hop_size_ms

    # Step 2: Group by entity keys + hop interval and aggregate
    agg_dict: Dict[str, Tuple[str, Union[str, Callable[[Any], Any]]]] = {}
    ir_metadata_dict = {}

    for agg in aggregations:
        feature_name = (
            f"{agg.function}_{agg.column}_{int(window_size.total_seconds())}s"
        )
        _, metadata = get_ir_metadata_for_aggregation(agg, feature_name)
        ir_metadata_dict[feature_name] = metadata

        # Build pandas aggregation dict based on IR metadata
        if metadata.type == "algebraic":
            # Algebraic aggregations: just aggregate directly
            if agg.function == "sum":
                agg_dict[f"_tail_{feature_name}"] = (agg.column, "sum")
            elif agg.function == "count":
                agg_dict[f"_tail_{feature_name}"] = (agg.column, "count")
            elif agg.function == "max":
                agg_dict[f"_tail_{feature_name}"] = (agg.column, "max")
            elif agg.function == "min":
                agg_dict[f"_tail_{feature_name}"] = (agg.column, "min")

        elif metadata.type in ("holistic", "avg", "std", "var"):
            # Holistic aggregations: compute IRs
            if metadata.ir_columns:
                ir_column_names = metadata.ir_columns

                # For avg: compute sum and count
                if len(ir_column_names) >= 2:
                    agg_dict[ir_column_names[0]] = (agg.column, "sum")  # sum
                    agg_dict[ir_column_names[1]] = (agg.column, "count")  # count

                    # For std/var: also compute sum of squares
                    if len(ir_column_names) >= 3:
                        agg_dict[ir_column_names[2]] = (
                            agg.column,
                            lambda x: (x**2).sum(),
                        )  # sum_sq

    # Perform aggregation within each hop
    grouped = df.groupby(group_by_keys + ["_hop_interval"], as_index=False)
    hop_aggregated = grouped.agg(**agg_dict) if agg_dict else grouped.first()

    # Step 3: Compute cumulative sums (convert hop aggregations to cumulative tiles)
    result = hop_aggregated.copy()

    # Sort by entity keys and hop interval
    result = result.sort_values(by=group_by_keys + ["_hop_interval"]).reset_index(
        drop=True
    )

    # For each IR column, compute cumulative sum within each entity group
    ir_columns = [
        col for col in result.columns if col not in group_by_keys + ["_hop_interval"]
    ]

    if ir_columns:
        # Generate a complete grid of hop intervals for forward-filling
        # This ensures we have tiles even where no data exists
        min_hop = result["_hop_interval"].min()
        max_hop = result["_hop_interval"].max()
        all_hops = range(int(min_hop), int(max_hop) + hop_size_ms, hop_size_ms)

        # Create cumulative tiles for each entity
        cumulative_results = []
        for entity_values, group_df in result.groupby(group_by_keys):
            # Create complete hop grid for this entity
            entity_dict = dict(
                zip(
                    group_by_keys,
                    entity_values
                    if isinstance(entity_values, tuple)
                    else (entity_values,),
                )
            )
            complete_grid = pd.DataFrame(
                {**entity_dict, "_hop_interval": list(all_hops)}
            )

            # Left join actual data
            merged = complete_grid.merge(
                group_df, on=group_by_keys + ["_hop_interval"], how="left"
            )

            # Fill NaN with 0 for IR columns
            for col in ir_columns:
                if col in merged.columns:
                    merged[col] = merged[col].fillna(0)

            # Compute cumulative sum for IR columns
            for col in ir_columns:
                if col in merged.columns:
                    merged[col] = merged[col].cumsum()

            cumulative_results.append(merged)

        result = pd.concat(cumulative_results, ignore_index=True)

    # Step 4: Add tile metadata
    result["_tile_start"] = result["_hop_interval"]
    result["_tile_end"] = result["_hop_interval"] + hop_size_ms

    # Step 5: Compute final feature values from IRs (for algebraic aggs, just rename)
    for agg in aggregations:
        feature_name = (
            f"{agg.function}_{agg.column}_{int(window_size.total_seconds())}s"
        )
        metadata = ir_metadata_dict[feature_name]

        if metadata.type == "algebraic":
            # For algebraic, final value = IR value
            ir_col = f"_tail_{feature_name}"
            if ir_col in result.columns:
                result[feature_name] = result[ir_col]

        elif metadata.type in ("holistic", "avg", "std", "var"):
            # For holistic, compute final value from IRs
            if metadata.ir_columns:
                # Use IR column names directly (they have _tail_ prefix)
                ir_column_names = metadata.ir_columns

                if len(ir_column_names) >= 2:
                    sum_col = ir_column_names[0]
                    count_col = ir_column_names[1]

                    if sum_col in result.columns and count_col in result.columns:
                        # Compute avg = sum / count
                        result[feature_name] = np.where(
                            result[count_col] > 0,
                            result[sum_col] / result[count_col],
                            0,
                        )

    return result
