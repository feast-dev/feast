"""
Tile subtraction logic.

This module provides the core algorithm for converting cumulative tiles
to windowed aggregations. All compute engines use this shared logic.
"""

from datetime import timedelta
from typing import List

import pandas as pd

from feast.aggregation import Aggregation
from feast.infra.tiling.base import get_ir_metadata_for_aggregation


def convert_cumulative_to_windowed(
    tiles_df: pd.DataFrame,
    entity_keys: List[str],
    timestamp_col: str,
    window_size: timedelta,
    aggregations: List[Aggregation],
) -> pd.DataFrame:
    """
    Convert cumulative tiles to windowed aggregations.

    This function performs the core mathematical operation:
        windowed_agg_at_T = cumulative_tile_at_T - cumulative_tile_at_(T-window_size)

    For holistic aggregations (avg, std, var), it:
    1. Subtracts intermediate representation (IR) components
    2. Recomputes the final value from windowed IRs

    Args:
        tiles_df: DataFrame with cumulative tiles from orchestrator.
                  Must contain: entity_keys, _tile_end, IR columns, feature columns
        entity_keys: Entity column names for grouping
        timestamp_col: Name of the timestamp column to create
        window_size: Size of the time window for aggregations
        aggregations: List of aggregation specifications

    Returns:
        DataFrame with windowed aggregations ready for online store.
        Contains: entity_keys, timestamp_col, feature columns (no IR columns or tile metadata)
    """
    if tiles_df.empty:
        return tiles_df

    window_size_ms = int(window_size.total_seconds() * 1000)

    windowed_results = []

    # Group by entity keys to process each entity separately
    for entity_group, group_df in tiles_df.groupby(entity_keys):
        # Sort by _tile_end to ensure correct temporal ordering
        group_df = group_df.sort_values("_tile_end").reset_index(drop=True)

        # For each tile, compute windowed aggregation by subtracting previous tile
        for idx, row in group_df.iterrows():
            tile_end = int(row["_tile_end"])
            window_start = tile_end - window_size_ms

            exact_match = group_df[group_df["_tile_end"] == window_start]
            if len(exact_match) > 0:
                prev_tile = exact_match.iloc[0]
                has_prev_tile = True
            else:
                # Only use exact matches to ensure correct window boundaries
                # If no exact match exists, any previous tile would end before
                # window_start, which would compute an incorrect window that's
                # larger than requested
                has_prev_tile = False

            # Create windowed row (will contain windowed aggregations)
            windowed_row = row.to_dict()

            # Subtract previous tile values from current tile for each aggregation
            for agg in aggregations:
                feature_name = (
                    f"{agg.function}_{agg.column}_{int(window_size.total_seconds())}s"
                )
                _, metadata = get_ir_metadata_for_aggregation(agg, feature_name)

                if metadata.type == "algebraic":
                    # For algebraic aggregations: sum and count can be subtracted
                    if feature_name in group_df.columns:
                        current_val = float(row[feature_name])

                        if agg.function.lower() in ("max", "min"):
                            if has_prev_tile and feature_name in prev_tile.index:
                                prev_val = float(prev_tile[feature_name])
                                windowed_row[feature_name] = current_val
                            else:
                                windowed_row[feature_name] = current_val
                        else:
                            # For sum and count: subtract previous from current
                            if has_prev_tile and feature_name in prev_tile.index:
                                prev_val = float(prev_tile[feature_name])
                            else:
                                prev_val = 0.0
                            windowed_row[feature_name] = current_val - prev_val

                elif metadata.type in ("holistic", "avg", "std", "var"):
                    # For holistic aggregations:
                    # 1. Subtract each IR component: windowed_IR = current_IR - previous_IR
                    # 2. Recompute final value from windowed IRs
                    if metadata.ir_columns:
                        ir_column_names = metadata.ir_columns

                        # Subtract each IR component
                        for ir_col in ir_column_names:
                            if ir_col in group_df.columns:
                                current_ir = row[ir_col]
                                if has_prev_tile and ir_col in prev_tile.index:
                                    prev_ir = prev_tile[ir_col]
                                else:
                                    prev_ir = 0
                                windowed_row[ir_col] = current_ir - prev_ir

                        # Recompute final value from windowed IRs
                        if len(ir_column_names) >= 2:
                            sum_col = ir_column_names[
                                0
                            ]  # e.g., "_tail_avg_amount_3600s_sum"
                            count_col = ir_column_names[
                                1
                            ]  # e.g., "_tail_avg_amount_3600s_count"

                            if sum_col in windowed_row and count_col in windowed_row:
                                count_val = windowed_row[count_col]
                                if count_val <= 0:
                                    windowed_row[feature_name] = 0
                                else:
                                    # avg = windowed_sum / windowed_count
                                    windowed_row[feature_name] = (
                                        windowed_row[sum_col] / count_val
                                    )

            # Set event_timestamp to the tile end time
            windowed_row[timestamp_col] = pd.to_datetime(tile_end, unit="ms")

            windowed_results.append(windowed_row)

    if not windowed_results:
        return pd.DataFrame()

    # Create DataFrame from windowed results
    result_df = pd.DataFrame(windowed_results)

    # Drop internal tile metadata columns
    cols_to_drop = ["_tile_start", "_tile_end", "_hop_interval"]
    result_df = result_df.drop(
        columns=[c for c in cols_to_drop if c in result_df.columns]
    )
    return result_df


def deduplicate_keep_latest(
    df: pd.DataFrame, entity_keys: List[str], timestamp_col: str
) -> pd.DataFrame:
    """
    Keep only the latest timestamp per entity.

    Args:
        df: DataFrame with entity_keys and timestamp_col
        entity_keys: List of entity key column names
        timestamp_col: Name of timestamp column

    Returns:
        DataFrame with one row per entity (latest timestamp)
    """
    if df.empty or timestamp_col not in df.columns:
        return df

    return (
        df.sort_values(by=timestamp_col, ascending=False)
        .groupby(entity_keys, as_index=False)
        .first()
    )
