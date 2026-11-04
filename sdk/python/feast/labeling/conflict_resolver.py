"""Offline-store conflict resolution for LabelView labels.

Applies the configured ConflictPolicy to a DataFrame containing all historical
label rows (from pull_all_from_table_or_query), producing one resolved row per
entity key.

The online store continues to use LAST_WRITE_WINS regardless of policy — this
resolver is for offline/batch reads used in training data generation, the UI
browse/quality endpoints, and any batch pipeline consuming resolved labels.
"""

from typing import List, Optional

import pandas as pd

from feast.labeling.conflict_policy import ConflictPolicy


def resolve_conflicts(
    df: pd.DataFrame,
    join_key_columns: List[str],
    feature_name_columns: List[str],
    timestamp_field: str,
    labeler_field: str,
    conflict_policy: ConflictPolicy,
    labeler_priorities: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Resolve label conflicts by applying the configured policy.

    Args:
        df: Full history DataFrame (all rows, not deduplicated).
        join_key_columns: Entity key column names.
        feature_name_columns: Label/feature column names.
        timestamp_field: Event timestamp column name.
        labeler_field: Column identifying who wrote the label.
        conflict_policy: The resolution strategy to apply.
        labeler_priorities: Ordered list of labelers from highest to lowest
            priority. Required for LABELER_PRIORITY policy.

    Returns:
        DataFrame with one resolved row per unique entity key combination.
    """
    if df.empty:
        return df

    if conflict_policy == ConflictPolicy.LAST_WRITE_WINS:
        return _resolve_last_write_wins(df, join_key_columns, timestamp_field)
    elif conflict_policy == ConflictPolicy.LABELER_PRIORITY:
        return _resolve_labeler_priority(
            df, join_key_columns, timestamp_field, labeler_field, labeler_priorities
        )
    elif conflict_policy == ConflictPolicy.MAJORITY_VOTE:
        return _resolve_majority_vote(
            df, join_key_columns, feature_name_columns, timestamp_field
        )
    else:
        return _resolve_last_write_wins(df, join_key_columns, timestamp_field)


def _resolve_last_write_wins(
    df: pd.DataFrame,
    join_key_columns: List[str],
    timestamp_field: str,
) -> pd.DataFrame:
    """Keep only the row with the latest timestamp per entity."""
    df_sorted = df.sort_values(timestamp_field, ascending=False)
    return df_sorted.drop_duplicates(subset=join_key_columns, keep="first").reset_index(
        drop=True
    )


def _resolve_labeler_priority(
    df: pd.DataFrame,
    join_key_columns: List[str],
    timestamp_field: str,
    labeler_field: str,
    labeler_priorities: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Pick the label from the highest-priority labeler per entity.

    If multiple rows exist from the same priority labeler, the latest timestamp
    wins. Labelers not in the priority list are ranked lowest.
    """
    if not labeler_priorities:
        return _resolve_last_write_wins(df, join_key_columns, timestamp_field)

    priority_map = {name: i for i, name in enumerate(labeler_priorities)}
    max_priority = len(labeler_priorities)

    df = df.copy()
    df["_priority_rank"] = df[labeler_field].map(
        lambda x: priority_map.get(x, max_priority)
    )
    df_sorted = df.sort_values(
        ["_priority_rank", timestamp_field], ascending=[True, False]
    )
    result = df_sorted.drop_duplicates(subset=join_key_columns, keep="first")
    result = result.drop(columns=["_priority_rank"])
    return result.reset_index(drop=True)


def _resolve_majority_vote(
    df: pd.DataFrame,
    join_key_columns: List[str],
    feature_name_columns: List[str],
    timestamp_field: str,
) -> pd.DataFrame:
    """For each entity, pick the most common value per feature column.

    Uses the most frequent value across all labelers for each feature.
    Ties are broken by recency (latest timestamp wins).
    """
    if not feature_name_columns:
        return _resolve_last_write_wins(df, join_key_columns, timestamp_field)

    groups = df.groupby(join_key_columns, sort=False)
    resolved_rows = []

    for keys, group in groups:
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(join_key_columns, keys))

        for col in feature_name_columns:
            value_counts = group[col].value_counts()
            if value_counts.empty:
                row[col] = None
                continue
            top_value = value_counts.index[0]
            top_count = value_counts.iloc[0]

            # Tie-breaking: if multiple values have the same count, pick the
            # one with the most recent timestamp
            tied = value_counts[value_counts == top_count]
            if len(tied) > 1:
                tied_values = tied.index.tolist()
                tied_rows = group[group[col].isin(tied_values)]
                latest_row = tied_rows.sort_values(
                    timestamp_field, ascending=False
                ).iloc[0]
                row[col] = latest_row[col]
            else:
                row[col] = top_value

        row[timestamp_field] = group[timestamp_field].max()
        if "labeler" in group.columns and "labeler" not in join_key_columns:
            row["labeler"] = "majority_vote"

        resolved_rows.append(row)

    if not resolved_rows:
        return df.head(0)

    result = pd.DataFrame(resolved_rows)
    # Preserve column order from original
    cols = [c for c in df.columns if c in result.columns]
    return result[cols].reset_index(drop=True)
