"""
Utility functions for dataset creation and entity DataFrame construction.

These helpers are used by the FeatureStore SDK and gRPC handlers to build
entity DataFrames from inline input and to resolve storage backend classes.
"""

import importlib
from typing import List, Optional, Union


def coerce_value(val: str) -> Union[int, float, str]:
    """Try to coerce a string value to int or float, otherwise keep as string."""
    try:
        return int(val)
    except ValueError:
        pass
    try:
        return float(val)
    except ValueError:
        pass
    return val


def build_saved_dataset_storage(storage_type: str, path: str):
    """Build a SavedDatasetStorage object from type string and path/table reference.

    Supports: file (default), bigquery, snowflake, redshift.
    Unknown types fall back to file storage.
    """
    from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

    storage_classes = {}
    for mod, cls_name, key in [
        (
            "feast.infra.offline_stores.bigquery_source",
            "SavedDatasetBigQueryStorage",
            "bigquery",
        ),
        (
            "feast.infra.offline_stores.snowflake_source",
            "SavedDatasetSnowflakeStorage",
            "snowflake",
        ),
        (
            "feast.infra.offline_stores.redshift_source",
            "SavedDatasetRedshiftStorage",
            "redshift",
        ),
    ]:
        try:
            m = importlib.import_module(mod)
            storage_classes[key] = getattr(m, cls_name)
        except (ImportError, AttributeError):
            pass

    if storage_type == "bigquery" and "bigquery" in storage_classes:
        return storage_classes["bigquery"](table=path)
    elif storage_type == "snowflake" and "snowflake" in storage_classes:
        return storage_classes["snowflake"](table_ref=path)
    elif storage_type == "redshift" and "redshift" in storage_classes:
        return storage_classes["redshift"](table_ref=path)
    else:
        return SavedDatasetFileStorage(path=path)


def build_entity_df_from_inline(
    entity_keys: List[str],
    entity_values: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    extra_columns: Optional[str] = None,
):
    """Build a pandas DataFrame from inline entity key values and optional time range.

    Args:
        entity_keys: Column names for the entity keys (e.g., ["driver_id"]).
        entity_values: Comma or newline separated values string.
        start_date: Optional ISO start date for event_timestamp range.
        end_date: Optional ISO end date for event_timestamp range.
        extra_columns: Optional newline-separated "col=value" pairs to add as
            constant columns (used for ODFV request-data inputs).

    Returns:
        pandas DataFrame with entity columns and event_timestamp.

    Raises:
        ValueError: If no entity values could be parsed.
    """
    import pandas as pd

    values_str = entity_values.strip()
    rows: list = []

    if len(entity_keys) == 1:
        raw_vals = [
            v.strip() for v in values_str.replace("\n", ",").split(",") if v.strip()
        ]
        for val in raw_vals:
            rows.append({entity_keys[0]: coerce_value(val)})
    else:
        lines = [line.strip() for line in values_str.split("\n") if line.strip()]
        for line in lines:
            parts = [v.strip() for v in line.split(",")]
            row = {}
            for i, key in enumerate(entity_keys):
                row[key] = coerce_value(parts[i]) if i < len(parts) else None
            rows.append(row)

    if not rows:
        raise ValueError("No entity values could be parsed from the input.")

    entity_df = pd.DataFrame(rows)

    if start_date and end_date:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        n = len(entity_df)
        if n == 1:
            entity_df["event_timestamp"] = [end]
        else:
            timestamps = pd.date_range(start=start, end=end, periods=n)
            entity_df["event_timestamp"] = timestamps
    elif end_date:
        entity_df["event_timestamp"] = pd.to_datetime(end_date)
    else:
        entity_df["event_timestamp"] = pd.Timestamp.now()

    if extra_columns:
        for col_line in extra_columns.strip().split("\n"):
            col_line = col_line.strip()
            if "=" in col_line:
                col_name, col_value = col_line.split("=", 1)
                col_name = col_name.strip()
                col_value = col_value.strip()
                if col_name:
                    entity_df[col_name] = coerce_value(col_value)

    return entity_df
