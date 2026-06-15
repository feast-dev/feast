from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Iterator, Optional

import pandas as pd
import pyarrow as pa

if TYPE_CHECKING:
    from feast.infra.compute_engines.flink.compute import FlinkComputeEngineConfig

logger = logging.getLogger(__name__)

DEFAULT_FLINK_RESULT_BATCH_SIZE = 10_000
_TEMP_VIEW_REGISTRY: dict[int, set[str]] = {}


def create_flink_table_environment(config: FlinkComputeEngineConfig) -> Any:
    """Create a PyFlink TableEnvironment from Feast engine config."""
    try:
        from pyflink.common import Configuration
        from pyflink.table import EnvironmentSettings, TableEnvironment
    except ImportError as exc:
        raise ImportError(
            "FlinkComputeEngine requires PyFlink. Install the `flink` extra with "
            "uv from a Feast source checkout, or otherwise make the `pyflink` "
            "package available to Feast."
        ) from exc

    flink_conf = Configuration()
    for key, value in (config.table_config or {}).items():
        flink_conf.set_string(key, value)
    if config.parallelism is not None:
        flink_conf.set_string("parallelism.default", str(config.parallelism))

    builder = EnvironmentSettings.new_instance().with_configuration(flink_conf)
    if config.execution_mode == "streaming":
        builder = builder.in_streaming_mode()
    else:
        builder = builder.in_batch_mode()
    return TableEnvironment.create(builder.build())


def pandas_to_flink_table(table_env: Any, df: pd.DataFrame, split_num: int = 1) -> Any:
    """Convert a pandas DataFrame to a PyFlink table."""
    schema = _build_pandas_flink_schema(df)
    kwargs: dict[str, Any] = {"splits_num": split_num}
    if schema is not None:
        kwargs["schema"] = schema
    return table_env.from_pandas(df, **kwargs)


def flink_table_to_pandas(table: Any) -> pd.DataFrame:
    """Collect a PyFlink table into pandas."""
    if hasattr(table, "to_pandas"):
        return table.to_pandas()
    raise TypeError(f"Expected a PyFlink table, got {type(table)}")


def flink_table_to_arrow(table: Any) -> pa.Table:
    """Collect a PyFlink table into Arrow."""
    value = flink_table_to_pandas(table)
    return pa.Table.from_pandas(value)


def flink_table_to_arrow_batches(
    table: Any,
    columns: list[str],
    batch_size: Optional[int],
) -> Iterator[pa.Table]:
    """Stream a PyFlink table into Arrow tables without collecting it all first."""
    effective_batch_size = batch_size or DEFAULT_FLINK_RESULT_BATCH_SIZE
    if effective_batch_size <= 0:
        raise ValueError("Flink result batch size must be positive.")

    if hasattr(table, "execute"):
        rows: list[dict[str, Any]] = []
        row_iterator = table.execute().collect()
        try:
            for row in row_iterator:
                rows.append(_row_to_dict(row, columns))
                if len(rows) >= effective_batch_size:
                    yield _rows_to_arrow(rows, columns)
                    rows = []
            if rows:
                yield _rows_to_arrow(rows, columns)
        finally:
            close = getattr(row_iterator, "close", None)
            if callable(close):
                close()
        return

    if hasattr(table, "to_pandas"):
        # Used by unit-test fakes and other table-like objects. Real PyFlink tables use
        # the execute().collect() path above.
        df = table.to_pandas()
        for start in range(0, len(df), effective_batch_size):
            yield pa.Table.from_pandas(
                df.iloc[start : start + effective_batch_size],
                preserve_index=False,
            )
        return

    raise TypeError(f"Expected a PyFlink table, got {type(table)}")


def register_flink_temporary_view(table_env: Any, view_name: str) -> None:
    """Track Feast-created temporary views so long-lived table envs can clean up."""
    views = _temporary_views_for_env(table_env)
    views.add(view_name)


def cleanup_flink_temporary_views(table_env: Any) -> None:
    """Drop Feast-created temporary views from a PyFlink TableEnvironment."""
    views = _temporary_views_for_env(table_env)
    if not views:
        return

    drop_view = getattr(table_env, "drop_temporary_view", None)
    if not callable(drop_view):
        return

    for view_name in list(views):
        try:
            drop_view(view_name)
        except Exception as exc:
            logger.debug("Failed to drop Flink temporary view %s: %s", view_name, exc)
        finally:
            views.discard(view_name)
    if not views:
        _TEMP_VIEW_REGISTRY.pop(id(table_env), None)


def _build_pandas_flink_schema(df: pd.DataFrame) -> Any:
    try:
        from pyflink.table import Schema
    except ImportError:
        return None

    builder = Schema.new_builder()
    for column in df.columns:
        builder.column(str(column), _pandas_dtype_to_flink_type(df[column]))
    return builder.build()


def _pandas_dtype_to_flink_type(series: pd.Series) -> Any:
    from pyflink.table import DataTypes

    dtype = series.dtype
    if pd.api.types.is_bool_dtype(dtype):
        return DataTypes.BOOLEAN()
    if pd.api.types.is_integer_dtype(dtype):
        dtype_name = str(dtype).lower()
        if dtype_name.endswith("int8"):
            return DataTypes.TINYINT()
        if dtype_name.endswith("int16"):
            return DataTypes.SMALLINT()
        if dtype_name.endswith("int32"):
            return DataTypes.INT()
        return DataTypes.BIGINT()
    if pd.api.types.is_float_dtype(dtype):
        return DataTypes.FLOAT() if str(dtype) == "float32" else DataTypes.DOUBLE()
    if pd.api.types.is_datetime64_any_dtype(dtype):
        if getattr(dtype, "tz", None) is not None:
            return DataTypes.TIMESTAMP_LTZ(3)
        return DataTypes.TIMESTAMP(3)
    if pd.api.types.is_timedelta64_dtype(dtype):
        return DataTypes.BIGINT()

    sample = _first_non_null_value(series)
    if isinstance(sample, (bytes, bytearray)):
        return DataTypes.BYTES()
    if isinstance(sample, date) and not isinstance(sample, datetime):
        return DataTypes.DATE()
    if isinstance(sample, Decimal):
        return DataTypes.DECIMAL(38, 18)
    return DataTypes.STRING()


def _first_non_null_value(series: pd.Series) -> Any:
    for value in series:
        if pd.notna(value):
            return value
    return None


def _row_to_dict(row: Any, columns: list[str]) -> dict[str, Any]:
    if isinstance(row, dict):
        return {column: row.get(column) for column in columns}

    as_dict = getattr(row, "as_dict", None)
    if callable(as_dict):
        row_dict = as_dict()
        if row_dict:
            return {column: row_dict.get(column) for column in columns}

    values = list(row)
    return dict(zip(columns, values))


def _rows_to_arrow(rows: list[dict[str, Any]], columns: list[str]) -> pa.Table:
    df = pd.DataFrame.from_records(rows, columns=columns)
    return pa.Table.from_pandas(df, preserve_index=False)


def _temporary_views_for_env(table_env: Any) -> set[str]:
    views = getattr(table_env, "_feast_temporary_views", None)
    if isinstance(views, set):
        return views

    views = _TEMP_VIEW_REGISTRY.setdefault(id(table_env), set())
    try:
        setattr(table_env, "_feast_temporary_views", views)
    except Exception:
        pass
    return views
