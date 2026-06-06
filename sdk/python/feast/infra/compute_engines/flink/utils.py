from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd
import pyarrow as pa

if TYPE_CHECKING:
    from feast.infra.compute_engines.flink.compute import FlinkComputeEngineConfig


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
    schema = list(df.columns)
    return table_env.from_pandas(df, schema=schema, splits_num=split_num)


def flink_table_to_pandas(table: Any) -> pd.DataFrame:
    """Collect a PyFlink table into pandas."""
    if hasattr(table, "to_pandas"):
        return table.to_pandas()
    raise TypeError(f"Expected a PyFlink table, got {type(table)}")


def flink_table_to_arrow(table: Any) -> pa.Table:
    """Collect a PyFlink table into Arrow."""
    value = flink_table_to_pandas(table)
    return pa.Table.from_pandas(value)
