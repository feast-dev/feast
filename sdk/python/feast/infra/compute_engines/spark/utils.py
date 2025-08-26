import time
from typing import Dict, Iterable, Literal, Optional

import pandas as pd
import pyarrow
import pyarrow as pa
from pyspark import SparkConf
from pyspark.sql import SparkSession

from feast.infra.common.serde import SerializedArtifacts
from feast.utils import _convert_arrow_to_proto, _run_pyarrow_field_mapping


def get_or_create_new_spark_session(
    spark_config: Optional[Dict[str, str]] = None,
) -> SparkSession:
    spark_session = SparkSession.getActiveSession()
    if not spark_session:
        spark_builder = SparkSession.builder
        if spark_config:
            spark_builder = spark_builder.config(
                conf=SparkConf().setAll([(k, v) for k, v in spark_config.items()])
            )

        spark_session = spark_builder.getOrCreate()
    spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    return spark_session


def map_in_arrow(
    iterator: Iterable[pa.RecordBatch],
    serialized_artifacts: "SerializedArtifacts",
    mode: Literal["online", "offline"] = "online",
):
    for batch in iterator:
        table = pa.Table.from_batches([batch])

        (
            feature_view,
            online_store,
            offline_store,
            repo_config,
        ) = serialized_artifacts.unserialize()

        if mode == "online":
            join_key_to_value_type = {
                entity.name: entity.dtype.to_value_type()
                for entity in feature_view.entity_columns
            }

            rows_to_write = _convert_arrow_to_proto(
                table, feature_view, join_key_to_value_type
            )

            online_store.online_write_batch(
                config=repo_config,
                table=feature_view,
                data=rows_to_write,
                progress=lambda x: None,
            )
        if mode == "offline":
            offline_store.offline_write_batch(
                config=repo_config,
                feature_view=feature_view,
                table=table,
                progress=lambda x: None,
            )

        yield batch


def map_in_pandas(iterator, serialized_artifacts: SerializedArtifacts):
    (
        feature_view,
        online_store,
        _,
        repo_config,
    ) = serialized_artifacts.unserialize()

    if (
        hasattr(repo_config.batch_engine, "suppress_warnings")
        and repo_config.batch_engine.suppress_warnings
    ):
        import os
        import warnings

        os.environ["PYTHONWARNINGS"] = "ignore::DeprecationWarning"
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        warnings.filterwarnings(
            "ignore", message=".*is_categorical_dtype is deprecated.*"
        )
        warnings.filterwarnings(
            "ignore", message=".*is_datetime64tz_dtype is deprecated.*"
        )
        warnings.filterwarnings(
            "ignore", message=".*distutils Version classes are deprecated.*"
        )

    total_batches = 0
    total_time = 0.0
    min_time = float("inf")
    max_time = float("-inf")

    total_rows = 0
    min_batch_size = float("inf")
    max_batch_size = float("-inf")

    for pdf in iterator:
        start_time = time.perf_counter()
        pdf_row_count = pdf.shape[0]
        if pdf.shape[0] == 0:
            print("Skipping")
            return

        table = pyarrow.Table.from_pandas(pdf)

        if feature_view.batch_source.field_mapping is not None:
            # Spark offline store does the field mapping in pull_latest_from_table_or_query() call
            # This may be needed in future if this materialization engine supports other offline stores
            table = _run_pyarrow_field_mapping(
                table, feature_view.batch_source.field_mapping
            )

        join_key_to_value_type = {
            entity.name: entity.dtype.to_value_type()
            for entity in feature_view.entity_columns
        }

        rows_to_write = _convert_arrow_to_proto(
            table, feature_view, join_key_to_value_type
        )
        online_store.online_write_batch(
            repo_config,
            feature_view,
            rows_to_write,
            lambda x: None,
        )

        batch_time = time.perf_counter() - start_time

        (
            total_batches,
            total_time,
            min_time,
            max_time,
            total_rows,
            min_batch_size,
            max_batch_size,
        ) = update_exec_stats(
            total_batches,
            total_time,
            min_time,
            max_time,
            total_rows,
            min_batch_size,
            max_batch_size,
            batch_time,
            pdf_row_count,
        )

    if total_batches > 0:
        print_exec_stats(
            total_batches,
            total_time,
            min_time,
            max_time,
            total_rows,
            min_batch_size,
            max_batch_size,
        )

    yield pd.DataFrame(
        [pd.Series(range(1, 2))]
    )  # dummy result because mapInPandas needs to return something


def update_exec_stats(
    total_batches,
    total_time,
    min_time,
    max_time,
    total_rows,
    min_batch_size,
    max_batch_size,
    batch_time,
    current_batch_size,
):
    total_batches += 1
    total_time += batch_time
    min_time = min(min_time, batch_time)
    max_time = max(max_time, batch_time)

    total_rows += current_batch_size
    min_batch_size = min(min_batch_size, current_batch_size)
    max_batch_size = max(max_batch_size, current_batch_size)

    return (
        total_batches,
        total_time,
        min_time,
        max_time,
        total_rows,
        min_batch_size,
        max_batch_size,
    )


def print_exec_stats(
    total_batches,
    total_time,
    min_time,
    max_time,
    total_rows,
    min_batch_size,
    max_batch_size,
):
    # TODO: Investigate why the logger is not working in Spark Executors
    avg_time = total_time / total_batches
    avg_batch_size = total_rows / total_batches
    print(
        f"Time - Total: {total_time:.6f}s, Avg: {avg_time:.6f}s, Min: {min_time:.6f}s, Max: {max_time:.6f}s | "
        f"Batch Size - Total: {total_rows}, Avg: {avg_batch_size:.2f}, Min: {min_batch_size}, Max: {max_batch_size}"
    )
