"""
Unit tests for SparkOfflineStore.offline_write_batch with path-based sources.

Covers https://github.com/feast-dev/feast/issues/6955: offline_write_batch only
used `file_format`, so a source configured with `table_format` (and no
`file_format`) was read and written with `format(None)`.
"""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from feast.infra.offline_stores.contrib.spark_offline_store import spark as spark_mod
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStore,
    SparkOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.table_format import DeltaFormat


def _write_batch(source):
    table = pa.table({"a": [1]})
    feature_view = MagicMock()
    feature_view.batch_source = source
    config = MagicMock()
    config.offline_store = MagicMock(spec=SparkOfflineStoreConfig)
    session = MagicMock()
    with (
        patch.object(
            spark_mod,
            "get_spark_session_or_start_new_with_repoconfig",
            return_value=session,
        ),
        patch.object(
            spark_mod.offline_utils,
            "get_pyarrow_schema_from_batch_source",
            return_value=(table.schema, ["a"]),
        ),
        patch.object(spark_mod, "_cast_data_frame", side_effect=lambda b, e: b),
    ):
        SparkOfflineStore.offline_write_batch(config, feature_view, table, None)
    return session


def test_offline_write_batch_uses_file_format():
    source = SparkSource(
        name="s", path="/tmp/x", file_format="parquet", timestamp_field="ts"
    )
    session = _write_batch(source)

    session.read.format.assert_called_once_with("parquet")
    writer = session.read.parquet.return_value.write
    writer.format.assert_called_once_with("parquet")
    writer.format.return_value.mode.assert_called_once_with("append")
    writer.format.return_value.mode.return_value.save.assert_called_once_with("/tmp/x")


def test_offline_write_batch_uses_table_format_without_file_format():
    source = SparkSource(
        name="s",
        path="/tmp/x",
        table_format=DeltaFormat(checkpoint_location="/tmp/ckpt"),
        timestamp_field="ts",
    )
    session = _write_batch(source)

    session.read.format.assert_called_once_with("delta")
    writer = session.read.parquet.return_value.write
    writer.format.assert_called_once_with("delta")
    writer.format.return_value.option.assert_called_once_with(
        "delta.checkpointLocation", "/tmp/ckpt"
    )
    option_writer = writer.format.return_value.option.return_value
    option_writer.mode.assert_called_once_with("append")
    option_writer.mode.return_value.save.assert_called_once_with("/tmp/x")


@pytest.mark.parametrize("file_format", [None, "parquet"])
def test_offline_write_batch_table_format_takes_precedence(file_format):
    source = SparkSource(
        name="s",
        path="/tmp/x",
        file_format=file_format,
        table_format=DeltaFormat(),
        timestamp_field="ts",
    )
    session = _write_batch(source)

    session.read.format.assert_called_once_with("delta")
    session.read.parquet.return_value.write.format.assert_called_once_with("delta")
