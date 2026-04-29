"""Unit tests for feast.infra.utils.aws_utils."""

from unittest.mock import MagicMock, patch

import pyarrow as pa

from feast.infra.utils.aws_utils import unload_athena_query_to_pa


def test_unload_athena_query_to_pa_returns_empty_table_when_no_parquet_files(
    tmp_path,
):
    """unload_athena_query_to_pa must return an empty PyArrow table when the
    Athena CTAS query produces no output rows.

    When there is no data in the requested time window the Athena CTAS query
    succeeds but writes zero Parquet files to S3.  Before the fix,
    ``pq.read_table`` on an empty directory returned a schema-less table whose
    columns were missing, causing a ``KeyError: 'event_timestamp'`` in the
    downstream dedup node.  After the fix an empty ``pa.table({})`` is returned
    immediately so the pipeline can short-circuit cleanly.
    """
    s3_path = "s3://test-bucket/test-prefix/unload/abc123"

    with (
        patch("feast.infra.utils.aws_utils.execute_athena_query_and_unload_to_s3"),
        patch("feast.infra.utils.aws_utils.download_s3_directory"),
        patch("feast.infra.utils.aws_utils.delete_s3_directory"),
        patch("tempfile.TemporaryDirectory") as mock_tmpdir,
    ):
        # Simulate an empty temporary directory (no Parquet files written by Athena)
        mock_tmpdir.return_value.__enter__.return_value = str(tmp_path)

        result = unload_athena_query_to_pa(
            athena_data_client=MagicMock(),
            data_source="awsdatacatalog",
            database="feature_store",
            workgroup="primary",
            s3_resource=MagicMock(),
            s3_path=s3_path,
            query="SELECT 1 WHERE 1=0",
            temp_table="_tmp_test",
        )

    assert isinstance(result, pa.Table)
    assert result.num_rows == 0
