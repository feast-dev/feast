from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa

from feast import FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.redshift import (
    RedshiftOfflineStore,
    RedshiftOfflineStoreConfig,
)
from feast.infra.offline_stores.redshift_source import RedshiftSource
from feast.infra.utils import aws_utils
from feast.repo_config import RepoConfig


@patch.object(aws_utils, "upload_arrow_table_to_redshift")
def test_offline_write_batch(
    mock_upload_arrow_table_to_redshift: MagicMock,
    simple_dataset_1: pd.DataFrame,
):
    repo_config = RepoConfig(
        registry="registry",
        project="project",
        provider="local",
        offline_store=RedshiftOfflineStoreConfig(
            type="redshift",
            region="us-west-2",
            cluster_id="cluster_id",
            database="database",
            user="user",
            iam_role="abcdef",
            s3_staging_location="s3://bucket/path",
        ),
    )

    batch_source = RedshiftSource(
        name="test_source",
        timestamp_field="ts",
        table="table_name",
        schema="schema_name",
    )
    feature_view = FeatureView(
        name="test_view",
        source=batch_source,
    )

    pa_dataset = pa.Table.from_pandas(simple_dataset_1)

    # patch some more things so that the function can run
    def mock_get_pyarrow_schema_from_batch_source(*args, **kwargs) -> pa.Schema:
        return pa_dataset.schema, pa_dataset.column_names

    with patch.object(
        offline_utils,
        "get_pyarrow_schema_from_batch_source",
        new=mock_get_pyarrow_schema_from_batch_source,
    ):
        RedshiftOfflineStore.offline_write_batch(
            repo_config, feature_view, pa_dataset, progress=None
        )

    # check that we have included the fully qualified table name
    mock_upload_arrow_table_to_redshift.assert_called_once()

    call = mock_upload_arrow_table_to_redshift.call_args_list[0]
    assert call.kwargs["table_name"] == "schema_name.table_name"
