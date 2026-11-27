from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from feast.infra.offline_stores.contrib.athena_offline_store.athena import (
    AthenaOfflineStore,
    AthenaOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.athena_offline_store.athena_source import (
    AthenaSource,
)
from feast.infra.utils import aws_utils
from feast.repo_config import RepoConfig


@patch.object(aws_utils, "get_s3_resource", MagicMock())
@patch.object(aws_utils, "get_athena_data_client", MagicMock())
def test_pull_all_partition_filter_uses_dates_only():
    config = RepoConfig(
        registry="registry",
        project="project",
        provider="local",
        offline_store=AthenaOfflineStoreConfig(
            type="athena",
            data_source="AwsDataCatalog",
            region="us-west-2",
            database="db",
            workgroup="wg",
            s3_staging_location="s3://bucket/path",
        ),
        entity_key_serialization_version=3,
    )
    source = AthenaSource(
        name="source",
        table="table",
        timestamp_field="ts",
        date_partition_column="dt",
    )

    job = AthenaOfflineStore.pull_all_from_table_or_query(
        config=config,
        data_source=source,
        join_key_columns=["id"],
        feature_name_columns=["f"],
        timestamp_field="ts",
        start_date=datetime(2026, 2, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2026, 2, 2, 2, tzinfo=timezone.utc),
    )

    with job._query_generator() as query:
        pass
    assert "ts BETWEEN TIMESTAMP '2026-02-01 01:00:00.000'" in query
    # The partition column holds dates, so the bounds must not carry a time of
    # day: '2026-02-01' < '2026-02-01 01:00:00.000' would drop the first day.
    assert "dt >= '2026-02-01' AND dt <= '2026-02-02'" in query
