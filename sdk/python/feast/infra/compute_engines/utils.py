from datetime import datetime
from typing import Optional

from feast.data_source import DataSource
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.offline_stores.offline_store import RetrievalJob


def create_offline_store_retrieval_job(
    data_source: DataSource,
    column_info: ColumnInfo,
    context: ExecutionContext,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> RetrievalJob:
    """
    Create a retrieval job for the offline store.
    Args:
        data_source: The data source to pull from.
        column_info: Column information containing join keys, feature columns, and timestamps.
        context:
        start_time:
        end_time:
    Returns:

    """
    offline_store = context.offline_store

    pull_latest = context.repo_config.materialization_config.pull_latest_features

    if pull_latest:
        if not start_time or not end_time:
            raise ValueError(
                "start_time and end_time must be provided when pull_latest_features is True"
            )

        retrieval_job = offline_store.pull_latest_from_table_or_query(
            config=context.repo_config,
            data_source=data_source,
            join_key_columns=column_info.join_keys,
            feature_name_columns=column_info.feature_cols,
            timestamp_field=column_info.ts_col,
            created_timestamp_column=column_info.created_ts_col,
            start_date=start_time,
            end_date=end_time,
        )
    else:
        # 📥 Reuse Feast's robust query resolver
        retrieval_job = offline_store.pull_all_from_table_or_query(
            config=context.repo_config,
            data_source=data_source,
            join_key_columns=column_info.join_keys,
            feature_name_columns=column_info.feature_cols,
            timestamp_field=column_info.ts_col,
            created_timestamp_column=column_info.created_ts_col,
            start_date=start_time,
            end_date=end_time,
        )

    return retrieval_job
