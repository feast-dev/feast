from typing import List

from feast.infra.compute_engines.dag.context import ExecutionContext

ENTITY_TS_ALIAS = "__entity_event_timestamp"


def get_partition_columns(context: ExecutionContext) -> List[str]:
    partition_columns = context.column_info.join_keys + [
        ENTITY_TS_ALIAS
        if context.entity_df is not None
        else context.column_info.timestamp_column
    ]
    return [col for col in partition_columns if col]
