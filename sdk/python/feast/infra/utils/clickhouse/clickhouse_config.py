from pydantic import ConfigDict, StrictStr

from feast.repo_config import FeastConfigBaseModel


class ClickhouseConfig(FeastConfigBaseModel):
    host: StrictStr
    port: int = 8123
    database: StrictStr
    user: StrictStr
    password: StrictStr
    use_temporary_tables_for_entity_df: bool = True

    # https://github.com/feast-dev/feast/issues/5707
    # We observed that for large materialization jobs, it's multiple times more efficient
    # pushdown deduplication to ClickHouse side rather than doing it in Feast (no matter compute engine used)
    deduplicate_pushdown: bool = False

    model_config = ConfigDict(frozen=True)
