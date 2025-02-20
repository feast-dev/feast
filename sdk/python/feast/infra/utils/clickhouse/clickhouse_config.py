from pydantic import ConfigDict, StrictStr

from feast.repo_config import FeastConfigBaseModel


class ClickhouseConfig(FeastConfigBaseModel):
    host: StrictStr
    port: int = 8123
    database: StrictStr
    user: StrictStr
    password: StrictStr
    use_temporary_tables_for_entity_df: bool = True

    model_config = ConfigDict(frozen=True)
