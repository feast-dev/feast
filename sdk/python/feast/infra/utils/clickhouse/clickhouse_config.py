from pydantic import StrictStr

from feast.repo_config import FeastConfigBaseModel


class ClickhouseConfig(FeastConfigBaseModel, frozen=True):
    host: StrictStr
    port: int = 8123
    database: StrictStr
    user: StrictStr
    password: StrictStr
    use_temporary_tables_for_entity_df: bool = True
