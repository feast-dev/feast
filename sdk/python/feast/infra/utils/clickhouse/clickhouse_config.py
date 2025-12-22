from pydantic import ConfigDict, StrictStr

from feast.repo_config import FeastConfigBaseModel


class ClickhouseConfig(FeastConfigBaseModel):
    host: StrictStr
    port: int = 8123
    database: StrictStr
    user: StrictStr
    password: StrictStr
    use_temporary_tables_for_entity_df: bool = True

    # Set this to higher than default, for larger scale offline store jobs
    send_receive_timeout: int | None = None

    model_config = ConfigDict(frozen=True)
