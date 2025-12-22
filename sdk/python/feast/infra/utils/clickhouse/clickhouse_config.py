from typing import Any

from pydantic import ConfigDict, StrictStr

from feast.repo_config import FeastConfigBaseModel


class ClickhouseConfig(FeastConfigBaseModel):
    host: StrictStr
    port: int = 8123
    database: StrictStr
    user: StrictStr
    password: StrictStr
    use_temporary_tables_for_entity_df: bool = True

    # See https://github.com/ClickHouse/clickhouse-connect/blob/main/clickhouse_connect/driver/__init__.py#L51
    # Some typical ones e.g. send_receive_timeout (read_timeout), etc
    additional_client_args: dict[str, Any] | None = None

    model_config = ConfigDict(frozen=True)
