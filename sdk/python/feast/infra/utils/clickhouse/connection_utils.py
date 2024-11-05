from functools import cache

import clickhouse_connect
from clickhouse_connect.driver import Client

from feast.infra.utils.clickhouse.clickhouse_config import ClickhouseConfig


@cache
def get_client(config: ClickhouseConfig) -> Client:
    client = clickhouse_connect.get_client(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.database,
    )
    return client
