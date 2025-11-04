import threading

import clickhouse_connect
from clickhouse_connect.driver import Client

from feast.infra.utils.clickhouse.clickhouse_config import ClickhouseConfig

thread_local = threading.local()


def get_client(config: ClickhouseConfig) -> Client:
    # Clickhouse client is not thread-safe, so we need to create a separate instance for each thread.
    if not hasattr(thread_local, "clickhouse_client"):
        thread_local.clickhouse_client = clickhouse_connect.get_client(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
        )

    return thread_local.clickhouse_client
