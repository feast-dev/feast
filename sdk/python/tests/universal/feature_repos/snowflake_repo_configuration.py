import os

from tests.universal.feature_repos.universal.data_sources.snowflake import (
    SnowflakeDataSourceCreator,
)

SNOWFLAKE_CONFIG = {
    "type": "snowflake.online",
    "account": os.getenv("SNOWFLAKE_CI_DEPLOYMENT", ""),
    "user": os.getenv("SNOWFLAKE_CI_USER", ""),
    "password": os.getenv("SNOWFLAKE_CI_PASSWORD", ""),
    "role": os.getenv("SNOWFLAKE_CI_ROLE", ""),
    "warehouse": os.getenv("SNOWFLAKE_CI_WAREHOUSE", ""),
    "database": os.getenv("SNOWFLAKE_CI_DATABASE", "FEAST"),
    "schema": os.getenv("SNOWFLAKE_CI_SCHEMA_ONLINE", "ONLINE"),
}

AVAILABLE_OFFLINE_STORES = [("aws", SnowflakeDataSourceCreator)]
AVAILABLE_ONLINE_STORES = {
    "snowflake": (SNOWFLAKE_CONFIG, None),
}
