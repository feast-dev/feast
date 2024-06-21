from enum import Enum
from typing import Optional

from pydantic import StrictStr

from feast.repo_config import FeastConfigBaseModel


class ConnectionType(Enum):
    singleton = "singleton"
    pool = "pool"


class PostgreSQLConfig(FeastConfigBaseModel):
    min_conn: int = 1
    max_conn: int = 10
    conn_type: ConnectionType = ConnectionType.singleton
    host: StrictStr
    port: int = 5432
    database: StrictStr
    db_schema: StrictStr = "public"
    user: StrictStr
    password: StrictStr
    sslmode: Optional[StrictStr] = None
    sslkey_path: Optional[StrictStr] = None
    sslcert_path: Optional[StrictStr] = None
    sslrootcert_path: Optional[StrictStr] = None
    keepalives_idle: Optional[int] = None
