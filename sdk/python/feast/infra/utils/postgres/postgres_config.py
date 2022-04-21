from pydantic import StrictStr

from feast.repo_config import FeastConfigBaseModel


class PostgreSQLConfig(FeastConfigBaseModel):
    host: StrictStr
    port: int = 5432
    database: StrictStr
    db_schema: StrictStr = "public"
    user: StrictStr
    password: StrictStr
